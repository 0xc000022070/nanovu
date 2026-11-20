use std::sync::Arc;
use std::time::Duration;

use nanovu_core::domain::{
    ChannelPrefs, DigestJoin, Run, RunStatus, StepKind, Workflow, WorkflowStep,
};
use nanovu_core::{Environment, time};
use serde_json::{Map, Value, json};
use tokio::task::JoinSet;

use crate::Shared;
use crate::bridge::{self, CallError, Execution};
use crate::delivery;

/// Runs processed at once, each holding its payload and digest events in memory.
const BATCH: u32 = 4;
pub(crate) const MAX_DIGEST_EVENTS: u32 = 50;
const MAX_DIGEST_BYTES: u32 = 64 << 10;
const MAX_ATTEMPTS: u32 = 3;
const IDLE_ON_ERROR: Duration = Duration::from_secs(5);
const MAX_DURATION: i64 = 366 * time::DAY;

pub(crate) async fn schedule(shared: Arc<Shared>) {
    loop {
        let healthy = run_due(&shared, time::now_ms()).await;
        let wait = match shared.state.store.next_run_at().await {
            Ok(next) if healthy => next
                .map(|at| Duration::from_millis(u64::try_from(at - time::now_ms()).unwrap_or(0))),
            Ok(_) => Some(IDLE_ON_ERROR),
            Err(err) => {
                tracing::error!(error = %err, "cannot read the next run time");
                Some(IDLE_ON_ERROR)
            }
        };
        match wait {
            Some(wait) => {
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = shared.wake.notified() => {}
                }
            }
            None => shared.wake.notified().await,
        }
    }
}

pub(crate) async fn run_due(shared: &Arc<Shared>, now: i64) -> bool {
    loop {
        let runs = match shared.state.store.claim_due_runs(now, BATCH).await {
            Ok(runs) => runs,
            Err(err) => {
                tracing::error!(error = %err, "cannot claim due runs");
                return false;
            }
        };
        if runs.is_empty() {
            return true;
        }
        let mut tasks = JoinSet::new();
        for run in runs {
            let shared = shared.clone();
            tasks.spawn(async move { process(&shared, run, now).await });
        }
        while tasks.join_next().await.is_some() {}
    }
}

enum Failure {
    Retry(String),
    Fail(String),
}

impl From<CallError> for Failure {
    fn from(err: CallError) -> Self {
        match err {
            CallError::Transient(m) => Self::Retry(m),
            CallError::Permanent(m) => Self::Fail(m),
        }
    }
}

impl From<nanovu_core::StoreError> for Failure {
    fn from(err: nanovu_core::StoreError) -> Self {
        Self::Retry(err.to_string())
    }
}

async fn process(shared: &Shared, mut run: Run, now: i64) {
    let error = match advance(shared, &mut run, now).await {
        Ok(()) => return,
        Err(Failure::Retry(e)) if run.attempts + 1 < MAX_ATTEMPTS => {
            run.attempts += 1;
            run.status = RunStatus::Pending;
            run.run_at = now + 10 * time::SECOND * 3_i64.pow(run.attempts);
            tracing::warn!(run = %run.id, attempt = run.attempts, error = %e, "run step failed, retrying");
            None
        }
        Err(Failure::Retry(e) | Failure::Fail(e)) => {
            run.status = RunStatus::Failed;
            Some(e)
        }
    };
    if let Some(error) = error {
        tracing::warn!(run = %run.id, workflow = %run.workflow_id, step = run.step_index, error = %error, "run failed");
    }
    if let Err(err) = shared.state.store.save_run(&run).await {
        tracing::error!(run = %run.id, error = %err, "cannot save run");
    }
}

async fn advance(shared: &Shared, run: &mut Run, now: i64) -> Result<(), Failure> {
    let store = &shared.state.store;
    let env = shared
        .state
        .environments
        .get(&run.env_id)
        .ok_or_else(|| Failure::Fail("environment no longer configured".into()))?;
    let workflow = store
        .workflow(&env.id, &run.workflow_id)
        .await?
        .ok_or_else(|| Failure::Fail("workflow no longer exists".into()))?;
    let subscriber = store
        .subscriber(&env.id, &run.subscriber_id)
        .await?
        .ok_or_else(|| Failure::Fail("subscriber no longer exists".into()))?;
    let preferences = store
        .subscriber_preferences(&env.id, &run.subscriber_id)
        .await?;
    let channels = workflow.resolve_channels(
        &preferences.global,
        preferences
            .workflows
            .get(&workflow.workflow_id)
            .unwrap_or(&ChannelPrefs::default()),
    );

    while let Some(step) = usize::try_from(run.step_index)
        .ok()
        .and_then(|i| workflow.steps.get(i))
    {
        let enabled = step
            .kind
            .channel()
            .is_none_or(|c| channels.get(c) != Some(false));
        if !enabled || step.kind == StepKind::Unsupported {
            complete(run, step, Map::new());
        } else if run_step(shared, env, run, &workflow, step, &subscriber, now).await? {
            return Ok(());
        }
        run.attempts = 0;
        if !store.save_run(run).await? {
            return Ok(());
        }
    }
    run.status = RunStatus::Done;
    store.save_run(run).await?;
    Ok(())
}

async fn run_step(
    shared: &Shared,
    env: &Environment,
    run: &mut Run,
    workflow: &Workflow,
    step: &WorkflowStep,
    subscriber: &nanovu_core::domain::Subscriber,
    now: i64,
) -> Result<bool, Failure> {
    if step.kind == StepKind::Digest && run.digest_key.is_some() {
        let (events, total) = shared
            .state
            .store
            .digest_events(&env.id, &run.id, MAX_DIGEST_EVENTS, MAX_DIGEST_BYTES)
            .await?;
        let events: Vec<Value> = events
            .into_iter()
            .map(|e| json!({"id": e.id, "time": time::iso(e.created_at), "payload": e.payload}))
            .collect();
        let result = json!({"eventCount": total, "events": events});
        run.digest_key = None;
        complete(run, step, as_map(result));
        return Ok(false);
    }
    let body = execute_body(shared, env, run, workflow, subscriber).await?;
    let execution = bridge::execute(
        &shared.http,
        env,
        &run.bridge_url,
        &workflow.workflow_id,
        &step.step_id,
        &body,
    )
    .await?;
    if execution.skipped() {
        complete(run, step, Map::new());
        return Ok(false);
    }
    act(
        shared, env, run, workflow, step, subscriber, &execution, now,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "the context `advance` loaded once per run"
)]
async fn act(
    shared: &Shared,
    env: &Environment,
    run: &mut Run,
    workflow: &Workflow,
    step: &WorkflowStep,
    subscriber: &nanovu_core::domain::Subscriber,
    execution: &Execution,
    now: i64,
) -> Result<bool, Failure> {
    let outputs = &execution.outputs;
    match step.kind {
        StepKind::InApp => delivery::in_app(&shared.state, run, workflow, step, outputs).await?,
        StepKind::Push | StepKind::Email => {
            delivery::deliver(shared, env, run, step, subscriber, execution).await?;
        }
        StepKind::Delay => {
            let duration = duration(outputs)?;
            complete(run, step, as_map(json!({ "duration": duration })));
            run.status = RunStatus::Pending;
            run.run_at = now + duration;
            run.attempts = 0;
            shared.state.store.save_run(run).await?;
            return Ok(true);
        }
        StepKind::Digest => {
            let window_end = now + duration(outputs)?;
            let key = outputs
                .get("digestKey")
                .map(|k| k.as_str().map_or_else(|| k.to_string(), str::to_owned))
                .unwrap_or_default();
            run.attempts = 0;
            if let DigestJoin::Merged { owner } = shared
                .state
                .store
                .join_digest(run, &key, window_end)
                .await?
            {
                tracing::debug!(run = %run.id, %owner, "digest event merged");
            }
            return Ok(true);
        }
        StepKind::Unsupported => {}
    }
    complete(run, step, Map::new());
    Ok(false)
}

fn duration(outputs: &Map<String, Value>) -> Result<i64, Failure> {
    let unit = match outputs.get("unit").and_then(Value::as_str) {
        Some("seconds") => time::SECOND,
        Some("minutes") => time::MINUTE,
        Some("hours") => time::HOUR,
        Some("days") => time::DAY,
        Some("weeks") => 7 * time::DAY,
        Some("months") => 30 * time::DAY,
        _ => {
            return Err(Failure::Fail(
                "only regular delays and digests (amount, unit) are supported".into(),
            ));
        }
    };
    let ms = outputs
        .get("amount")
        .and_then(Value::as_f64)
        .filter(|a| *a >= 0.0)
        .ok_or_else(|| Failure::Fail("delay or digest without a valid amount".into()))?
        * unit as f64;
    if !(0.0..=MAX_DURATION as f64).contains(&ms) {
        return Err(Failure::Fail("delay or digest longer than a year".into()));
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "checked against MAX_DURATION"
    )]
    Ok(ms as i64)
}

fn as_map(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

fn complete(run: &mut Run, step: &WorkflowStep, result: Map<String, Value>) {
    if !run.state.is_object() {
        run.state = Value::Object(Map::new());
    }
    if let Value::Object(state) = &mut run.state {
        state.insert(step.step_id.clone(), Value::Object(result));
    }
    run.step_index += 1;
}

fn default_result(kind: StepKind) -> Value {
    match kind {
        StepKind::InApp => {
            json!({"seen": false, "read": false, "lastSeenDate": null, "lastReadDate": null})
        }
        StepKind::Delay => json!({"duration": 0}),
        StepKind::Digest => json!({"events": [], "eventCount": 0}),
        StepKind::Email | StepKind::Push | StepKind::Unsupported => json!({}),
    }
}

async fn execute_body(
    shared: &Shared,
    env: &Environment,
    run: &Run,
    workflow: &Workflow,
    subscriber: &nanovu_core::domain::Subscriber,
) -> Result<Value, Failure> {
    let messages = shared.state.store.run_messages(&env.id, &run.id).await?;
    let done = usize::try_from(run.step_index).unwrap_or(0);
    let state: Vec<Value> = workflow
        .steps
        .iter()
        .take(done)
        .map(|step| {
            let stored = run.state.get(&step.step_id).filter(|v| v.as_object().is_some_and(|m| !m.is_empty()));
            let outputs = match (step.kind, messages.iter().find(|m| m.step_id == step.step_id)) {
                (StepKind::InApp, Some(m)) => json!({
                    "seen": m.is_seen(),
                    "read": m.is_read(),
                    "lastSeenDate": m.seen_at.map(time::iso),
                    "lastReadDate": m.read_at.map(time::iso),
                }),
                _ => stored.cloned().unwrap_or_else(|| default_result(step.kind)),
            };
            json!({"stepId": step.step_id, "outputs": outputs, "state": {"status": "completed", "error": false}})
        })
        .collect();
    let mut person = Map::new();
    person.insert("subscriberId".into(), json!(subscriber.subscriber_id));
    for (key, value) in [
        ("firstName", &subscriber.first_name),
        ("lastName", &subscriber.last_name),
        ("email", &subscriber.email),
        ("phone", &subscriber.phone),
        ("avatar", &subscriber.avatar),
        ("locale", &subscriber.locale),
        ("timezone", &subscriber.timezone),
    ] {
        if let Some(value) = value {
            person.insert(key.into(), json!(value));
        }
    }
    if let Some(data) = &subscriber.data {
        person.insert("data".into(), data.clone());
    }
    let mut body = json!({
        "payload": *run.payload,
        "subscriber": person,
        "state": state,
        "controls": {},
        "context": {},
        "env": {},
        "workflowId": workflow.workflow_id,
        "action": "execute",
    });
    if let Some(actor) = &run.actor {
        body["actor"] = json!({"subscriberId": actor});
    }
    Ok(body)
}
