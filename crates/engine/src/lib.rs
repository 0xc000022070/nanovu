mod bridge;
mod delivery;
mod runner;
#[cfg(test)]
mod tests;

use std::sync::{Arc, OnceLock};

use nanovu_core::domain::{Run, RunStatus, SubscriberPatch, Workflow};
use nanovu_core::{AppState, Environment, HttpError, Id, time};
use serde_json::{Map, Value};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const MAX_RECIPIENTS: usize = 2000;

#[derive(Clone, Debug, PartialEq)]
pub enum Recipient {
    Subscriber {
        subscriber_id: String,
        patch: Box<SubscriberPatch>,
    },
    Topic {
        key: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TriggerRequest {
    pub workflow_id: String,
    pub to: Vec<Recipient>,
    pub payload: Value,
    pub transaction_id: Option<String>,
    pub actor: Option<String>,
    pub bridge_url: Option<String>,
    pub idempotency_key: Option<String>,
}

pub(crate) struct Shared {
    state: AppState,
    wake: Notify,
    http: OnceLock<reqwest::Client>,
    fcm_tokens: delivery::TokenCache,
}

#[derive(Clone)]
pub struct Engine(Arc<Shared>);

impl Engine {
    pub fn new(state: AppState) -> Self {
        Self(Arc::new(Shared {
            state,
            wake: Notify::new(),
            http: OnceLock::new(),
            fcm_tokens: Default::default(),
        }))
    }

    pub fn spawn(&self) -> JoinHandle<()> {
        tokio::spawn(runner::schedule(self.0.clone()))
    }

    pub async fn trigger(
        &self,
        env: &Environment,
        request: TriggerRequest,
    ) -> Result<String, HttpError> {
        let store = &self.0.state.store;
        let workflow = store
            .workflow(&env.id, &request.workflow_id)
            .await?
            .ok_or_else(|| {
                HttpError::unprocessable(format!(
                    "workflow_not_found: workflow {} is not synced",
                    request.workflow_id
                ))
            })?;
        if let Some(schema) = &workflow.payload_schema {
            validate(schema, &request.payload, "payload")
                .map_err(|e| HttpError::bad_request(format!("Payload validation failed: {e}")))?;
        }
        let bridge_url = request.bridge_url.unwrap_or(workflow.bridge_url);
        bridge::bridge_url(env, &bridge_url).map_err(HttpError::bad_request)?;

        let mut subscribers: Vec<String> = Vec::new();
        for recipient in &request.to {
            let ids = match recipient {
                Recipient::Subscriber {
                    subscriber_id,
                    patch,
                } => {
                    store
                        .upsert_subscriber(&env.id, subscriber_id, patch)
                        .await?;
                    vec![subscriber_id.clone()]
                }
                Recipient::Topic { key } => store
                    .topic_subscriber_ids(&env.id, key, MAX_RECIPIENTS as u32 + 1)
                    .await?
                    .into_iter()
                    .filter(|id| request.actor.as_ref() != Some(id))
                    .collect(),
            };
            for id in ids {
                if !subscribers.contains(&id) {
                    subscribers.push(id);
                }
            }
            if subscribers.len() > MAX_RECIPIENTS {
                return Err(HttpError::bad_request(format!(
                    "trigger must reach at most {MAX_RECIPIENTS} subscribers"
                )));
            }
        }

        let transaction_id = request
            .transaction_id
            .unwrap_or_else(|| Id::new().to_string());
        let now = time::now_ms();
        let payload = Arc::new(request.payload);
        let runs: Vec<Run> = subscribers
            .into_iter()
            .map(|subscriber_id| Run {
                id: Id::new(),
                env_id: env.id.clone(),
                workflow_id: workflow.workflow_id.clone(),
                subscriber_id,
                transaction_id: transaction_id.clone(),
                bridge_url: bridge_url.clone(),
                payload: payload.clone(),
                actor: request.actor.clone(),
                step_index: 0,
                state: Value::Object(Map::new()),
                status: RunStatus::Pending,
                run_at: now,
                attempts: 0,
                digest_key: None,
                created_at: now,
                updated_at: now,
            })
            .collect();
        let earlier = store
            .create_runs(
                &env.id,
                &transaction_id,
                request.idempotency_key.as_deref(),
                &runs,
            )
            .await?;
        self.0.wake.notify_one();
        Ok(earlier.unwrap_or(transaction_id))
    }

    pub async fn cancel(&self, env: &Environment, transaction_id: &str) -> Result<bool, HttpError> {
        let cancelled = self
            .0
            .state
            .store
            .cancel_transaction(&env.id, transaction_id)
            .await?;
        self.0.wake.notify_one();
        Ok(cancelled > 0)
    }

    pub async fn sync(
        &self,
        env: &Environment,
        bridge_url: &str,
    ) -> Result<Vec<Workflow>, HttpError> {
        let workflows = bridge::discover(&self.0.http, env, bridge_url)
            .await
            .map_err(|e| {
                tracing::warn!(env = %env.name, error = %e.message(), "bridge discover failed");
                HttpError::bad_request(format!("Bridge discover failed: {}", e.message()))
            })?;
        Ok(self
            .0
            .state
            .store
            .replace_workflows(&env.id, &workflows)
            .await?)
    }
}

fn validate(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let schema = match schema {
        Value::Bool(true) => return Ok(()),
        Value::Bool(false) => return Err(format!("{path} is not allowed")),
        Value::Object(schema) => schema,
        _ => return Ok(()),
    };
    let fail = |reason: String| format!("{path} {reason}");
    check_literals(schema, value).map_err(fail)?;
    check_combinators(schema, value, path)?;
    match value {
        Value::Object(object) => check_object(schema, object, path),
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                if let Some(s) = schema.get("items") {
                    validate(s, item, &format!("{path}[{i}]"))?;
                }
            }
            check_length(
                schema,
                items.len(),
                "Items",
                "has the wrong number of items",
            )
            .map_err(fail)
        }
        Value::String(s) => {
            check_length(schema, s.chars().count(), "Length", "has the wrong length").map_err(fail)
        }
        Value::Number(n) => check_range(schema, n.as_f64().unwrap_or_default()).map_err(fail),
        Value::Null | Value::Bool(_) => Ok(()),
    }
}

fn type_matches(t: &str, value: &Value) -> bool {
    match t {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_f64().is_some_and(|n| n.fract() == 0.0),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => true,
    }
}

fn check_literals(schema: &Map<String, Value>, value: &Value) -> Result<(), String> {
    let types: Vec<&str> = match schema.get("type") {
        Some(Value::String(t)) => vec![t.as_str()],
        Some(Value::Array(ts)) => ts.iter().filter_map(Value::as_str).collect(),
        _ => vec![],
    };
    if !types.is_empty() && !types.into_iter().any(|t| type_matches(t, value)) {
        return Err(format!("must be {}", schema["type"]));
    }
    if let Some(Value::Array(options)) = schema.get("enum")
        && !options.contains(value)
    {
        return Err("must be one of the allowed values".into());
    }
    match schema.get("const") {
        Some(expected) if expected != value => Err(format!("must be {expected}")),
        _ => Ok(()),
    }
}

fn check_combinators(schema: &Map<String, Value>, value: &Value, path: &str) -> Result<(), String> {
    let passes = |s: &Value| validate(s, value, path).is_ok();
    if let Some(Value::Array(all)) = schema.get("allOf") {
        for s in all {
            validate(s, value, path)?;
        }
    }
    if let Some(Value::Array(any)) = schema.get("anyOf")
        && !any.iter().any(passes)
    {
        return Err(format!("{path} must match a schema in anyOf"));
    }
    if let Some(Value::Array(one)) = schema.get("oneOf")
        && one.iter().filter(|s| passes(s)).count() != 1
    {
        return Err(format!("{path} must match exactly one schema in oneOf"));
    }
    Ok(())
}

fn check_object(
    schema: &Map<String, Value>,
    object: &Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    if let Some(Value::Array(required)) = schema.get("required")
        && let Some(key) = required
            .iter()
            .filter_map(Value::as_str)
            .find(|key| !object.contains_key(*key))
    {
        return Err(format!("{path} must have required property '{key}'"));
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    for (key, item) in object {
        let item_schema = properties
            .and_then(|p| p.get(key))
            .or_else(|| schema.get("additionalProperties"));
        if let Some(s) = item_schema {
            validate(s, item, &format!("{path}.{key}"))?;
        }
    }
    Ok(())
}

fn check_length(
    schema: &Map<String, Value>,
    len: usize,
    suffix: &str,
    reason: &str,
) -> Result<(), String> {
    let bound = |key: &str| schema.get(key).and_then(Value::as_f64);
    let len = len as f64;
    if bound(&format!("min{suffix}")).is_some_and(|m| len < m)
        || bound(&format!("max{suffix}")).is_some_and(|m| len > m)
    {
        return Err(reason.into());
    }
    Ok(())
}

fn check_range(schema: &Map<String, Value>, n: f64) -> Result<(), String> {
    let bound = |key: &str| schema.get(key).and_then(Value::as_f64);
    if bound("minimum").is_some_and(|m| n < m)
        || bound("maximum").is_some_and(|m| n > m)
        || bound("exclusiveMinimum").is_some_and(|m| n <= m)
        || bound("exclusiveMaximum").is_some_and(|m| n >= m)
    {
        return Err("is out of range".into());
    }
    Ok(())
}
