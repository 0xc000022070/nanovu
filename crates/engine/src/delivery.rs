use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use lettre::message::{Mailbox, header::ContentType};
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use nanovu_core::domain::{
    Action, Channel, Message, Redirect, Run, Subscriber, Workflow, WorkflowStep,
};
use nanovu_core::env::{Fcm, Provider, Secret};
use nanovu_core::{AppState, Environment, Event, Id, time};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, StatusCode};
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::Shared;
use crate::bridge::{self, CallError, Execution};

const FCM_ENDPOINT: &str = "https://fcm.googleapis.com";
const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

pub(crate) type TokenCache = Mutex<HashMap<(Id, String), (String, i64)>>;

pub(crate) async fn in_app(
    state: &AppState,
    run: &Run,
    workflow: &Workflow,
    step: &WorkflowStep,
    outputs: &Map<String, Value>,
) -> Result<(), CallError> {
    let store = &state.store;
    let existing = store.run_messages(&run.env_id, &run.id).await?;
    if existing.iter().any(|m| m.step_id == step.step_id) {
        return Ok(());
    }
    let text = |key: &str| outputs.get(key).and_then(Value::as_str).map(str::to_owned);
    let redirect = |value: Option<&Value>| -> Option<Redirect> {
        let value = value?;
        Some(Redirect {
            url: value.get("url")?.as_str()?.to_owned(),
            target: value
                .get("target")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    };
    let action = |key: &str| -> Option<Action> {
        let value = outputs.get(key)?;
        Some(Action {
            label: value.get("label")?.as_str()?.to_owned(),
            redirect: redirect(value.get("redirect")),
        })
    };
    let now = time::now_ms();
    let message = Message {
        id: Id::new(),
        run_id: Some(run.id.clone()),
        transaction_id: run.transaction_id.clone(),
        subscriber_id: run.subscriber_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        step_id: step.step_id.clone(),
        channel: Channel::InApp,
        subject: text("subject"),
        body: text("body").unwrap_or_default(),
        avatar: text("avatar"),
        redirect: redirect(outputs.get("redirect")),
        primary_action: action("primaryAction"),
        secondary_action: action("secondaryAction"),
        data: outputs.get("data").and_then(Value::as_object).cloned(),
        tags: workflow.tags.clone(),
        severity: workflow.severity,
        seen_at: None,
        read_at: None,
        archived_at: None,
        created_at: now,
        updated_at: now,
    };
    store.insert_message(&run.env_id, &message).await?;
    state.hub.publish(
        &run.env_id,
        &run.subscriber_id,
        Event::MessageCreated(Arc::new(message)),
    );
    Ok(())
}

#[derive(Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    token_uri: String,
    project_id: String,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
struct AccessToken {
    access_token: String,
    expires_in: i64,
}

fn assertion(pem: &str, claims: &Claims) -> Result<String, String> {
    let der: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    let der = STANDARD.decode(der.trim()).map_err(|e| e.to_string())?;
    let key = RsaKeyPair::from_pkcs8(&der).map_err(|e| e.to_string())?;
    let claims = serde_json::to_vec(claims).map_err(|e| e.to_string())?;
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#),
        URL_SAFE_NO_PAD.encode(claims)
    );
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .map_err(|e| e.to_string())?;
    Ok(format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature)))
}

async fn access_token(
    http: &Client,
    cache: &TokenCache,
    key: (Id, String),
    account: &ServiceAccount,
) -> Result<String, CallError> {
    let now = time::now_ms();
    let lock = || cache.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some((token, expires)) = lock().get(&key)
        && *expires > now + time::MINUTE
    {
        return Ok(token.clone());
    }
    let iat = now / 1000;
    let claims = Claims {
        iss: &account.client_email,
        scope: FCM_SCOPE,
        aud: &account.token_uri,
        iat,
        exp: iat + 3600,
    };
    let assertion = assertion(&account.private_key, &claims)
        .map_err(|e| CallError::Permanent(format!("fcm service account key: {e}")))?;
    let body = format!(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={assertion}"
    );
    let response = http
        .post(&account.token_uri)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await;
    let bytes = bridge::read(response).await?;
    let token: AccessToken = serde_json::from_slice(&bytes)
        .map_err(|e| CallError::Permanent(format!("fcm token response: {e}")))?;
    let expires = now + token.expires_in * time::SECOND;
    lock().insert(key, (token.access_token.clone(), expires));
    Ok(token.access_token)
}

fn string_map(value: Option<&Value>) -> Map<String, Value> {
    let Some(Value::Object(map)) = value else {
        return Map::new();
    };
    map.iter()
        .map(|(k, v)| {
            let text = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (k.clone(), Value::String(text))
        })
        .collect()
}

fn merge(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (k, v) in patch {
                merge(target.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

fn fcm_message(
    token: &str,
    outputs: &Map<String, Value>,
    payload: &Value,
    overrides: Option<&Value>,
) -> Value {
    let mut message = json!({
        "token": token,
        "notification": {
            "title": outputs.get("subject").cloned().unwrap_or_default(),
            "body": outputs.get("body").cloned().unwrap_or_default(),
        },
        "data": string_map(Some(payload)),
    });
    if let Some(overrides) = overrides {
        if let Some(Value::Object(n)) = overrides.get("notification") {
            for key in ["title", "body", "image"] {
                if let Some(v) = n.get(key) {
                    message["notification"][key] = v.clone();
                }
            }
        }
        merge(
            &mut message["data"],
            &Value::Object(string_map(overrides.get("data"))),
        );
        if let Some(body) = overrides.pointer("/_passthrough/body") {
            merge(&mut message, body);
        }
    }
    message
}

fn dead_token(body: &[u8]) -> bool {
    let Ok(body) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    let details = body
        .pointer("/error/details")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    details.iter().any(|d| {
        d.get("errorCode").and_then(Value::as_str) == Some("UNREGISTERED")
            || d.get("fieldViolations")
                .and_then(Value::as_array)
                .is_some_and(|v| {
                    v.iter()
                        .any(|f| f.get("field").and_then(Value::as_str) == Some("message.token"))
                })
    })
}

pub(crate) async fn deliver(
    shared: &Shared,
    env: &Environment,
    run: &mut Run,
    step: &WorkflowStep,
    subscriber: &Subscriber,
    execution: &Execution,
) -> Result<(), CallError> {
    let Some(channel) = step.kind.channel() else {
        return Ok(());
    };
    let mut failure = None;
    let mut integrations = 0;
    for integration in env.channel_integrations(channel) {
        integrations += 1;
        let sent = match &integration.provider {
            Provider::Fcm(fcm) => {
                push(
                    shared,
                    env,
                    run,
                    &step.step_id,
                    &integration.identifier,
                    fcm,
                    execution,
                )
                .await
            }
            Provider::Smtp { url, from } => email(url, from, subscriber, &execution.outputs).await,
        };
        if let (Err(e), None | Some(CallError::Permanent(_))) = (sent, &failure) {
            failure = Some(e);
        }
    }
    if integrations == 0 {
        tracing::warn!(env = %env.name, step = %step.step_id, "step without an integration for its channel");
    }
    if !matches!(failure, Some(CallError::Transient(_)))
        && let Value::Object(state) = &mut run.state
    {
        let prefix = format!("{}#sent/", step.step_id);
        state.retain(|key, _| !key.starts_with(&prefix));
    }
    failure.map_or(Ok(()), Err)
}

enum FcmSent {
    Delivered,
    DeadToken,
    Rejected,
}

async fn fcm_send(
    http: &Client,
    url: &str,
    bearer: &str,
    message: Value,
    cache: &TokenCache,
    key: &(Id, String),
) -> Result<FcmSent, CallError> {
    let response = http
        .post(url)
        .header(AUTHORIZATION, format!("Bearer {bearer}"))
        .json(&json!({ "message": message }))
        .send()
        .await;
    let (status, body) = bridge::fetch(response).await?;
    match CallError::status(status, &body) {
        _ if status.is_success() => Ok(FcmSent::Delivered),
        CallError::Permanent(_) if dead_token(&body) => Ok(FcmSent::DeadToken),
        CallError::Permanent(e) if status == StatusCode::UNAUTHORIZED => {
            cache
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(key);
            Err(CallError::Transient(e))
        }
        CallError::Permanent(e) => {
            tracing::warn!(error = %e, "fcm rejected message");
            Ok(FcmSent::Rejected)
        }
        e => Err(e),
    }
}

async fn push(
    shared: &Shared,
    env: &Environment,
    run: &mut Run,
    step_id: &str,
    identifier: &str,
    fcm: &Fcm,
    execution: &Execution,
) -> Result<(), CallError> {
    let (store, cache) = (&shared.state.store, &shared.fcm_tokens);
    let tokens = store
        .device_tokens(&env.id, &run.subscriber_id, identifier)
        .await?;
    if tokens.is_empty() {
        return Ok(());
    }
    let account: ServiceAccount = serde_json::from_str(fcm.service_account.expose())
        .map_err(|e| CallError::Permanent(format!("fcm service account: {e}")))?;
    let http = bridge::client(&shared.http)?;
    let key = (env.id.clone(), identifier.to_owned());
    let url = format!(
        "{}/v1/projects/{}/messages:send",
        fcm.endpoint
            .as_deref()
            .unwrap_or(FCM_ENDPOINT)
            .trim_end_matches('/'),
        account.project_id
    );
    let sent_key = format!("{step_id}#sent/{identifier}");
    let mut sent: Vec<String> = run
        .state
        .get(&sent_key)
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let mut dead = Vec::new();
    let mut transient = None;
    let unsent: Vec<String> = tokens.into_iter().filter(|t| !sent.contains(t)).collect();
    for token in &unsent {
        let bearer = access_token(http, cache, key.clone(), &account).await?;
        let message = fcm_message(
            token,
            &execution.outputs,
            &run.payload,
            execution.providers.get("fcm"),
        );
        match fcm_send(http, &url, &bearer, message, cache, &key).await {
            Ok(FcmSent::Delivered) => sent.push(token.clone()),
            Ok(FcmSent::DeadToken) => dead.push(token.clone()),
            Ok(FcmSent::Rejected) => {}
            Err(e) => transient = Some(e),
        }
    }
    if !dead.is_empty() {
        store
            .remove_device_tokens(&env.id, identifier, &dead)
            .await?;
    }
    if let Value::Object(state) = &mut run.state {
        state.insert(sent_key, json!(sent));
    }
    transient.map_or(Ok(()), Err)
}

async fn email(
    url: &Secret,
    from: &str,
    subscriber: &Subscriber,
    outputs: &Map<String, Value>,
) -> Result<(), CallError> {
    let Some(to) = &subscriber.email else {
        return Ok(());
    };
    let text = |key: &str| outputs.get(key).and_then(Value::as_str);
    let mailbox = |s: &str| {
        s.parse::<Mailbox>()
            .map_err(|e| CallError::Permanent(format!("email address {s}: {e}")))
    };
    let mut builder = lettre::Message::builder()
        .from(mailbox(text("from").unwrap_or(from))?)
        .to(mailbox(to)?)
        .subject(text("subject").unwrap_or_default())
        .header(ContentType::TEXT_HTML);
    if let Some(reply_to) = text("replyTo") {
        builder = builder.reply_to(mailbox(reply_to)?);
    }
    let message = builder
        .body(text("body").unwrap_or_default().to_owned())
        .map_err(|e| CallError::Permanent(format!("email: {e}")))?;
    let transport = AsyncSmtpTransport::<Tokio1Executor>::from_url(url.expose())
        .map_err(|e| CallError::Permanent(format!("smtp url: {e}")))?
        .timeout(Some(std::time::Duration::from_secs(30)))
        .build();
    match transport.send(message).await {
        Ok(_) => Ok(()),
        Err(e) if e.is_permanent() => Err(CallError::Permanent(format!("smtp: {e}"))),
        Err(e) => Err(CallError::Transient(format!("smtp: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn smtp_errors_never_carry_the_password() {
        let subscriber = Subscriber {
            id: Id::new(),
            subscriber_id: "s".into(),
            email: Some("to@example.com".into()),
            first_name: None,
            last_name: None,
            phone: None,
            avatar: None,
            locale: None,
            timezone: None,
            data: None,
            created_at: 0,
            updated_at: 0,
        };
        let outputs = Map::new();
        for url in ["smtp://u:SECRETPW@[bad", "smtp://u:SECRETPW@127.0.0.1:1"] {
            let url = Secret::new(url.into());
            let message = match email(&url, "a@b.c", &subscriber, &outputs).await {
                Err(CallError::Transient(m) | CallError::Permanent(m)) => m,
                Ok(()) => panic!("sent through {}", url.expose()),
            };
            assert!(!message.contains("SECRETPW"), "{message}");
        }
    }

    #[test]
    fn fcm_overrides_merge_over_defaults() {
        let outputs = json!({"subject": "Hi", "body": "There"});
        let overrides = json!({
            "notification": {"title": "Over", "color": "#fff"},
            "data": {"extra": 1},
            "_passthrough": {"body": {"android": {"priority": "high"}}},
        });
        let message = fcm_message(
            "tok",
            outputs.as_object().unwrap(),
            &json!({"n": 2, "s": "x"}),
            Some(&overrides),
        );
        assert_eq!(
            message,
            json!({
                "token": "tok",
                "notification": {"title": "Over", "body": "There"},
                "data": {"n": "2", "s": "x", "extra": "1"},
                "android": {"priority": "high"},
            })
        );
    }
}
