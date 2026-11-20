use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::Duration;

use nanovu_core::domain::{
    ChannelPrefs, Severity, StepKind, Workflow, WorkflowPreferences, WorkflowStep,
};
use nanovu_core::{Environment, Id, time};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, Response, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Map, Value};

const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RESPONSE: usize = 2 << 20;

#[derive(Debug)]
pub(crate) enum CallError {
    Transient(String),
    Permanent(String),
}

impl CallError {
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Transient(m) | Self::Permanent(m) => m,
        }
    }

    pub(crate) fn status(status: StatusCode, body: &[u8]) -> Self {
        let message = format!(
            "{status}: {}",
            String::from_utf8_lossy(&body[..body.len().min(2048)])
        );
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            Self::Transient(message)
        } else {
            Self::Permanent(message)
        }
    }
}

impl From<nanovu_core::StoreError> for CallError {
    fn from(err: nanovu_core::StoreError) -> Self {
        Self::Transient(err.to_string())
    }
}

pub(crate) fn client(cell: &OnceLock<Client>) -> Result<&Client, CallError> {
    if let Some(client) = cell.get() {
        return Ok(client);
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(TIMEOUT)
        .build()
        .map_err(|e| CallError::Transient(format!("http client: {e}")))?;
    Ok(cell.get_or_init(|| client))
}

pub(crate) async fn read(response: reqwest::Result<Response>) -> Result<Vec<u8>, CallError> {
    match fetch(response).await? {
        (status, body) if status.is_success() => Ok(body),
        (status, body) => Err(CallError::status(status, &body)),
    }
}

pub(crate) async fn fetch(
    response: reqwest::Result<Response>,
) -> Result<(StatusCode, Vec<u8>), CallError> {
    let mut response = response.map_err(|e| CallError::Transient(e.to_string()))?;
    let status = response.status();
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| CallError::Transient(e.to_string()))?
    {
        if body.len() + chunk.len() > MAX_RESPONSE {
            return Err(CallError::Permanent("response too large".into()));
        }
        body.extend_from_slice(&chunk);
    }
    Ok((status, body))
}

pub(crate) fn bridge_url(env: &Environment, raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|_| "Invalid bridge URL".to_owned())?;
    let host = url
        .host_str()
        .ok_or("Invalid bridge URL")?
        .trim_matches(['[', ']']);
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err("Bridge URL must use https".into());
    }
    if !env
        .bridge_hosts
        .iter()
        .any(|h| h.trim_matches(['[', ']']).eq_ignore_ascii_case(host))
    {
        return Err(format!("Bridge host {host} is not allowed"));
    }
    Ok(url)
}

async fn call(
    http: &OnceLock<Client>,
    env: &Environment,
    raw_url: &str,
    query: &[(&str, &str)],
    body: Option<&Value>,
) -> Result<Vec<u8>, CallError> {
    let mut url = bridge_url(env, raw_url).map_err(CallError::Permanent)?;
    url.query_pairs_mut().extend_pairs(query);
    let bytes = js_json(body.unwrap_or(&Value::Object(Map::new())));
    let signature = env.sign_bridge(time::now_ms(), &bytes);
    let request = match body {
        Some(_) => client(http)?
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(bytes),
        None => client(http)?.get(url),
    };
    read(request.header("novu-signature", signature).send().await).await
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct Execution {
    pub outputs: Map<String, Value>,
    pub providers: Map<String, Value>,
    pub options: ExecutionOptions,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct ExecutionOptions {
    pub skip: bool,
}

impl Execution {
    pub(crate) fn skipped(&self) -> bool {
        self.options.skip || self.outputs.is_empty()
    }
}

pub(crate) async fn execute(
    http: &OnceLock<Client>,
    env: &Environment,
    bridge_url: &str,
    workflow_id: &str,
    step_id: &str,
    body: &Value,
) -> Result<Execution, CallError> {
    let query = [
        ("action", "execute"),
        ("workflowId", workflow_id),
        ("stepId", step_id),
    ];
    let bytes = call(http, env, bridge_url, &query, Some(body)).await?;
    serde_json::from_slice(&bytes)
        .map_err(|e| CallError::Permanent(format!("invalid execute response: {e}")))
}

#[derive(Deserialize)]
struct Discovery {
    workflows: Vec<DiscoveredWorkflow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveredWorkflow {
    workflow_id: String,
    name: Option<String>,
    description: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    severity: Option<String>,
    #[serde(default)]
    preferences: DiscoveredPreferences,
    payload: Option<DiscoveredSchema>,
    steps: Vec<DiscoveredStep>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct DiscoveredPreferences {
    all: Option<DiscoveredAll>,
    channels: Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveredAll {
    enabled: Option<bool>,
    read_only: Option<bool>,
}

#[derive(Deserialize)]
struct DiscoveredSchema {
    schema: Option<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveredStep {
    step_id: String,
    #[serde(rename = "type")]
    kind: String,
}

pub(crate) async fn discover(
    http: &OnceLock<Client>,
    env: &Environment,
    bridge_url: &str,
) -> Result<Vec<Workflow>, CallError> {
    let bytes = call(http, env, bridge_url, &[("action", "discover")], None).await?;
    let discovery: Discovery = serde_json::from_slice(&bytes)
        .map_err(|e| CallError::Permanent(format!("invalid discover response: {e}")))?;
    let now = time::now_ms();
    Ok(discovery
        .workflows
        .into_iter()
        .map(|w| {
            let enabled = |v: &Value| v.get("enabled").and_then(Value::as_bool);
            let mut channels = ChannelPrefs::default();
            for (key, value) in &w.preferences.channels {
                if let Some(channel) = nanovu_core::domain::Channel::parse(key) {
                    channels.set(channel, enabled(value));
                }
            }
            let all = w.preferences.all.as_ref();
            Workflow {
                id: Id::new(),
                name: w.name.unwrap_or_else(|| w.workflow_id.clone()),
                workflow_id: w.workflow_id,
                description: w.description,
                tags: w.tags,
                severity: w
                    .severity
                    .as_deref()
                    .and_then(Severity::parse)
                    .unwrap_or_default(),
                preferences: WorkflowPreferences {
                    enabled: all.and_then(|a| a.enabled).unwrap_or(true),
                    read_only: all.and_then(|a| a.read_only).unwrap_or(false),
                    channels,
                },
                payload_schema: w.payload.and_then(|p| p.schema).filter(Value::is_object),
                steps: w
                    .steps
                    .into_iter()
                    .map(|s| WorkflowStep {
                        kind: serde_json::from_value(Value::String(s.kind))
                            .unwrap_or(StepKind::Unsupported),
                        step_id: s.step_id,
                    })
                    .collect(),
                bridge_url: bridge_url.to_owned(),
                created_at: now,
                updated_at: now,
            }
        })
        .collect())
}

pub(crate) fn js_json(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_js(value, &mut out);
    out
}

fn write_js(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => {
            out.extend_from_slice(value.to_string().as_bytes());
        }
        Value::Number(n) => {
            let text = match (n.as_u64(), n.as_i64()) {
                (Some(u), _) if u <= 1 << 53 => u.to_string(),
                (_, Some(i)) if i.unsigned_abs() <= 1 << 53 => i.to_string(),
                _ => js_number(n.as_f64().unwrap_or_default()),
            };
            out.extend_from_slice(text.as_bytes());
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_js(item, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by_key(|(k, _)| array_index(k).unwrap_or(u32::MAX));
            out.push(b'{');
            for (i, (key, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(Value::String(key.clone()).to_string().as_bytes());
                out.push(b':');
                write_js(item, out);
            }
            out.push(b'}');
        }
    }
}

fn array_index(key: &str) -> Option<u32> {
    let n: u32 = key.parse().ok()?;
    (n != u32::MAX && n.to_string() == key).then_some(n)
}

fn js_number(x: f64) -> String {
    if x == 0.0 {
        return "0".into();
    }
    if x < 0.0 {
        return format!("-{}", js_number(-x));
    }
    let sci = format!("{x:e}");
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let k = i64::try_from(digits.len()).unwrap_or(0);
    let n = exp.parse::<i64>().unwrap_or(0) + 1;
    let zeros = |count: i64| "0".repeat(usize::try_from(count).unwrap_or(0));
    if k <= n && n <= 21 {
        format!("{digits}{}", zeros(n - k))
    } else if 0 < n && n <= 21 {
        let (int, frac) = digits.split_at(usize::try_from(n).unwrap_or(0));
        format!("{int}.{frac}")
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", zeros(-n))
    } else {
        let e = n - 1;
        let sign = if e >= 0 { "+" } else { "-" };
        let (first, rest) = digits.split_at(1);
        if rest.is_empty() {
            format!("{first}e{sign}{}", e.abs())
        } else {
            format!("{first}.{rest}e{sign}{}", e.abs())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_match_javascript() {
        let cases = [
            (1.0, "1"),
            (0.1, "0.1"),
            (100.0, "100"),
            (1.5e-7, "1.5e-7"),
            (1e-7, "1e-7"),
            (0.000001, "0.000001"),
            (1e21, "1e+21"),
            (1.2345e21, "1.2345e+21"),
            (123456789012345680000.0, "123456789012345680000"),
            (-2.5, "-2.5"),
            (-0.0, "0"),
            (123.456, "123.456"),
        ];
        for (x, expected) in cases {
            assert_eq!(js_number(x), expected, "{x}");
        }
    }

    #[test]
    fn json_matches_javascript_reserialization() {
        let text =
            r#"{"b":1.0,"2":[1e21,9007199254740993,-0.0],"1":"é\u0001/","a":{"10":null,"9":true}}"#;
        let value: Value = serde_json::from_str(text).unwrap();
        assert_eq!(
            String::from_utf8(js_json(&value)).unwrap(),
            r#"{"1":"é\u0001/","2":[1e+21,9007199254740992,0],"b":1,"a":{"9":true,"10":null}}"#
        );
        assert_eq!(
            js_json(&json!({"4294967295": 1, "0": 2, "01": 3})),
            br#"{"0":2,"4294967295":1,"01":3}"#
        );
    }
}
