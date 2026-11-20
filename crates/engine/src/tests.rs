use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use nanovu_core::config::Config;
use nanovu_core::domain::{ChannelPrefs, Run, SubscriberPatch, TopicMember};
use nanovu_core::sqlite::SqliteStore;
use nanovu_core::{AppState, Environment, Event, Hub, Id, time};
use pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
use serde_json::{Value, json};

use super::*;

type Respond = Box<dyn Fn(&str, &Value) -> (u16, Value) + Send + Sync>;

struct Call {
    step_id: String,
    body: Value,
    raw: Vec<u8>,
    signature: String,
}

struct Fake {
    discover: Value,
    respond: Respond,
    calls: Mutex<Vec<Call>>,
    fcm: Mutex<Vec<(String, String, Value)>>,
    access_tokens: Mutex<u32>,
    gate: tokio::sync::RwLock<()>,
}

async fn fake_handler(
    State(fake): State<Arc<Fake>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let query = |name: &str| {
        uri.query()
            .unwrap_or_default()
            .split('&')
            .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
            .unwrap_or_default()
            .to_owned()
    };
    let reply = |status: u16, value: Value| {
        (
            StatusCode::from_u16(status).expect("fake replies use valid HTTP status codes"),
            axum::Json(value),
        )
            .into_response()
    };
    match (method, uri.path()) {
        (Method::GET, "/api/novu") => reply(200, fake.discover.clone()),
        (Method::POST, "/api/novu") => {
            let parsed: Value =
                serde_json::from_slice(&body).expect("the engine posts JSON to the bridge");
            let step_id = query("stepId");
            let (status, value) = (fake.respond)(&step_id, &parsed);
            fake.calls
                .lock()
                .expect("no test panics while holding the calls lock")
                .push(Call {
                    step_id,
                    body: parsed,
                    raw: body.to_vec(),
                    signature: headers["novu-signature"]
                        .to_str()
                        .expect("the engine signs every bridge call")
                        .to_owned(),
                });
            let _open = fake.gate.read().await;
            reply(status, value)
        }
        (Method::POST, "/token") => {
            let form = String::from_utf8(body.to_vec()).expect("the token request is a UTF-8 form");
            let jwt = form
                .split_once("&assertion=")
                .expect("the token request carries an assertion")
                .1;
            let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
            validation.validate_aud = false;
            let public = jsonwebtoken::DecodingKey::from_rsa_pem(service_key().1.as_bytes())
                .expect("the generated public key is valid PEM");
            let claims = jsonwebtoken::decode::<Value>(jwt, &public, &validation)
                .expect("the engine signs with the service account key")
                .claims;
            assert_eq!(
                claims["scope"],
                "https://www.googleapis.com/auth/firebase.messaging"
            );
            assert_eq!(claims["iss"], "nanovu@test.iam.gserviceaccount.com");
            let mut issued = fake
                .access_tokens
                .lock()
                .expect("no test panics while holding the access token lock");
            *issued += 1;
            reply(
                200,
                json!({"access_token": format!("fake-access-{issued}"), "expires_in": 3600}),
            )
        }
        (Method::POST, path) if path.ends_with("/v1/projects/p/messages:send") => {
            let auth = headers["authorization"]
                .to_str()
                .expect("the engine sends an ASCII bearer token")
                .to_owned();
            let parsed: Value =
                serde_json::from_slice(&body).expect("the engine posts JSON to FCM");
            let token = parsed["message"]["token"].clone();
            let mut sent = fake
                .fcm
                .lock()
                .expect("no test panics while holding the fcm lock");
            let first_flaky =
                token == "flaky" && !sent.iter().any(|(_, _, m)| m["message"]["token"] == token);
            sent.push((path.to_owned(), auth.clone(), parsed));
            drop(sent);
            if first_flaky {
                reply(
                    503,
                    json!({"error": {"code": 503, "status": "UNAVAILABLE"}}),
                )
            } else if token == "stale-auth" && auth == "Bearer fake-access-1" {
                reply(
                    401,
                    json!({"error": {"code": 401, "status": "UNAUTHENTICATED"}}),
                )
            } else if token == "dead" || token == "dead-verbose" {
                let detail = json!({"@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", "errorCode": "UNREGISTERED"});
                let message = if token == "dead" {
                    String::new()
                } else {
                    "x".repeat(4096)
                };
                reply(
                    404,
                    json!({"error": {"code": 404, "message": message, "status": "NOT_FOUND", "details": [detail]}}),
                )
            } else {
                reply(200, json!({"name": "projects/p/messages/1"}))
            }
        }
        _ => reply(404, json!({})),
    }
}

fn service_key() -> &'static (String, String) {
    static KEY: OnceLock<(String, String)> = OnceLock::new();
    KEY.get_or_init(|| {
        let key =
            rsa::PrivateKey::new(&mut getrandom::SysRng, 2048).expect("the OS provides randomness");
        let private = key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("a fresh RSA key encodes as PKCS#8")
            .to_string();
        let public = key
            .to_public_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("a fresh RSA key encodes as SPKI");
        (private, public)
    })
}

struct Harness {
    engine: Engine,
    state: AppState,
    fake: Arc<Fake>,
    bridge: String,
    dir: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Harness {
    async fn new(
        steps: Value,
        respond: impl Fn(&str, &Value) -> (u16, Value) + Send + Sync + 'static,
    ) -> Self {
        let discover = json!({"workflows": [{
            "workflowId": "wf",
            "name": "Workflow",
            "tags": ["t"],
            "severity": "high",
            "preferences": {"all": {"enabled": true}, "channels": {"email": {"enabled": false}}},
            "payload": {"schema": {"type": "object", "properties": {"n": {"type": "number"}}}},
            "steps": steps,
        }], "agents": []});
        let fake = Arc::new(Fake {
            discover,
            respond: Box::new(respond),
            calls: Mutex::default(),
            fcm: Mutex::default(),
            access_tokens: Mutex::default(),
            gate: tokio::sync::RwLock::default(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback has a free port");
        let addr: SocketAddr = listener
            .local_addr()
            .expect("a bound listener has an address");
        let app = Router::new()
            .fallback(fake_handler)
            .with_state(fake.clone());
        tokio::spawn(async move { axum::serve(listener, app).await });

        let dir = std::env::temp_dir().join(format!("nanovu-engine-{}", Id::new()));
        std::fs::create_dir_all(&dir).expect("the temp dir is writable");
        let secret = |name: &str, value: &str| {
            let path = dir.join(name);
            std::fs::write(&path, value).expect("the temp dir is writable");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .expect("the temp file was just written");
            path
        };
        let account = json!({
            "client_email": "nanovu@test.iam.gserviceaccount.com",
            "private_key": service_key().0,
            "token_uri": format!("http://{addr}/token"),
            "project_id": "p",
        });
        let config = format!(
            "database = \"/unused\"\n[environments.test]\napplication_identifier = \"app\"\n\
             secret_key_file = \"{}\"\nbridge_hosts = [\"127.0.0.1\"]\n\
             [environments.test.integrations.push]\nprovider = \"fcm\"\nservice_account_file = \"{}\"\nendpoint = \"http://{addr}\"\n\
             [environments.test.integrations.push-b]\nprovider = \"fcm\"\nservice_account_file = \"{1}\"\nendpoint = \"http://{addr}/b\"\n",
            secret("key", "0123456789abcdef0123456789abcdef").display(),
            secret("sa.json", &account.to_string()).display(),
        );
        let config =
            Config::load(&secret("config.toml", &config)).expect("the harness config is valid");
        let state = AppState {
            environments: Arc::new(config.environments),
            store: Arc::new(
                SqliteStore::open_in_memory()
                    .await
                    .expect("in-memory SQLite opens"),
            ),
            hub: Hub::default(),
        };
        let engine = Engine::new(state.clone());
        let bridge = format!("http://{addr}/api/novu");
        let harness = Self {
            engine,
            state,
            fake,
            bridge,
            dir,
        };
        let synced = harness
            .engine
            .sync(harness.env(), &harness.bridge)
            .await
            .expect("the fake bridge serves discover");
        assert_eq!(synced.len(), 1);
        harness
    }

    fn env(&self) -> &Environment {
        self.state
            .environments
            .by_name("test")
            .expect("the harness config defines env test")
    }

    async fn trigger(&self, to: &[&str], payload: Value, tx: Option<&str>) -> String {
        let request = TriggerRequest {
            workflow_id: "wf".into(),
            to: to
                .iter()
                .map(|id| Recipient::Subscriber {
                    subscriber_id: (*id).into(),
                    patch: Box::default(),
                })
                .collect(),
            payload,
            transaction_id: tx.map(str::to_owned),
            actor: None,
            bridge_url: None,
            idempotency_key: None,
        };
        self.engine
            .trigger(self.env(), request)
            .await
            .expect("the harness trigger is valid")
    }

    async fn tick(&self, now: i64) {
        assert!(runner::run_due(&self.engine.0, now).await);
    }

    async fn messages(&self, subscriber: &str) -> Vec<nanovu_core::domain::Message> {
        let query = nanovu_core::domain::MessageQuery {
            subscriber_id: Some(subscriber.into()),
            limit: 100,
            ..Default::default()
        };
        self.state
            .store
            .messages(&self.env().id, &query)
            .await
            .expect("in-memory SQLite answers")
            .0
    }

    fn calls(&self, step: &str) -> Vec<Value> {
        let calls = self
            .fake
            .calls
            .lock()
            .expect("no test panics while holding the calls lock");
        calls
            .iter()
            .filter(|c| c.step_id == step)
            .map(|c| c.body.clone())
            .collect()
    }

    async fn drain(&self, now: i64) -> Vec<Run> {
        self.state
            .store
            .claim_due_runs(now, 100)
            .await
            .expect("in-memory SQLite answers")
    }
}

fn in_app(_: &str, _: &Value) -> (u16, Value) {
    (
        200,
        json!({"outputs": {"body": "Hello", "subject": "S"}, "providers": {}, "options": {"skip": false}}),
    )
}

const IN_APP: &str = r#"[{"stepId": "a", "type": "in_app"}]"#;

fn steps(text: &str) -> Value {
    serde_json::from_str(text).expect("test step fixtures are valid JSON")
}

#[tokio::test]
async fn signature_bytes_verify_and_match_javascript_reserialization() {
    let h = Harness::new(steps(IN_APP), in_app).await;
    h.trigger(
        &["s1"],
        json!({"n": 1.0, "2": 3, "1": 9007199254740993_u64}),
        None,
    )
    .await;
    h.tick(time::now_ms()).await;
    let calls = h.fake.calls.lock().unwrap();
    let call = &calls[0];
    assert!(h.env().verify_bridge_signature(&call.signature, &call.raw));
    let reparsed: Value = serde_json::from_slice(&call.raw).unwrap();
    assert_eq!(bridge::js_json(&reparsed), call.raw);
    assert!(
        String::from_utf8_lossy(&call.raw)
            .contains(r#""payload":{"1":9007199254740992,"2":3,"n":1}"#)
    );
    assert_eq!(call.body["subscriber"]["subscriberId"], "s1");
    assert_eq!(call.body["state"], json!([]));
}

#[tokio::test]
async fn topics_dedup_and_exclude_the_actor_only_from_expansion() {
    let h = Harness::new(steps(IN_APP), in_app).await;
    let (env, store) = (&h.env().id, &h.state.store);
    for id in ["a", "b", "actor"] {
        store
            .upsert_subscriber(env, id, &SubscriberPatch::default())
            .await
            .unwrap();
    }
    let members: Vec<TopicMember> = ["a", "b", "actor"]
        .iter()
        .map(|id| TopicMember {
            subscriber_id: (*id).into(),
            identifier: (*id).into(),
        })
        .collect();
    store.subscribe_topic(env, "team", &members).await.unwrap();
    let request = |to: Vec<Recipient>| TriggerRequest {
        workflow_id: "wf".into(),
        to,
        payload: json!({}),
        transaction_id: None,
        actor: Some("actor".into()),
        bridge_url: None,
        idempotency_key: None,
    };
    let subscriber = |id: &str| Recipient::Subscriber {
        subscriber_id: id.into(),
        patch: Box::default(),
    };
    let topic = Recipient::Topic { key: "team".into() };

    h.engine
        .trigger(
            h.env(),
            request(vec![subscriber("a"), topic.clone(), topic]),
        )
        .await
        .unwrap();
    let mut ids: Vec<String> = h
        .drain(time::now_ms())
        .await
        .into_iter()
        .map(|r| r.subscriber_id)
        .collect();
    ids.sort();
    assert_eq!(ids, ["a", "b"]);

    h.engine
        .trigger(h.env(), request(vec![subscriber("actor")]))
        .await
        .unwrap();
    let ids: Vec<String> = h
        .drain(time::now_ms())
        .await
        .into_iter()
        .map(|r| r.subscriber_id)
        .collect();
    assert_eq!(ids, ["actor"]);
}

#[tokio::test]
async fn disabled_channel_skips_the_bridge_call() {
    let h = Harness::new(steps(IN_APP), in_app).await;
    let prefs = ChannelPrefs {
        in_app: Some(false),
        ..Default::default()
    };
    let env = &h.env().id;
    h.state
        .store
        .upsert_subscriber(env, "s1", &SubscriberPatch::default())
        .await
        .unwrap();
    h.state
        .store
        .update_preferences(env, "s1", Some("wf"), &prefs)
        .await
        .unwrap();
    h.trigger(&["s1", "s2"], json!({}), None).await;
    h.tick(time::now_ms()).await;
    let calls = h.calls("a");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["subscriber"]["subscriberId"], "s2");
    assert!(h.messages("s1").await.is_empty());
    assert_eq!(h.messages("s2").await.len(), 1);
}

#[tokio::test]
async fn new_in_app_message_publishes_one_event() {
    let h = Harness::new(steps(IN_APP), in_app).await;
    let mut events = h.state.hub.subscribe(&h.env().id, "s1").unwrap();
    h.trigger(&["s1"], json!({}), None).await;
    h.tick(time::now_ms()).await;
    assert!(matches!(
        events.recv().await,
        Some(Event::MessageCreated(_))
    ));
    let next = tokio::time::timeout(std::time::Duration::from_millis(50), events.recv()).await;
    assert!(next.is_err(), "unexpected second event");
}

#[tokio::test]
async fn cancel_ends_a_run_mid_step_and_the_runner_cannot_resurrect_it() {
    for parking in ["delay", "digest"] {
        let respond = |step: &str, body: &Value| match step {
            "w" => (
                200,
                json!({"outputs": {"type": "regular", "amount": 30, "unit": "seconds"}}),
            ),
            _ => in_app(step, body),
        };
        let workflow = format!(
            r#"[{{"stepId": "w", "type": "{parking}"}}, {{"stepId": "a", "type": "in_app"}}]"#
        );
        let h = Harness::new(steps(&workflow), respond).await;
        h.trigger(&["s1"], json!({}), Some("tx")).await;
        let now = time::now_ms();
        let held = h.fake.gate.write().await;
        let shared = h.engine.0.clone();
        let runner = tokio::spawn(async move { runner::run_due(&shared, now).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while h.calls("w").is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the runner never reached the parking step");
        assert!(h.engine.cancel(h.env(), "tx").await.unwrap(), "{parking}");
        drop(held);
        assert!(runner.await.unwrap());
        assert!(h.drain(now + time::HOUR).await.is_empty(), "{parking}");
        assert!(
            h.calls("a").is_empty() && h.messages("s1").await.is_empty(),
            "{parking}"
        );
    }
}

#[tokio::test]
async fn push_retry_skips_tokens_already_sent() {
    let respond = |_: &str, _: &Value| {
        (
            200,
            json!({"outputs": {"subject": "Title", "body": "Body"}}),
        )
    };
    let h = Harness::new(steps(r#"[{"stepId": "p", "type": "push"}]"#), respond).await;
    let env = &h.env().id;
    h.state
        .store
        .upsert_subscriber(env, "s1", &SubscriberPatch::default())
        .await
        .unwrap();
    let tokens = ["good".to_owned(), "flaky".to_owned()];
    h.state
        .store
        .set_device_tokens(env, "s1", "push", &tokens)
        .await
        .unwrap();
    h.trigger(&["s1"], json!({}), None).await;
    let now = time::now_ms();
    h.tick(now).await;
    h.tick(now + time::MINUTE).await;
    let mut sent: Vec<String> = h
        .fake
        .fcm
        .lock()
        .unwrap()
        .iter()
        .map(|(_, _, m)| m["message"]["token"].to_string())
        .collect();
    sent.sort();
    assert_eq!(sent, [r#""flaky""#, r#""flaky""#, r#""good""#]);
    assert!(h.drain(now + time::HOUR).await.is_empty());
}

#[tokio::test]
async fn push_retry_skips_tokens_sent_through_another_integration() {
    let respond = |_: &str, _: &Value| {
        (
            200,
            json!({"outputs": {"subject": "Title", "body": "Body"}}),
        )
    };
    let h = Harness::new(steps(r#"[{"stepId": "p", "type": "push"}]"#), respond).await;
    let env = &h.env().id;
    let store = &h.state.store;
    store
        .upsert_subscriber(env, "s1", &SubscriberPatch::default())
        .await
        .unwrap();
    for (identifier, token) in [("push", "good"), ("push-b", "flaky")] {
        store
            .set_device_tokens(env, "s1", identifier, &[token.to_owned()])
            .await
            .unwrap();
    }
    h.trigger(&["s1"], json!({}), None).await;
    let now = time::now_ms();
    h.tick(now).await;
    h.tick(now + time::MINUTE).await;
    let mut sent: Vec<(String, String)> = h
        .fake
        .fcm
        .lock()
        .unwrap()
        .iter()
        .map(|(path, _, m)| (path.clone(), m["message"]["token"].to_string()))
        .collect();
    sent.sort();
    let send = |path: &str, token: &str| (path.to_owned(), format!("{token:?}"));
    assert_eq!(
        sent,
        [
            send("/b/v1/projects/p/messages:send", "flaky"),
            send("/b/v1/projects/p/messages:send", "flaky"),
            send("/v1/projects/p/messages:send", "good"),
        ]
    );
    assert!(h.drain(now + time::HOUR).await.is_empty());
}

#[tokio::test]
async fn delay_parks_the_run_and_resumes_after_it() {
    let respond = |step: &str, body: &Value| match step {
        "d" => (
            200,
            json!({"outputs": {"type": "regular", "amount": 30, "unit": "seconds"}}),
        ),
        _ => in_app(step, body),
    };
    let h = Harness::new(
        steps(r#"[{"stepId": "d", "type": "delay"}, {"stepId": "a", "type": "in_app"}]"#),
        respond,
    )
    .await;
    h.trigger(&["s1"], json!({}), None).await;
    let now = time::now_ms();
    h.tick(now).await;
    assert!(h.messages("s1").await.is_empty());
    h.tick(now + 29 * time::SECOND).await;
    assert!(h.calls("a").is_empty());
    h.tick(now + 31 * time::SECOND).await;
    let messages = h.messages("s1").await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, "Hello");
    assert_eq!(messages[0].tags, ["t"]);
    let state = &h.calls("a")[0]["state"];
    assert_eq!(
        state,
        &json!([{"stepId": "d", "outputs": {"duration": 30000}, "state": {"status": "completed", "error": false}}])
    );
}

#[tokio::test]
async fn absurd_durations_fail_the_run() {
    for (amount, unit) in [
        (json!(367), "days"),
        (json!(1e300), "weeks"),
        (json!(-1), "seconds"),
    ] {
        let respond = move |step: &str, body: &Value| match step {
            "d" => (200, json!({"outputs": {"amount": amount, "unit": unit}})),
            _ => in_app(step, body),
        };
        let h = Harness::new(
            steps(r#"[{"stepId": "d", "type": "delay"}, {"stepId": "a", "type": "in_app"}]"#),
            respond,
        )
        .await;
        h.trigger(&["s1"], json!({}), None).await;
        let now = time::now_ms();
        h.tick(now).await;
        assert_eq!(h.calls("d").len(), 1, "{unit}");
        assert!(h.drain(i64::MAX).await.is_empty(), "{unit}");
    }
}

#[tokio::test]
async fn digest_merges_events_and_cancelling_the_owner_promotes_the_oldest() {
    let respond = |step: &str, body: &Value| match step {
        "g" => (
            200,
            json!({"outputs": {"amount": 5, "unit": "minutes", "digestKey": "op-1"}}),
        ),
        _ => in_app(step, body),
    };
    let h = Harness::new(
        steps(r#"[{"stepId": "g", "type": "digest"}, {"stepId": "a", "type": "in_app"}]"#),
        respond,
    )
    .await;
    for (tx, n) in [("tx1", 1), ("tx2", 2), ("tx3", 3)] {
        h.trigger(&["s1"], json!({"n": n}), Some(tx)).await;
        h.tick(time::now_ms()).await;
    }
    assert_eq!(h.calls("g").len(), 3);
    assert!(h.engine.cancel(h.env(), "tx1").await.unwrap());
    assert!(!h.engine.cancel(h.env(), "tx1").await.unwrap());

    h.tick(time::now_ms() + 6 * time::MINUTE).await;
    let messages = h.messages("s1").await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].transaction_id, "tx2");
    let digest = &h.calls("a")[0]["state"][0]["outputs"];
    assert_eq!(digest["eventCount"], 2);
    let payloads: Vec<&Value> = digest["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| &e["payload"])
        .collect();
    assert_eq!(payloads, [&json!({"n": 2}), &json!({"n": 3})]);
}

#[tokio::test]
async fn transient_failures_retry_then_fail() {
    let h = Harness::new(steps(IN_APP), |_: &str, _: &Value| {
        (503, json!({"message": "down"}))
    })
    .await;
    h.trigger(&["s1"], json!({}), None).await;
    let now = time::now_ms();
    for hours in 0..5 {
        h.tick(now + hours * time::HOUR).await;
    }
    assert_eq!(h.calls("a").len(), 3);
    assert!(h.drain(now + 100 * time::HOUR).await.is_empty());
}

#[tokio::test]
async fn bridge_client_errors_fail_without_retry() {
    let h = Harness::new(steps(IN_APP), |_: &str, _: &Value| {
        (400, json!({"message": "bad"}))
    })
    .await;
    h.trigger(&["s1"], json!({}), None).await;
    let now = time::now_ms();
    h.tick(now).await;
    h.tick(now + time::HOUR).await;
    assert_eq!(h.calls("a").len(), 1);
}

#[tokio::test]
async fn early_return_skips_and_later_steps_get_default_state() {
    let respond = |_: &str, _: &Value| {
        (
            200,
            json!({"outputs": {}, "providers": {}, "options": {"skip": false}}),
        )
    };
    let h = Harness::new(
        steps(r#"[{"stepId": "a", "type": "in_app"}, {"stepId": "p", "type": "push"}]"#),
        respond,
    )
    .await;
    let env = &h.env().id;
    h.state
        .store
        .upsert_subscriber(env, "s1", &SubscriberPatch::default())
        .await
        .unwrap();
    h.state
        .store
        .set_device_tokens(env, "s1", "push", &["tok".into()])
        .await
        .unwrap();
    h.trigger(&["s1"], json!({}), None).await;
    h.tick(time::now_ms()).await;
    assert!(h.messages("s1").await.is_empty());
    assert!(h.fake.fcm.lock().unwrap().is_empty());
    let state = &h.calls("p")[0]["state"][0];
    assert_eq!(
        state["outputs"],
        json!({"seen": false, "read": false, "lastSeenDate": null, "lastReadDate": null})
    );
}

#[tokio::test]
async fn push_sends_through_fcm_and_removes_dead_tokens() {
    let respond = |_: &str, _: &Value| {
        (
            200,
            json!({"outputs": {"subject": "Title", "body": "Body"}, "providers": {}}),
        )
    };
    let h = Harness::new(steps(r#"[{"stepId": "p", "type": "push"}]"#), respond).await;
    let env = &h.env().id;
    h.state
        .store
        .upsert_subscriber(env, "s1", &SubscriberPatch::default())
        .await
        .unwrap();
    let tokens = [
        "good".to_owned(),
        "dead".to_owned(),
        "dead-verbose".to_owned(),
    ];
    h.state
        .store
        .set_device_tokens(env, "s1", "push", &tokens)
        .await
        .unwrap();
    h.trigger(&["s1"], json!({"n": 1}), None).await;
    h.tick(time::now_ms()).await;
    {
        let sent = h.fake.fcm.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[0].1, "Bearer fake-access-1");
        assert_eq!(
            sent[0].2["message"]["notification"],
            json!({"title": "Title", "body": "Body"})
        );
        assert_eq!(sent[0].2["message"]["data"], json!({"n": "1"}));
    }
    assert_eq!(
        h.state
            .store
            .device_tokens(env, "s1", "push")
            .await
            .unwrap(),
        ["good"]
    );
}

#[tokio::test]
async fn fcm_401_refreshes_the_access_token_and_retries() {
    let respond = |_: &str, _: &Value| (200, json!({"outputs": {"subject": "T", "body": "B"}}));
    let h = Harness::new(steps(r#"[{"stepId": "p", "type": "push"}]"#), respond).await;
    let env = &h.env().id;
    h.state
        .store
        .upsert_subscriber(env, "s1", &SubscriberPatch::default())
        .await
        .unwrap();
    h.state
        .store
        .set_device_tokens(env, "s1", "push", &["stale-auth".into()])
        .await
        .unwrap();
    h.trigger(&["s1"], json!({}), None).await;
    let now = time::now_ms();
    h.tick(now).await;
    h.tick(now + time::MINUTE).await;
    let auth: Vec<String> = h
        .fake
        .fcm
        .lock()
        .unwrap()
        .iter()
        .map(|(_, a, _)| a.clone())
        .collect();
    assert_eq!(auth, ["Bearer fake-access-1", "Bearer fake-access-2"]);
    assert!(h.drain(now + time::HOUR).await.is_empty());
}

#[tokio::test]
async fn idempotent_trigger_returns_the_first_transaction() {
    let h = Harness::new(steps(IN_APP), in_app).await;
    let request = TriggerRequest {
        workflow_id: "wf".into(),
        to: vec![Recipient::Subscriber {
            subscriber_id: "s1".into(),
            patch: Box::default(),
        }],
        payload: json!({}),
        transaction_id: None,
        actor: None,
        bridge_url: None,
        idempotency_key: Some("key-1".into()),
    };
    let first = h.engine.trigger(h.env(), request.clone()).await.unwrap();
    let second = h.engine.trigger(h.env(), request).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(h.drain(time::now_ms()).await.len(), 1);
}

#[tokio::test]
async fn trigger_rejects_bad_payloads_unknown_workflows_and_foreign_bridges() {
    let h = Harness::new(steps(IN_APP), in_app).await;
    let mut request = TriggerRequest {
        workflow_id: "wf".into(),
        to: vec![],
        payload: json!({"n": "x"}),
        transaction_id: None,
        actor: None,
        bridge_url: None,
        idempotency_key: None,
    };
    let status = |r: Result<String, HttpError>| r.unwrap_err().status.as_u16();
    assert_eq!(
        status(h.engine.trigger(h.env(), request.clone()).await),
        400
    );
    request.payload = json!({"n": 1});
    request.bridge_url = Some("https://evil.example/api/novu".into());
    assert_eq!(
        status(h.engine.trigger(h.env(), request.clone()).await),
        400
    );
    request.bridge_url = Some("http://127.0.0.1:1/x".into());
    assert!(h.engine.trigger(h.env(), request.clone()).await.is_ok());
    request.workflow_id = "nope".into();
    assert_eq!(status(h.engine.trigger(h.env(), request).await), 422);
    assert!(
        h.engine
            .sync(h.env(), "http://10.0.0.1/api/novu")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_trigger_reaching_too_many_subscribers_stores_nothing() {
    let h = Harness::new(steps(IN_APP), in_app).await;
    let (env, store) = (&h.env().id, &h.state.store);
    let members: Vec<TopicMember> = (0..=MAX_RECIPIENTS)
        .map(|i| TopicMember {
            subscriber_id: format!("s{i}"),
            identifier: format!("s{i}"),
        })
        .collect();
    for m in &members {
        store
            .upsert_subscriber(env, &m.subscriber_id, &SubscriberPatch::default())
            .await
            .unwrap();
    }
    store.subscribe_topic(env, "all", &members).await.unwrap();
    let request = TriggerRequest {
        workflow_id: "wf".into(),
        to: vec![Recipient::Topic { key: "all".into() }],
        payload: json!({}),
        transaction_id: None,
        actor: None,
        bridge_url: None,
        idempotency_key: None,
    };
    let err = h
        .engine
        .trigger(h.env(), request.clone())
        .await
        .unwrap_err();
    assert_eq!(err.status.as_u16(), 400);
    assert!(h.drain(time::now_ms()).await.is_empty());

    store
        .unsubscribe_topic(env, "all", &["s0".into()])
        .await
        .unwrap();
    h.engine.trigger(h.env(), request).await.unwrap();
    let runs = h.state.store.claim_due_runs(time::now_ms(), 5000).await;
    assert_eq!(runs.unwrap().len(), MAX_RECIPIENTS);
}

#[tokio::test]
async fn digests_hand_over_a_bounded_event_list_and_the_full_count() {
    let respond = |step: &str, body: &Value| match step {
        "g" => (
            200,
            json!({"outputs": {"amount": 5, "unit": "minutes", "digestKey": "k"}}),
        ),
        _ => in_app(step, body),
    };
    let h = Harness::new(
        steps(r#"[{"stepId": "g", "type": "digest"}, {"stepId": "a", "type": "in_app"}]"#),
        respond,
    )
    .await;
    let total = runner::MAX_DIGEST_EVENTS + 2;
    for n in 0..total {
        h.trigger(&["s1"], json!({"n": n}), None).await;
        h.tick(time::now_ms()).await;
    }
    h.tick(time::now_ms() + 6 * time::MINUTE).await;
    let digest = &h.calls("a")[0]["state"][0]["outputs"];
    assert_eq!(digest["eventCount"], total);
    let events = digest["events"].as_array().unwrap();
    assert_eq!(events.len(), runner::MAX_DIGEST_EVENTS as usize);
    assert_eq!(events[0]["payload"], json!({"n": 0}));
}

#[test]
fn schema_subset() {
    let schema = json!({
        "type": "object",
        "required": ["id"],
        "additionalProperties": false,
        "properties": {
            "id": {"type": "integer", "minimum": 1},
            "kind": {"enum": ["a", "b"]},
            "note": {"type": ["string", "null"], "maxLength": 3},
            "items": {"type": "array", "items": {"anyOf": [{"type": "string"}, {"const": 1}]}},
        },
    });
    assert!(
        validate(
            &schema,
            &json!({"id": 1, "kind": "a", "note": null, "items": ["x", 1]}),
            "payload"
        )
        .is_ok()
    );
    for bad in [
        json!({}),
        json!({"id": 0}),
        json!({"id": 1.5}),
        json!({"id": 1, "kind": "c"}),
        json!({"id": 1, "note": "long"}),
        json!({"id": 1, "items": [2]}),
        json!({"id": 1, "extra": true}),
    ] {
        assert!(validate(&schema, &bad, "payload").is_err(), "{bad}");
    }
}
