use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::Request as HttpRequest;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use nanovu_core::config::Config;
use nanovu_core::domain::{StepKind, WorkflowPreferences, WorkflowStep};
use nanovu_core::hub::Hub;
use nanovu_core::sqlite::SqliteStore;
use nanovu_core::time::now_ms;
use tokio_tungstenite::tungstenite::{self, Message as WsMessage};
use tower::ServiceExt;

use super::*;

const SUB: &str = "sub-1";

async fn state() -> AppState {
    let dir = std::env::temp_dir().join(format!("nanovu-inbox-{}", Id::new()));
    fs::create_dir_all(&dir).expect("the temp dir is writable");
    let key = dir.join("key");
    fs::write(&key, "0123456789abcdef0123456789abcdef").expect("the temp dir is writable");
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600))
        .expect("the key file was just written");
    let path = dir.join("config.toml");
    let config = format!(
        "database = \"x.db\"\n[environments.stg]\napplication_identifier = \"app\"\nsecret_key_file = \"{}\"\n",
        key.display()
    );
    fs::write(&path, config).expect("the temp dir is writable");
    let config = Config::load(&path).expect("the test config is valid");
    fs::remove_dir_all(&dir).expect("the temp dir was just created");
    let state = AppState {
        environments: Arc::new(config.environments),
        store: Arc::new(
            SqliteStore::open_in_memory()
                .await
                .expect("in-memory SQLite opens"),
        ),
        hub: Hub::default(),
    };
    state
        .store
        .upsert_subscriber(&env(&state).id, SUB, &SubscriberPatch::default())
        .await
        .expect("in-memory SQLite answers");
    state
}

fn env(state: &AppState) -> &Environment {
    state
        .environments
        .by_application_identifier("app")
        .expect("the test config defines app")
}

fn token(state: &AppState) -> String {
    env(state)
        .issue_session(SUB)
        .expect("the test env has a secret key")
}

async fn call(state: &AppState, method: &str, uri: &str, body: Option<Value>) -> (u16, Value) {
    let request = HttpRequest::builder()
        .method(method)
        .uri(uri.replace("[]", "%5B%5D"))
        .header("authorization", format!("Bearer {}", token(state)))
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .expect("the test request is well formed");
    let response = router(state.clone())
        .oneshot(request)
        .await
        .expect("the router is infallible");
    let status = response.status().as_u16();
    let bytes = to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("responses are under 1 MiB");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("the inbox answers JSON")
    };
    (status, value)
}

fn message(tags: &[&str], severity: Severity) -> Message {
    let now = now_ms();
    Message {
        id: Id::new(),
        run_id: None,
        transaction_id: "tx".into(),
        subscriber_id: SUB.into(),
        workflow_id: "wf".into(),
        step_id: "in-app".into(),
        channel: Channel::InApp,
        subject: Some("Hello".into()),
        body: "Body".into(),
        avatar: None,
        redirect: Some(Redirect {
            url: "/x".into(),
            target: None,
        }),
        primary_action: Some(Action {
            label: "Open".into(),
            redirect: None,
        }),
        secondary_action: None,
        data: None,
        tags: tags.iter().map(|t| (*t).to_owned()).collect(),
        severity,
        seen_at: None,
        read_at: None,
        archived_at: None,
        created_at: now,
        updated_at: now,
    }
}

async fn insert(state: &AppState, message: &Message) {
    state
        .store
        .insert_message(&env(state).id, message)
        .await
        .expect("in-memory SQLite answers");
}

fn workflow(identifier: &str, critical: bool, steps: &[StepKind]) -> Workflow {
    Workflow {
        id: Id::new(),
        workflow_id: identifier.into(),
        name: identifier.to_uppercase(),
        description: None,
        tags: vec!["Activity".into()],
        severity: Severity::None,
        preferences: WorkflowPreferences {
            read_only: critical,
            ..WorkflowPreferences::default()
        },
        payload_schema: None,
        steps: steps
            .iter()
            .map(|kind| WorkflowStep {
                step_id: format!("{kind:?}"),
                kind: *kind,
            })
            .collect(),
        bridge_url: "http://localhost/api/novu".into(),
        created_at: now_ms(),
        updated_at: now_ms(),
    }
}

#[tokio::test]
async fn renders_message_shape() {
    let app = Router::new()
        .route("/v1/inbox/x", get(|| async { HttpError::unauthorized() }))
        .layer(middleware::from_fn(render_error));
    let response = app
        .oneshot(HttpRequest::get("/v1/inbox/x").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], br#"{"message":"Unauthorized"}"#);
}

#[tokio::test]
async fn session_checks_hash_and_upserts() {
    let state = state().await;
    let hash = env(&state).subscriber_hash("sub-2");
    let body = |app: &str, hash: &str| {
        json!({
            "applicationIdentifier": app,
            "subscriberHash": hash,
            "subscriber": { "subscriberId": "sub-2", "firstName": "Ada", "timezone": "UTC" },
        })
    };
    insert(&state, &message(&[], Severity::High)).await;

    let (status, response) = call(
        &state,
        "POST",
        "/v1/inbox/session",
        Some(body("app", &hash)),
    )
    .await;
    assert_eq!(status, 201);
    let data = &response["data"];
    let keys: Vec<_> = data.as_object().unwrap().keys().cloned().collect();
    assert_eq!(
        keys,
        [
            "token",
            "totalUnreadCount",
            "unreadCount",
            "removeNovuBranding",
            "isDevelopmentMode",
            "maxSnoozeDurationHours",
            "applicationIdentifier",
            "contextKeys"
        ]
    );
    let (_, subscriber) = state
        .environments
        .verify_session(data["token"].as_str().unwrap())
        .unwrap();
    assert_eq!(subscriber, "sub-2");
    assert_eq!(data["totalUnreadCount"], 0);
    assert_eq!(
        data["unreadCount"],
        json!({"total": 0, "severity": {"high": 0, "medium": 0, "low": 0, "none": 0}})
    );
    let stored = state
        .store
        .subscriber(&env(&state).id, "sub-2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.first_name, None);
    assert_eq!(stored.timezone.as_deref(), Some("UTC"));

    let (status, response) = call(
        &state,
        "POST",
        "/v1/inbox/session",
        Some(body("app", "nope")),
    )
    .await;
    assert_eq!(
        (status, response),
        (401, json!({"message": "Unauthorized"}))
    );
    let (status, _) = call(
        &state,
        "POST",
        "/v1/inbox/session",
        Some(body("other", &hash)),
    )
    .await;
    assert_eq!(status, 400);

    let oversized = json!({"applicationIdentifier": "app", "pad": "x".repeat(8 << 10)});
    let (status, _) = call(&state, "POST", "/v1/inbox/session", Some(oversized)).await;
    assert_eq!(status, 413);

    let sub1 = env(&state).subscriber_hash(SUB);
    let legacy =
        json!({"applicationIdentifier": "app", "subscriberHash": sub1, "subscriberId": SUB});
    let (status, response) = call(&state, "POST", "/v1/inbox/session", Some(legacy)).await;
    assert_eq!(status, 201);
    assert_eq!(response["data"]["unreadCount"]["severity"]["high"], 1);
}

#[tokio::test]
async fn lists_with_filters_and_cursor() {
    let state = state().await;
    let mut old = message(&["Activity"], Severity::High);
    old.data = Some(json!({"kind": "invite"}).as_object().unwrap().clone());
    insert(&state, &old).await;
    let mut read = message(&["Other"], Severity::None);
    read.read_at = Some(now_ms());
    read.seen_at = read.read_at;
    insert(&state, &read).await;
    let mut archived = message(&["Activity"], Severity::Low);
    archived.archived_at = Some(now_ms());
    archived.read_at = archived.archived_at;
    archived.seen_at = archived.archived_at;
    insert(&state, &archived).await;

    let ids = |v: &Value| -> Vec<String> {
        v["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap().to_owned())
            .collect()
    };
    let (old_id, read_id, archived_id) = (
        old.id.to_string(),
        read.id.to_string(),
        archived.id.to_string(),
    );

    let (status, all) = call(&state, "GET", "/v1/inbox/notifications", None).await;
    assert_eq!(status, 200);
    assert_eq!(
        ids(&all),
        [archived_id.as_str(), read_id.as_str(), old_id.as_str()]
    );
    assert_eq!(all["hasMore"], false);
    assert_eq!(all["filter"], json!({}));
    let first = &all["data"][2];
    assert_eq!(first["subject"], "Hello");
    assert_eq!(first["to"]["subscriberId"], SUB);
    assert_eq!(first["isRead"], false);
    assert_eq!(first["isSnoozed"], false);
    assert_eq!(first["channelType"], "in_app");
    assert_eq!(first["severity"], "high");
    assert_eq!(first["redirect"], json!({"url": "/x"}));
    assert_eq!(
        first["primaryAction"],
        json!({"label": "Open", "isCompleted": false})
    );
    assert_eq!(first["data"], json!({"kind": "invite"}));
    assert!(first.get("readAt").is_none());

    let cases = [
        (
            "tags[]=Activity",
            vec![archived_id.as_str(), old_id.as_str()],
        ),
        (
            "tags[]=Activity&tags[]=Other",
            vec![archived_id.as_str(), read_id.as_str(), old_id.as_str()],
        ),
        ("read=false", vec![old_id.as_str()]),
        ("seen=true", vec![archived_id.as_str(), read_id.as_str()]),
        ("archived=false&tags[]=Activity", vec![old_id.as_str()]),
        (
            "severity[]=low&severity[]=none",
            vec![archived_id.as_str(), read_id.as_str()],
        ),
        ("severity=high", vec![old_id.as_str()]),
        (
            "data=%7B%22kind%22%3A%22invite%22%7D",
            vec![old_id.as_str()],
        ),
        ("snoozed=true", vec![]),
        (
            "tags[0][]=Activity&tags[0][]=Other&tags[1][]=Activity",
            vec![archived_id.as_str(), old_id.as_str()],
        ),
        ("tags[0][]=Activity&tags[1][]=Other", vec![]),
        (
            "data=%7B%22kind%22%3A%7B%22or%22%3A%5B%22invite%22%2C%22x%22%5D%7D%7D",
            vec![old_id.as_str()],
        ),
    ];
    for (query, expected) in cases {
        let (status, page) = call(
            &state,
            "GET",
            &format!("/v1/inbox/notifications?{query}"),
            None,
        )
        .await;
        assert_eq!(status, 200, "{query}");
        assert_eq!(ids(&page), expected, "{query}");
    }
    let (_, page) = call(
        &state,
        "GET",
        "/v1/inbox/notifications?archived=false&tags[]=Activity",
        None,
    )
    .await;
    assert_eq!(
        page["filter"],
        json!({"archived": false, "tags": ["Activity"]})
    );
    let (_, page) = call(
        &state,
        "GET",
        "/v1/inbox/notifications?tags[1][]=b&tags[0][]=a",
        None,
    )
    .await;
    assert_eq!(
        page["filter"],
        json!({"tags": {"and": [{"or": ["a"]}, {"or": ["b"]}]}})
    );

    let (_, page) = call(&state, "GET", "/v1/inbox/notifications?limit=1", None).await;
    assert_eq!(ids(&page), [archived_id.as_str()]);
    assert_eq!(page["hasMore"], true);
    let (_, page) = call(
        &state,
        "GET",
        &format!("/v1/inbox/notifications?limit=1&after={archived_id}"),
        None,
    )
    .await;
    assert_eq!(ids(&page), [read_id.as_str()]);
    let (_, page) = call(
        &state,
        "GET",
        "/v1/inbox/notifications?limit=1&offset=2",
        None,
    )
    .await;
    assert_eq!(ids(&page), [old_id.as_str()]);

    for query in [
        "read=maybe",
        "after=nope",
        "severity=urgent",
        "createdGte=1",
        "tags[30][]=a",
        "tags[]=a&tags[0][]=b",
        "data=%7B%22%24where%22%3A1%7D",
    ] {
        let (status, _) = call(
            &state,
            "GET",
            &format!("/v1/inbox/notifications?{query}"),
            None,
        )
        .await;
        assert_eq!(status, 400, "{query}");
    }
    let request = HttpRequest::get("/v1/inbox/notifications")
        .body(Body::empty())
        .unwrap();
    let response = router(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn count_echoes_filters() {
    let state = state().await;
    insert(&state, &message(&["Activity"], Severity::None)).await;
    insert(&state, &message(&["Other"], Severity::High)).await;
    let uri = |filters: Value| {
        format!(
            "/v1/inbox/notifications/count?filters={}",
            filters
                .to_string()
                .replace('"', "%22")
                .replace('{', "%7B")
                .replace('}', "%7D")
                .replace('[', "%5B")
                .replace(']', "%5D")
        )
    };
    let (status, _) = call(&state, "GET", &uri(json!(vec![json!({}); 31])), None).await;
    assert_eq!(status, 400);
    let (status, _) = call(&state, "GET", &uri(json!(vec![json!({}); 30])), None).await;
    assert_eq!(status, 200);
    let long = json!([{"tags": ["x".repeat(8 << 10)]}]);
    let (status, _) = call(&state, "GET", &uri(long), None).await;
    assert_eq!(status, 414);

    for bad in [
        json!([{"tags": {"or": "x"}}]),
        json!([{"tags": {"and": [{"or": []}]}}]),
        json!([{"data": {"a": {"b": {"c": 1}}}}]),
    ] {
        let (status, _) = call(&state, "GET", &uri(bad.clone()), None).await;
        assert_eq!(status, 400, "{bad}");
    }

    let filters = json!([
        {"seen": false, "tags": ["Activity"]},
        {"read": false},
        {"severity": ["high"], "archived": true},
        {"tags": {"or": ["Activity", "Other"]}},
        {"tags": {"and": [{"or": ["Activity"]}, {"or": ["Other"]}]}},
    ]);
    let (status, response) = call(&state, "GET", &uri(filters), None).await;
    assert_eq!(status, 200);
    assert_eq!(
        response,
        json!({"data": [
            {"count": 1, "filter": {"seen": false, "tags": ["Activity"]}},
            {"count": 2, "filter": {"read": false}},
            {"count": 0, "filter": {"severity": ["high"], "archived": true}},
            {"count": 2, "filter": {"tags": {"or": ["Activity", "Other"]}}},
            {"count": 0, "filter": {"tags": {"and": [{"or": ["Activity"]}, {"or": ["Other"]}]}}},
        ]})
    );
}

#[tokio::test]
async fn marks_respond_and_publish() {
    let state = state().await;
    let mut events = state.hub.subscribe(&env(&state).id, SUB).unwrap();
    let a = message(&["Activity"], Severity::None);
    let b = message(&["Other"], Severity::None);
    let c = message(&["Activity"], Severity::None);
    insert(&state, &a).await;
    insert(&state, &b).await;
    insert(&state, &c).await;

    let (status, response) = call(
        &state,
        "PATCH",
        &format!("/v1/inbox/notifications/{}/read", a.id),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(response["data"]["id"], a.id.to_string());
    assert_eq!(response["data"]["isRead"], true);
    assert_eq!(response["data"]["isSeen"], true);
    assert!(response["data"]["readAt"].is_string());
    assert!(matches!(events.recv().await, Some(Event::CountsChanged)));

    let (status, response) = call(
        &state,
        "PATCH",
        &format!("/v1/inbox/notifications/{}/archive", a.id),
        None,
    )
    .await;
    assert_eq!(
        (status, &response["data"]["isArchived"]),
        (200, &json!(true))
    );
    assert!(matches!(events.recv().await, Some(Event::CountsChanged)));

    let (status, _) = call(
        &state,
        "PATCH",
        &format!("/v1/inbox/notifications/{}/snooze", a.id),
        None,
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = call(
        &state,
        "PATCH",
        &format!("/v1/inbox/notifications/{}/read", Id::new()),
        None,
    )
    .await;
    assert_eq!(status, 404);

    let none = json!({"notificationIds": []});
    let (status, _) = call(&state, "POST", "/v1/inbox/notifications/seen", Some(none)).await;
    assert_eq!(status, 204);
    let stored = state
        .store
        .message(&env(&state).id, SUB, &c.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!stored.is_seen());

    let seen = json!({"notificationIds": [b.id.to_string()]});
    let (status, _) = call(&state, "POST", "/v1/inbox/notifications/seen", Some(seen)).await;
    assert_eq!(status, 204);
    assert!(matches!(events.recv().await, Some(Event::CountsChanged)));
    let stored = state
        .store
        .message(&env(&state).id, SUB, &b.id)
        .await
        .unwrap()
        .unwrap();
    assert!(stored.is_seen() && !stored.is_read());

    let (status, _) = call(
        &state,
        "POST",
        "/v1/inbox/notifications/read",
        Some(json!({"tags": {"and": [{"or": ["Other", "x"]}]}, "data": "{}"})),
    )
    .await;
    assert_eq!(status, 204);
    assert!(matches!(events.recv().await, Some(Event::CountsChanged)));
    let stored = state
        .store
        .message(&env(&state).id, SUB, &b.id)
        .await
        .unwrap()
        .unwrap();
    assert!(stored.is_read() && !stored.is_archived());
    let untouched = state
        .store
        .message(&env(&state).id, SUB, &c.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!untouched.is_read());

    let (status, _) = call(
        &state,
        "POST",
        "/v1/inbox/notifications/read-archive",
        Some(json!({})),
    )
    .await;
    assert_eq!(status, 204);
    assert!(matches!(events.recv().await, Some(Event::CountsChanged)));
    let stored = state
        .store
        .message(&env(&state).id, SUB, &b.id)
        .await
        .unwrap()
        .unwrap();
    assert!(stored.is_archived());

    let (status, _) = call(&state, "POST", "/v1/inbox/notifications/archive", None).await;
    assert_eq!(status, 204);
    let (status, _) = call(
        &state,
        "POST",
        "/v1/inbox/notifications/read",
        Some(json!({"data": "nope"})),
    )
    .await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn preferences_merge_and_guard_critical() {
    let state = state().await;
    let env_id = env(&state).id.clone();
    let workflows = [
        workflow("comments", false, &[StepKind::InApp, StepKind::Push]),
        workflow("security", true, &[StepKind::InApp, StepKind::Email]),
    ];
    let stored = state
        .store
        .replace_workflows(&env_id, &workflows)
        .await
        .unwrap();
    let comments_id = stored
        .iter()
        .find(|w| w.workflow_id == "comments")
        .unwrap()
        .id
        .to_string();

    let (status, response) = call(&state, "GET", "/v1/inbox/preferences", None).await;
    assert_eq!(status, 200);
    let data = response["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(
        data[0],
        json!({"level": "global", "enabled": true, "channels": {"in_app": true}})
    );
    assert_eq!(data[1]["level"], "template");
    assert_eq!(data[1]["channels"], json!({"in_app": true, "push": true}));
    assert_eq!(
        data[1]["workflow"],
        json!({"id": comments_id, "identifier": "comments", "name": "COMMENTS", "critical": false, "tags": ["Activity"], "severity": "none"})
    );

    let (_, response) = call(&state, "GET", "/v1/inbox/preferences?criticality=all", None).await;
    assert_eq!(response["data"].as_array().unwrap().len(), 3);
    let (_, response) = call(
        &state,
        "GET",
        "/v1/inbox/preferences?criticality=critical",
        None,
    )
    .await;
    assert_eq!(response["data"][1]["workflow"]["identifier"], "security");
    assert_eq!(
        response["data"][1]["channels"],
        json!({"in_app": true, "email": true})
    );
    let (_, response) = call(&state, "GET", "/v1/inbox/preferences?tags[]=Nope", None).await;
    assert_eq!(response["data"].as_array().unwrap().len(), 1);

    let (status, response) = call(
        &state,
        "PATCH",
        &format!("/v1/inbox/preferences/{comments_id}"),
        Some(json!({"push": false})),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        response["data"]["channels"],
        json!({"in_app": true, "push": false})
    );
    assert_eq!(response["data"]["enabled"], true);

    let (status, response) = call(
        &state,
        "PATCH",
        "/v1/inbox/preferences",
        Some(json!({"in_app": false, "schedule": {}})),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(response["data"]["channels"], json!({"in_app": false}));
    let (_, response) = call(&state, "GET", "/v1/inbox/preferences", None).await;
    assert_eq!(
        response["data"][1]["channels"],
        json!({"in_app": false, "push": false})
    );
    assert_eq!(response["data"][1]["enabled"], false);

    let (status, _) = call(
        &state,
        "PATCH",
        "/v1/inbox/preferences/security",
        Some(json!({"email": false})),
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = call(
        &state,
        "PATCH",
        "/v1/inbox/preferences/missing",
        Some(json!({"email": false})),
    )
    .await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn cors_preflight() {
    let state = state().await;
    let request = HttpRequest::builder()
        .method("OPTIONS")
        .uri("/v1/inbox/notifications")
        .header("origin", "http://localhost:8081")
        .header(
            "access-control-request-headers",
            "authorization,novu-api-version",
        )
        .body(Body::empty())
        .unwrap();
    let response = router(state.clone()).oneshot(request).await.unwrap();
    assert_eq!(response.status(), 204);
    let headers = response.headers();
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert_eq!(
        headers["access-control-allow-headers"],
        "authorization,novu-api-version"
    );
    assert!(headers.get("access-control-allow-credentials").is_none());
}

#[tokio::test]
async fn websocket_round_trip() {
    let state = state().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    let url = |token: &str| format!("ws://{addr}/?token={token}");

    let (mut socket, _) = tokio_tungstenite::connect_async(url(&token(&state)))
        .await
        .unwrap();
    socket.send(WsMessage::text("ping")).await.unwrap();
    assert_eq!(
        socket.next().await.unwrap().unwrap(),
        WsMessage::text("pong")
    );
    socket.send(WsMessage::text("{\"hello\":1}")).await.unwrap();

    let mut message = message(&["Activity"], Severity::High);
    message.primary_action = Some(Action {
        label: "Open".into(),
        redirect: Some(Redirect {
            url: "https://x".into(),
            target: Some("_blank".into()),
        }),
    });
    insert(&state, &message).await;
    state.hub.publish(
        &env(&state).id,
        SUB,
        Event::MessageCreated(Arc::new(message.clone())),
    );
    let mut next = async || -> Value {
        let frame = socket.next().await.unwrap().unwrap();
        serde_json::from_str(frame.to_text().unwrap()).unwrap()
    };
    let received = next().await;
    assert_eq!(received["event"], "notification_received");
    let raw = &received["data"]["message"];
    assert_eq!(raw["_id"], message.id.to_string());
    assert_eq!(raw["content"], "Body");
    assert_eq!(raw["read"], false);
    assert_eq!(raw["subscriber"]["subscriberId"], SUB);
    assert_eq!(raw["cta"]["data"], json!({"url": "/x", "target": null}));
    assert_eq!(
        raw["cta"]["action"]["buttons"],
        json!([{"type": "primary", "content": "Open", "url": "https://x", "target": "_blank"}])
    );
    let unread = next().await;
    assert_eq!(
        unread,
        json!({"event": "unread_count_changed", "data": {
            "counts": {"total": 1, "severity": {"high": 1, "medium": 0, "low": 0, "none": 0}},
            "unreadCount": 1,
        }})
    );
    assert_eq!(
        next().await,
        json!({"event": "unseen_count_changed", "data": {"unseenCount": 1}})
    );

    state
        .hub
        .publish(&env(&state).id, SUB, Event::CountsChanged);
    assert_eq!(next().await["event"], "unread_count_changed");
    assert_eq!(next().await["event"], "unseen_count_changed");

    let status = |err: tungstenite::Error| match err {
        tungstenite::Error::Http(response) => response.status().as_u16(),
        other => panic!("unexpected {other}"),
    };
    let refused = tokio_tungstenite::connect_async(url("bad"))
        .await
        .unwrap_err();
    assert_eq!(status(refused), 401);

    let held: Vec<_> = (0..4)
        .map(|_| state.hub.subscribe(&env(&state).id, SUB).unwrap())
        .collect();
    let refused = tokio_tungstenite::connect_async(url(&token(&state)))
        .await
        .unwrap_err();
    assert_eq!(status(refused), 429);
    drop(held);

    let big = "x".repeat(MAX_TEST_FRAME);
    socket.send(WsMessage::text(big)).await.unwrap();
    assert!(matches!(
        socket.next().await,
        Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None
    ));
}

const MAX_TEST_FRAME: usize = 8 * 1024;

#[tokio::test]
async fn websocket_closes_idle_connections() {
    let state = state().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    let url = format!("ws://{addr}/ws?token={}", token(&state));
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    tokio::time::pause();
    assert!(matches!(
        socket.next().await,
        Some(Ok(WsMessage::Close(_)) | Err(_)) | None
    ));
}

#[tokio::test]
async fn a_deleted_subscriber_is_refused() {
    let state = state().await;
    let token = token(&state);
    assert!(
        state
            .store
            .delete_subscriber(&env(&state).id, SUB)
            .await
            .unwrap()
    );
    let (status, _) = call(&state, "GET", "/v1/inbox/notifications", None).await;
    assert_eq!(status, 401);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    let refused = tokio_tungstenite::connect_async(format!("ws://{addr}/?token={token}"))
        .await
        .unwrap_err();
    assert!(matches!(refused, tungstenite::Error::Http(r) if r.status() == 401));
}

#[test]
fn query_parsers_keep_the_last_value_and_reject_any_invalid_one() {
    let pairs = |items: &[(&str, &str)]| {
        items
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect::<Vec<_>>()
    };
    let (page, raw) =
        list_query(pairs(&[("limit", "5"), ("limit", "7"), ("bogus", "x")])).expect("valid limits");
    assert_eq!((page.limit, raw.len()), (7, 0));
    let prefs = preferences_query(pairs(&[
        ("criticality", "all"),
        ("criticality", "critical"),
    ]))
    .expect("valid criticality");
    assert_eq!(prefs.critical, Some(true));
    for (a, b) in [("x", "5"), ("5", "x")] {
        assert!(list_query(pairs(&[("limit", a), ("limit", b)])).is_err());
        let read = [a, b].map(|v| ("read", if v == "x" { "{" } else { "true" }));
        assert!(list_query(pairs(&read)).is_err());
    }
    for order in [["nope", "all"], ["all", "nope"]] {
        let criticality = order.map(|v| ("criticality", v));
        assert!(preferences_query(pairs(&criticality)).is_err());
    }
}
