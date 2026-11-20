mod ws;

use axum::extract::{DefaultBodyLimit, FromRequestParts, Path, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header, request::Parts};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use nanovu_core::domain::{
    Action, Channel, ChannelPrefs, InboxPage, Mark, Message, MessageFilter, Redirect, Severity,
    Subscriber, SubscriberPatch, Workflow,
};
use nanovu_core::time::{iso, iso_ser, iso_ser_opt};
use nanovu_core::{AppState, Environment, Event, HttpError, Id, JsonBody, QueryParams};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

const BODY_LIMIT: usize = 8 << 10;
const MAX_COUNT_FILTERS: usize = 30;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/inbox/session", post(session))
        .route("/v1/inbox/notifications", get(list))
        .route("/v1/inbox/notifications/count", get(count))
        .route("/v1/inbox/notifications/{action}", post(mark_all))
        .route("/v1/inbox/notifications/{id}/{action}", patch(mark_one))
        .route(
            "/v1/inbox/preferences",
            get(preferences).patch(update_global),
        )
        .route("/v1/inbox/preferences/{workflow}", patch(update_workflow))
        .route("/", get(ws::upgrade))
        .route("/ws", get(ws::upgrade))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn(render_error))
        .layer(middleware::from_fn(cors))
        .with_state(state)
}

async fn render_error(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    match response.extensions_mut().remove::<HttpError>() {
        Some(err) => (err.status, Json(json!({ "message": err.message }))).into_response(),
        None => response,
    }
}

async fn cors(request: Request, next: Next) -> Response {
    let mut response = if request.method() == Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        let headers = response.headers_mut();
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, PATCH, DELETE, OPTIONS"),
        );
        if let Some(requested) = request
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        {
            headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, requested.clone());
        }
        headers.insert(
            header::ACCESS_CONTROL_MAX_AGE,
            HeaderValue::from_static("86400"),
        );
        response
    } else {
        next.run(request).await
    };
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}

struct Auth {
    env: Id,
    subscriber_id: String,
}

impl FromRequestParts<AppState> for Auth {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, HttpError> {
        let token = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(HttpError::unauthorized)?;
        let (env, subscriber_id) = authenticate(state, token).await?;
        Ok(Self { env, subscriber_id })
    }
}

async fn authenticate(state: &AppState, token: &str) -> Result<(Id, String), HttpError> {
    let (env, subscriber_id) = state
        .environments
        .verify_session(token)
        .ok_or_else(HttpError::unauthorized)?;
    state
        .store
        .subscriber(&env.id, &subscriber_id)
        .await?
        .ok_or_else(HttpError::unauthorized)?;
    Ok((env.id.clone(), subscriber_id))
}

impl Auth {
    fn environment<'a>(&self, state: &'a AppState) -> Result<&'a Environment, HttpError> {
        state
            .environments
            .get(&self.env)
            .ok_or_else(HttpError::unauthorized)
    }

    fn publish(&self, state: &AppState) {
        state
            .hub
            .publish(&self.env, &self.subscriber_id, Event::CountsChanged);
    }
}

struct Counts {
    total: u64,
    unread: Value,
    unseen: u64,
}

async fn counts(state: &AppState, env: &Id, subscriber_id: &str) -> Result<Counts, HttpError> {
    let unread = |severity: Vec<Severity>| MessageFilter {
        read: Some(false),
        severity,
        ..MessageFilter::default()
    };
    let mut filters = vec![unread(Vec::new())];
    filters.extend(Severity::ALL.map(|s| unread(vec![s])));
    filters.push(MessageFilter {
        seen: Some(false),
        ..MessageFilter::default()
    });
    let counts = state
        .store
        .inbox_counts(env, subscriber_id, &filters)
        .await?;
    let [total, high, medium, low, none, unseen] = counts[..] else {
        return Err(HttpError::internal());
    };
    Ok(Counts {
        total,
        unread: json!({
            "total": total,
            "severity": { "high": high, "medium": medium, "low": low, "none": none },
        }),
        unseen,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionRequest {
    application_identifier: String,
    subscriber_hash: Option<String>,
    subscriber_id: Option<String>,
    subscriber: Option<SessionSubscriber>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SessionSubscriber {
    subscriber_id: Option<String>,
    locale: Option<String>,
    timezone: Option<String>,
}

async fn session(
    State(state): State<AppState>,
    JsonBody(body): JsonBody<SessionRequest>,
) -> Result<Response, HttpError> {
    let env = state
        .environments
        .by_application_identifier(&body.application_identifier)
        .ok_or_else(|| HttpError::bad_request("Unknown applicationIdentifier"))?;
    let subscriber = body.subscriber.unwrap_or_default();
    let subscriber_id = subscriber
        .subscriber_id
        .or(body.subscriber_id)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| HttpError::bad_request("subscriberId is required"))?;
    let hash = body.subscriber_hash.unwrap_or_default();
    if !env.verify_subscriber_hash(&subscriber_id, &hash) {
        return Err(HttpError::unauthorized());
    }
    let patch = SubscriberPatch {
        locale: subscriber.locale,
        timezone: subscriber.timezone,
        ..SubscriberPatch::default()
    };
    state
        .store
        .upsert_subscriber(&env.id, &subscriber_id, &patch)
        .await?;
    let token = env.issue_session(&subscriber_id)?;
    let counts = counts(&state, &env.id, &subscriber_id).await?;
    let body = json!({ "data": {
        "token": token,
        "totalUnreadCount": counts.total,
        "unreadCount": counts.unread,
        "removeNovuBranding": true,
        "isDevelopmentMode": false,
        "maxSnoozeDurationHours": 0,
        "applicationIdentifier": env.application_identifier,
        "contextKeys": [],
    }});
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

fn invalid(field: &str) -> HttpError {
    HttpError::bad_request(format!("Invalid {field}"))
}

fn filter_flag(raw: &Map<String, Value>, field: &str) -> Result<Option<bool>, HttpError> {
    match raw.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(invalid(field)),
    }
}

fn filter_strings(raw: &Map<String, Value>, field: &str) -> Result<Vec<String>, HttpError> {
    match raw.get(field) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(vec![s.clone()]),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| v.as_str().map(str::to_owned).ok_or_else(|| invalid(field)))
            .collect(),
        Some(_) => Err(invalid(field)),
    }
}

fn parse_filter(raw: &Map<String, Value>) -> Result<MessageFilter, HttpError> {
    if raw.contains_key("createdGte") || raw.contains_key("createdLte") {
        return Err(HttpError::bad_request(
            "createdGte and createdLte are not supported",
        ));
    }
    let flag = |field: &str| filter_flag(raw, field);
    let strings = |field: &str| filter_strings(raw, field);
    Ok(MessageFilter {
        tags: strings("tags")?,
        read: flag("read")?,
        seen: flag("seen")?,
        archived: flag("archived")?,
        snoozed: flag("snoozed")?,
        data: match raw.get("data") {
            None | Some(Value::Null) => None,
            Some(Value::Object(data)) => Some(data.clone()),
            Some(_) => return Err(invalid("data")),
        },
        severity: strings("severity")?
            .iter()
            .map(|s| Severity::parse(s).ok_or_else(|| invalid("severity")))
            .collect::<Result<_, _>>()?,
    })
}

async fn list(
    State(state): State<AppState>,
    auth: Auth,
    QueryParams(query): QueryParams<Vec<(String, String)>>,
) -> Result<Json<Value>, HttpError> {
    let (page, raw) = list_query(query)?;
    let filter = parse_filter(&raw)?;
    let (messages, has_more) = state
        .store
        .inbox(&auth.env, &auth.subscriber_id, &filter, &page)
        .await?;
    let context = Context::load(&state, &auth.env, &auth.subscriber_id).await?;
    let data: Vec<_> = messages.iter().map(|m| context.notification(m)).collect();
    Ok(Json(
        json!({ "data": data, "hasMore": has_more, "filter": raw }),
    ))
}

fn list_query(query: Vec<(String, String)>) -> Result<(InboxPage, Map<String, Value>), HttpError> {
    let mut page = InboxPage {
        limit: 10,
        offset: 0,
        after: None,
    };
    let mut raw = Map::new();
    for (key, value) in query {
        match key.as_str() {
            "limit" => {
                page.limit = value
                    .parse::<u32>()
                    .map_err(|_| invalid("limit"))?
                    .clamp(1, 100);
            }
            "offset" => page.offset = value.parse().map_err(|_| invalid("offset"))?,
            "after" => page.after = Some(Id::parse(&value).ok_or_else(|| invalid("after"))?),
            _ => filter_param(&mut raw, key, value)?,
        }
    }
    Ok((page, raw))
}

fn filter_param(raw: &mut Map<String, Value>, key: String, value: String) -> Result<(), HttpError> {
    if matches!(key.as_str(), "tags" | "tags[]" | "severity" | "severity[]") {
        let entry = raw
            .entry(key.trim_end_matches("[]"))
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Value::Array(items) = entry {
            items.push(Value::String(value));
        }
    } else if matches!(
        key.as_str(),
        "read" | "seen" | "archived" | "snoozed" | "data"
    ) {
        let parsed = serde_json::from_str(&value).map_err(|_| invalid(&key))?;
        raw.insert(key, parsed);
    } else if matches!(key.as_str(), "createdGte" | "createdLte") {
        raw.insert(key, Value::String(value));
    }
    Ok(())
}

#[derive(Deserialize)]
struct CountQuery {
    filters: String,
}

async fn count(
    State(state): State<AppState>,
    auth: Auth,
    QueryParams(query): QueryParams<CountQuery>,
) -> Result<Json<Value>, HttpError> {
    let raw: Vec<Map<String, Value>> = serde_json::from_str(&query.filters)
        .map_err(|_| HttpError::bad_request("Invalid filters"))?;
    if raw.len() > MAX_COUNT_FILTERS {
        return Err(HttpError::bad_request(format!(
            "filters must contain at most {MAX_COUNT_FILTERS} items"
        )));
    }
    let filters = raw
        .iter()
        .map(parse_filter)
        .collect::<Result<Vec<_>, _>>()?;
    let counts = state
        .store
        .inbox_counts(&auth.env, &auth.subscriber_id, &filters)
        .await?;
    let data: Vec<_> = counts
        .into_iter()
        .zip(raw)
        .map(|(count, filter)| json!({ "count": count, "filter": filter }))
        .collect();
    Ok(Json(json!({ "data": data })))
}

async fn mark_one(
    State(state): State<AppState>,
    auth: Auth,
    Path((id, action)): Path<(String, String)>,
) -> Result<Json<Value>, HttpError> {
    let mark = match action.as_str() {
        "read" => Mark::Read,
        "unread" => Mark::Unread,
        "archive" => Mark::Archive,
        "unarchive" => Mark::Unarchive,
        "snooze" | "unsnooze" => return Err(HttpError::bad_request("Snooze is not supported")),
        _ => return Err(HttpError::not_found("Not found")),
    };
    let not_found = || HttpError::not_found("Notification not found");
    let id = Id::parse(&id).ok_or_else(not_found)?;
    let messages = state
        .store
        .mark_messages(&auth.env, &auth.subscriber_id, &[id], mark)
        .await?;
    let message = messages.first().ok_or_else(not_found)?;
    auth.publish(&state);
    let context = Context::load(&state, &auth.env, &auth.subscriber_id).await?;
    Ok(Json(json!({ "data": context.notification(message) })))
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct BulkRequest {
    #[serde(default)]
    notification_ids: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
    data: Option<String>,
}

async fn mark_all(
    State(state): State<AppState>,
    auth: Auth,
    Path(action): Path<String>,
    JsonBody(body): JsonBody<Option<BulkRequest>>,
) -> Result<StatusCode, HttpError> {
    let (mark, read) = match action.as_str() {
        "read" => (Mark::Read, None),
        "seen" => (Mark::Seen, None),
        "archive" => (Mark::Archive, None),
        "read-archive" => (Mark::Archive, Some(true)),
        _ => return Err(HttpError::not_found("Not found")),
    };
    let body = body.unwrap_or_default();
    if body.notification_ids.is_empty() {
        let data = body
            .data
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_| HttpError::bad_request("Invalid data"))?;
        let filter = MessageFilter {
            tags: body.tags,
            read,
            data,
            ..MessageFilter::default()
        };
        state
            .store
            .mark_inbox(&auth.env, &auth.subscriber_id, &filter, mark)
            .await?;
    } else {
        let ids = body
            .notification_ids
            .iter()
            .map(|id| {
                Id::parse(id).ok_or_else(|| HttpError::bad_request("Invalid notificationIds"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        state
            .store
            .mark_messages(&auth.env, &auth.subscriber_id, &ids, mark)
            .await?;
    }
    auth.publish(&state);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct Preference<'a> {
    level: &'static str,
    enabled: bool,
    channels: ChannelPrefs,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow: Option<WorkflowRef<'a>>,
}

impl Preference<'_> {
    fn global(env: &Environment, stored: &ChannelPrefs) -> Self {
        let mut channels = ChannelPrefs::default();
        for channel in Channel::ALL {
            if channel == Channel::InApp || env.channel_integrations(channel).next().is_some() {
                channels.set(channel, Some(stored.get(channel).unwrap_or(true)));
            }
        }
        Preference {
            level: "global",
            enabled: true,
            channels,
            workflow: None,
        }
    }
}

impl<'a> Preference<'a> {
    fn workflow(workflow: &'a Workflow, global: &ChannelPrefs, own: &ChannelPrefs) -> Self {
        let channels = workflow.resolve_channels(global, own);
        Preference {
            level: "template",
            enabled: Channel::ALL.iter().any(|c| channels.get(*c) == Some(true)),
            channels,
            workflow: Some(WorkflowRef::new(workflow)),
        }
    }
}

struct PreferencesQuery {
    tags: Vec<String>,
    severity: Vec<Severity>,
    critical: Option<bool>,
}

fn preferences_query(query: Vec<(String, String)>) -> Result<PreferencesQuery, HttpError> {
    let mut tags = Vec::new();
    let mut severity = Vec::new();
    let mut critical = Some(false);
    for (key, value) in query {
        if matches!(key.as_str(), "tags" | "tags[]") {
            tags.push(value);
        } else if matches!(key.as_str(), "severity" | "severity[]") {
            severity.push(
                Severity::parse(&value)
                    .ok_or_else(|| HttpError::bad_request("Invalid severity"))?,
            );
        } else if key == "criticality" {
            critical = match value.as_str() {
                "nonCritical" => Some(false),
                "critical" => Some(true),
                "all" => None,
                _ => return Err(HttpError::bad_request("Invalid criticality")),
            };
        }
    }
    Ok(PreferencesQuery {
        tags,
        severity,
        critical,
    })
}

async fn preferences(
    State(state): State<AppState>,
    auth: Auth,
    QueryParams(query): QueryParams<Vec<(String, String)>>,
) -> Result<Json<Value>, HttpError> {
    let PreferencesQuery {
        tags,
        severity,
        critical,
    } = preferences_query(query)?;
    let env = auth.environment(&state)?;
    let stored = state
        .store
        .subscriber_preferences(&auth.env, &auth.subscriber_id)
        .await?;
    let mut workflows = state.store.workflows(&auth.env).await?;
    workflows.sort_by_key(|w| w.created_at);
    let none = ChannelPrefs::default();
    let mut data = vec![Preference::global(env, &stored.global)];
    data.extend(
        workflows
            .iter()
            .filter(|w| critical.is_none_or(|c| w.is_critical() == c))
            .filter(|w| tags.is_empty() || w.tags.iter().any(|t| tags.contains(t)))
            .filter(|w| severity.is_empty() || severity.contains(&w.severity))
            .map(|w| {
                let own = stored.workflows.get(&w.workflow_id).unwrap_or(&none);
                Preference::workflow(w, &stored.global, own)
            }),
    );
    Ok(Json(json!({ "data": data })))
}

async fn update_global(
    State(state): State<AppState>,
    auth: Auth,
    JsonBody(patch): JsonBody<ChannelPrefs>,
) -> Result<Json<Value>, HttpError> {
    let env = auth.environment(&state)?;
    let global = state
        .store
        .update_preferences(&auth.env, &auth.subscriber_id, None, &patch)
        .await?;
    Ok(Json(json!({ "data": Preference::global(env, &global) })))
}

async fn update_workflow(
    State(state): State<AppState>,
    auth: Auth,
    Path(key): Path<String>,
    JsonBody(patch): JsonBody<ChannelPrefs>,
) -> Result<Json<Value>, HttpError> {
    let workflow = state
        .store
        .workflow(&auth.env, &key)
        .await?
        .ok_or_else(|| HttpError::not_found("Workflow not found"))?;
    if workflow.is_critical() {
        return Err(HttpError::bad_request(
            "Critical workflow preferences cannot be changed",
        ));
    }
    let own = state
        .store
        .update_preferences(
            &auth.env,
            &auth.subscriber_id,
            Some(&workflow.workflow_id),
            &patch,
        )
        .await?;
    let global = state
        .store
        .subscriber_preferences(&auth.env, &auth.subscriber_id)
        .await?
        .global;
    Ok(Json(
        json!({ "data": Preference::workflow(&workflow, &global, &own) }),
    ))
}

#[derive(Serialize)]
struct WorkflowRef<'a> {
    id: &'a Id,
    identifier: &'a str,
    name: &'a str,
    critical: bool,
    tags: &'a [String],
    severity: Severity,
}

impl<'a> WorkflowRef<'a> {
    fn new(workflow: &'a Workflow) -> Self {
        Self {
            id: &workflow.id,
            identifier: &workflow.workflow_id,
            name: &workflow.name,
            critical: workflow.is_critical(),
            tags: &workflow.tags,
            severity: workflow.severity,
        }
    }
}

#[derive(Serialize)]
struct RedirectDto<'a> {
    url: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<&'a str>,
}

impl<'a> RedirectDto<'a> {
    fn new(redirect: &'a Redirect) -> Self {
        Self {
            url: &redirect.url,
            target: redirect.target.as_deref(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActionDto<'a> {
    label: &'a str,
    is_completed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    redirect: Option<RedirectDto<'a>>,
}

impl<'a> ActionDto<'a> {
    fn new(action: &'a Action) -> Self {
        Self {
            label: &action.label,
            is_completed: false,
            redirect: action.redirect.as_ref().map(RedirectDto::new),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct To<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a Id>,
    subscriber_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InboxNotification<'a> {
    id: &'a Id,
    transaction_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<&'a str>,
    body: &'a str,
    to: To<'a>,
    is_read: bool,
    is_seen: bool,
    is_archived: bool,
    is_snoozed: bool,
    #[serde(serialize_with = "iso_ser")]
    created_at: i64,
    #[serde(
        serialize_with = "iso_ser_opt",
        skip_serializing_if = "Option::is_none"
    )]
    read_at: Option<i64>,
    #[serde(
        serialize_with = "iso_ser_opt",
        skip_serializing_if = "Option::is_none"
    )]
    first_seen_at: Option<i64>,
    #[serde(
        serialize_with = "iso_ser_opt",
        skip_serializing_if = "Option::is_none"
    )]
    archived_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary_action: Option<ActionDto<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secondary_action: Option<ActionDto<'a>>,
    channel_type: &'static str,
    tags: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redirect: Option<RedirectDto<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow: Option<WorkflowRef<'a>>,
    severity: Severity,
}

struct Context {
    subscriber: Option<Subscriber>,
    workflows: Vec<Workflow>,
}

impl Context {
    async fn load(state: &AppState, env: &Id, subscriber_id: &str) -> Result<Self, HttpError> {
        Ok(Self {
            subscriber: state.store.subscriber(env, subscriber_id).await?,
            workflows: state.store.workflows(env).await?,
        })
    }

    fn workflow(&self, message: &Message) -> Option<WorkflowRef<'_>> {
        self.workflows
            .iter()
            .find(|w| w.workflow_id == message.workflow_id)
            .map(WorkflowRef::new)
    }

    fn notification<'a>(&'a self, m: &'a Message) -> InboxNotification<'a> {
        let s = self.subscriber.as_ref();
        InboxNotification {
            id: &m.id,
            transaction_id: &m.transaction_id,
            subject: m.subject.as_deref(),
            body: &m.body,
            to: To {
                id: s.map(|s| &s.id),
                subscriber_id: &m.subscriber_id,
                first_name: s.and_then(|s| s.first_name.as_deref()),
                last_name: s.and_then(|s| s.last_name.as_deref()),
                avatar: s.and_then(|s| s.avatar.as_deref()),
            },
            is_read: m.is_read(),
            is_seen: m.is_seen(),
            is_archived: m.is_archived(),
            is_snoozed: false,
            created_at: m.created_at,
            read_at: m.read_at,
            first_seen_at: m.seen_at,
            archived_at: m.archived_at,
            avatar: m.avatar.as_deref(),
            primary_action: m.primary_action.as_ref().map(ActionDto::new),
            secondary_action: m.secondary_action.as_ref().map(ActionDto::new),
            channel_type: m.channel.as_str(),
            tags: &m.tags,
            data: m.data.as_ref(),
            redirect: m.redirect.as_ref().map(RedirectDto::new),
            workflow: self.workflow(m),
            severity: m.severity,
        }
    }

    fn raw(&self, m: &Message) -> Value {
        let s = self.subscriber.as_ref();
        let button = |kind: &str, action: &Option<Action>| {
            action.as_ref().map(|a| {
                json!({
                    "type": kind,
                    "content": a.label,
                    "url": a.redirect.as_ref().map(|r| &r.url),
                    "target": a.redirect.as_ref().and_then(|r| r.target.as_ref()),
                })
            })
        };
        let buttons: Vec<Value> = [
            button("primary", &m.primary_action),
            button("secondary", &m.secondary_action),
        ]
        .into_iter()
        .flatten()
        .collect();
        json!({
            "_id": m.id,
            "transactionId": m.transaction_id,
            "content": m.body,
            "subject": m.subject,
            "read": m.is_read(),
            "seen": m.is_seen(),
            "archived": m.is_archived(),
            "createdAt": iso(m.created_at),
            "lastReadDate": m.read_at.map(iso),
            "firstSeenDate": m.seen_at.map(iso),
            "archivedAt": m.archived_at.map(iso),
            "channel": m.channel.as_str(),
            "subscriber": {
                "_id": s.map(|s| &s.id),
                "subscriberId": m.subscriber_id,
                "firstName": s.and_then(|s| s.first_name.as_ref()),
                "lastName": s.and_then(|s| s.last_name.as_ref()),
                "avatar": s.and_then(|s| s.avatar.as_ref()),
                "locale": s.and_then(|s| s.locale.as_ref()),
                "timezone": s.and_then(|s| s.timezone.as_ref()),
                "email": s.and_then(|s| s.email.as_ref()),
                "phone": s.and_then(|s| s.phone.as_ref()),
                "data": s.and_then(|s| s.data.as_ref()),
            },
            "avatar": m.avatar,
            "cta": {
                "data": {
                    "url": m.redirect.as_ref().map(|r| &r.url),
                    "target": m.redirect.as_ref().and_then(|r| r.target.as_ref()),
                },
                "action": { "buttons": buttons },
            },
            "tags": m.tags,
            "data": m.data,
            "workflow": self.workflow(m),
            "severity": m.severity,
        })
    }
}

#[cfg(test)]
mod tests;
