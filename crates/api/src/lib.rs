use axum::extract::{DefaultBodyLimit, FromRequestParts, Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header, request::Parts};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Json, Router};
use nanovu_core::domain::{
    Channel, Mark, Message, MessageQuery, Subscriber, SubscriberPatch, SubscriberUpdate, Topic,
    TopicMember, TopicSubscriptions,
};
use nanovu_core::{AppState, Environment, Event, HttpError, Id, JsonBody, QueryParams, time};
use nanovu_engine::{Engine, Recipient, TriggerRequest};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value, json};

const MAX_BATCH: usize = 100;
const BODY_LIMIT: usize = 16 << 10;
const TRIGGER_BODY_LIMIT: usize = 32 << 10;
const MAX_PAGE_SIZE: u32 = 1000;

#[derive(Clone)]
struct Ctx {
    state: AppState,
    engine: Engine,
}

type Reply = Result<(StatusCode, Json<Value>), HttpError>;

pub fn router(state: AppState, engine: Engine) -> Router {
    Router::new()
        .route("/v1/bridge/sync", post(sync))
        .route(
            "/v1/events/trigger",
            post(trigger).layer(DefaultBodyLimit::max(TRIGGER_BODY_LIMIT)),
        )
        .route("/v1/events/trigger/{transaction_id}", delete(cancel))
        .route("/v2/subscribers", post(create_subscriber))
        .route(
            "/v2/subscribers/{subscriber_id}",
            patch(update_subscriber).delete(delete_subscriber),
        )
        .route(
            "/v1/subscribers/{subscriber_id}/credentials",
            put(set_credentials),
        )
        .route(
            "/v1/subscribers/{subscriber_id}/messages/mark-as",
            post(mark_messages),
        )
        .route("/v2/topics", post(upsert_topic))
        .route(
            "/v2/topics/{key}/subscriptions",
            post(subscribe).delete(unsubscribe),
        )
        .route("/v1/messages", get(list_messages))
        .route(
            "/v1/messages/transaction/{transaction_id}",
            delete(delete_transaction),
        )
        .with_state(Ctx { state, engine })
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn(render_error))
}

async fn render_error(request: Request, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    let mut response = next.run(request).await;
    let Some(err) = response.extensions_mut().remove::<HttpError>() else {
        return response;
    };
    let mut body = json!({
        "statusCode": err.status.as_u16(),
        "timestamp": time::iso(time::now_ms()),
        "path": path,
        "message": err.message,
    });
    if err.status == StatusCode::BAD_REQUEST {
        body["type"] = json!("BAD_REQUEST");
        body["errors"] = json!([]);
    } else if err.status == StatusCode::UNPROCESSABLE_ENTITY {
        body["errors"] = json!({});
    }
    (err.status, Json(body)).into_response()
}

struct Caller(Id);

impl FromRequestParts<Ctx> for Caller {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut Parts, ctx: &Ctx) -> Result<Self, HttpError> {
        parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("ApiKey "))
            .and_then(|key| ctx.state.environments.authenticate_api_key(key))
            .map(|env| Self(env.id.clone()))
            .ok_or_else(HttpError::unauthorized)
    }
}

impl Caller {
    fn environment<'a>(&self, state: &'a AppState) -> Result<&'a Environment, HttpError> {
        state
            .environments
            .get(&self.0)
            .ok_or_else(HttpError::unauthorized)
    }
}

fn data(status: StatusCode, data: Value) -> Reply {
    Ok((status, Json(json!({ "data": data }))))
}

fn required(value: Option<String>, field: &str) -> Result<String, HttpError> {
    value
        .filter(|v| !v.is_empty())
        .ok_or_else(|| HttpError::bad_request(format!("{field} is required")))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    Many(Vec<T>),
    One(T),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::Many(v) => v,
            Self::One(v) => vec![v],
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriberBody {
    subscriber_id: Option<String>,
    email: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    phone: Option<String>,
    avatar: Option<String>,
    locale: Option<String>,
    timezone: Option<String>,
    data: Option<Map<String, Value>>,
}

impl SubscriberBody {
    fn into_parts(self) -> (Option<String>, SubscriberPatch) {
        let patch = SubscriberPatch {
            email: self.email,
            first_name: self.first_name,
            last_name: self.last_name,
            phone: self.phone,
            avatar: self.avatar,
            locale: self.locale,
            timezone: self.timezone,
            data: self.data.map(Value::Object),
        };
        (self.subscriber_id, patch)
    }
}

fn nullable<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriberUpdateBody {
    #[serde(default, deserialize_with = "nullable")]
    email: Option<Option<String>>,
    #[serde(default, deserialize_with = "nullable")]
    first_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "nullable")]
    last_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "nullable")]
    phone: Option<Option<String>>,
    #[serde(default, deserialize_with = "nullable")]
    avatar: Option<Option<String>>,
    #[serde(default, deserialize_with = "nullable")]
    locale: Option<Option<String>>,
    #[serde(default, deserialize_with = "nullable")]
    timezone: Option<Option<String>>,
    #[serde(default, deserialize_with = "nullable")]
    data: Option<Option<Map<String, Value>>>,
}

fn subscriber_dto(env: &Id, s: &Subscriber) -> Value {
    let mut dto = json!({
        "_id": s.id,
        "subscriberId": s.subscriber_id,
        "_organizationId": env,
        "_environmentId": env,
        "firstName": s.first_name,
        "lastName": s.last_name,
        "email": s.email,
        "phone": s.phone,
        "avatar": s.avatar,
        "locale": s.locale,
        "timezone": s.timezone,
        "data": s.data,
        "deleted": false,
        "createdAt": time::iso(s.created_at),
        "updatedAt": time::iso(s.updated_at),
    });
    if let Value::Object(fields) = &mut dto {
        fields.retain(|_, v| !v.is_null());
    }
    dto
}

async fn create_subscriber(
    State(ctx): State<Ctx>,
    caller: Caller,
    JsonBody(payload): JsonBody<SubscriberBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let (subscriber_id, patch) = payload.into_parts();
    let subscriber_id = required(subscriber_id, "subscriberId")?;
    let subscriber = ctx
        .state
        .store
        .upsert_subscriber(&env.id, &subscriber_id, &patch)
        .await?;
    data(StatusCode::CREATED, subscriber_dto(&env.id, &subscriber))
}

async fn update_subscriber(
    State(ctx): State<Ctx>,
    caller: Caller,
    Path(subscriber_id): Path<String>,
    JsonBody(payload): JsonBody<SubscriberUpdateBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let update = SubscriberUpdate {
        email: payload.email,
        first_name: payload.first_name,
        last_name: payload.last_name,
        phone: payload.phone,
        avatar: payload.avatar,
        locale: payload.locale,
        timezone: payload.timezone,
        data: payload.data.map(|data| data.map(Value::Object)),
    };
    let subscriber = ctx
        .state
        .store
        .update_subscriber(&env.id, &subscriber_id, &update)
        .await?
        .ok_or_else(|| HttpError::not_found("Subscriber not found"))?;
    data(StatusCode::OK, subscriber_dto(&env.id, &subscriber))
}

async fn delete_subscriber(
    State(ctx): State<Ctx>,
    caller: Caller,
    Path(subscriber_id): Path<String>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    if !ctx
        .state
        .store
        .delete_subscriber(&env.id, &subscriber_id)
        .await?
    {
        return Err(HttpError::not_found("Subscriber not found"));
    }
    ctx.state.hub.close(&env.id, &subscriber_id);
    data(
        StatusCode::OK,
        json!({ "acknowledged": true, "status": "deleted" }),
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialsBody {
    provider_id: String,
    integration_identifier: Option<String>,
    credentials: Credentials,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Credentials {
    #[serde(default)]
    device_tokens: Vec<String>,
}

async fn set_credentials(
    State(ctx): State<Ctx>,
    caller: Caller,
    Path(subscriber_id): Path<String>,
    JsonBody(payload): JsonBody<CredentialsBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let request = payload;
    let integration = env
        .integration(
            &request.provider_id,
            request.integration_identifier.as_deref(),
        )
        .ok_or_else(|| {
            HttpError::bad_request(format!(
                "Subscribers environment ({}) do not have active {} integration.",
                env.id, request.provider_id
            ))
        })?;
    let tokens = request.credentials.device_tokens;
    if tokens.len() > MAX_BATCH {
        return Err(HttpError::bad_request(format!(
            "deviceTokens must contain at most {MAX_BATCH} items"
        )));
    }
    let not_found = || HttpError::bad_request(format!("SubscriberId: {subscriber_id} not found"));
    let store = &ctx.state.store;
    let subscriber = store
        .subscriber(&env.id, &subscriber_id)
        .await?
        .ok_or_else(not_found)?;
    if !store
        .set_device_tokens(&env.id, &subscriber_id, &integration.identifier, &tokens)
        .await?
    {
        return Err(not_found());
    }
    data(StatusCode::OK, subscriber_dto(&env.id, &subscriber))
}

#[derive(Deserialize)]
struct TopicBody {
    key: String,
    name: Option<String>,
}

fn topic_dto(topic: &Topic) -> Value {
    let mut dto = json!({
        "_id": topic.id,
        "key": topic.key,
        "createdAt": time::iso(topic.created_at),
        "updatedAt": time::iso(topic.updated_at),
    });
    if let Some(name) = &topic.name {
        dto["name"] = json!(name);
    }
    dto
}

async fn upsert_topic(
    State(ctx): State<Ctx>,
    caller: Caller,
    JsonBody(payload): JsonBody<TopicBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let request = payload;
    let key = required(Some(request.key), "key")?;
    let topic = ctx
        .state
        .store
        .upsert_topic(&env.id, &key, request.name.as_deref())
        .await?;
    data(StatusCode::CREATED, topic_dto(&topic))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionsBody {
    #[serde(default)]
    subscriptions: Vec<SubscriptionEntry>,
    #[serde(default)]
    subscriber_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SubscriptionEntry {
    SubscriberId(String),
    Member {
        #[serde(rename = "subscriberId")]
        subscriber_id: Option<String>,
        identifier: Option<String>,
    },
}

impl SubscriptionsBody {
    fn members(self, key: &str) -> Result<Vec<TopicMember>, HttpError> {
        let entries = self.subscriptions.into_iter().chain(
            self.subscriber_ids
                .into_iter()
                .map(SubscriptionEntry::SubscriberId),
        );
        let members = entries
            .map(|entry| {
                let (subscriber_id, identifier) = match entry {
                    SubscriptionEntry::SubscriberId(id) => (id, None),
                    SubscriptionEntry::Member {
                        subscriber_id,
                        identifier,
                    } => (required(subscriber_id, "subscriberId")?, identifier),
                };
                let identifier =
                    identifier.unwrap_or_else(|| format!("tk_{key}:si_{subscriber_id}"));
                Ok(TopicMember {
                    subscriber_id,
                    identifier,
                })
            })
            .collect::<Result<Vec<_>, HttpError>>()?;
        if members.is_empty() {
            return Err(HttpError::bad_request("subscriptions is required"));
        }
        if members.len() > MAX_BATCH {
            return Err(HttpError::bad_request(format!(
                "subscriptions must contain at most {MAX_BATCH} items"
            )));
        }
        Ok(members)
    }
}

fn subscriptions_reply(
    status: StatusCode,
    path: &str,
    requested: usize,
    result: &TopicSubscriptions,
    missing_code: &str,
) -> Reply {
    let topic = topic_dto(&result.topic);
    let data: Vec<Value> = result
        .subscriptions
        .iter()
        .map(|s| {
            json!({
                "_id": s.id,
                "identifier": s.identifier,
                "topic": topic,
                "subscriber": {
                    "_id": s.subscriber.id,
                    "subscriberId": s.subscriber.subscriber_id,
                    "avatar": s.subscriber.avatar,
                    "firstName": s.subscriber.first_name,
                    "lastName": s.subscriber.last_name,
                    "email": s.subscriber.email,
                },
                "createdAt": time::iso(s.created_at),
                "updatedAt": time::iso(s.updated_at),
            })
        })
        .collect();
    let errors: Vec<Value> = result
        .missing
        .iter()
        .map(|id| {
            json!({
                "subscriberId": id,
                "code": missing_code,
                "message": format!("Subscriber with ID '{id}' could not be found."),
            })
        })
        .collect();
    let mut body = json!({
        "data": data,
        "meta": {
            "totalCount": requested,
            "successful": result.subscriptions.len(),
            "failed": result.missing.len(),
        },
        "errors": errors,
    });
    if result.subscriptions.is_empty() && !result.missing.is_empty() {
        // Upstream's exception filter sends the result both flat and under `ctx`;
        // the SDK's ErrorDto keeps only `ctx`.
        body["ctx"] = body.clone();
        body["statusCode"] = json!(400);
        body["timestamp"] = json!(time::iso(time::now_ms()));
        body["path"] = json!(path);
        body["message"] = json!("Api Exception Raised with status 400");
        return Ok((StatusCode::BAD_REQUEST, Json(body)));
    }
    Ok((status, Json(body)))
}

async fn subscribe(
    State(ctx): State<Ctx>,
    caller: Caller,
    uri: Uri,
    Path(key): Path<String>,
    JsonBody(payload): JsonBody<SubscriptionsBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let members = payload.members(&key)?;
    let result = ctx
        .state
        .store
        .subscribe_topic(&env.id, &key, &members)
        .await?;
    subscriptions_reply(
        StatusCode::CREATED,
        uri.path(),
        members.len(),
        &result,
        "SUBSCRIBER_NOT_FOUND",
    )
}

async fn unsubscribe(
    State(ctx): State<Ctx>,
    caller: Caller,
    uri: Uri,
    Path(key): Path<String>,
    JsonBody(payload): JsonBody<SubscriptionsBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let subscriber_ids: Vec<String> = payload
        .members(&key)?
        .into_iter()
        .map(|m| m.subscriber_id)
        .collect();
    let result = ctx
        .state
        .store
        .unsubscribe_topic(&env.id, &key, &subscriber_ids)
        .await?
        .ok_or_else(|| HttpError::not_found("Topic not found"))?;
    subscriptions_reply(
        StatusCode::OK,
        uri.path(),
        subscriber_ids.len(),
        &result,
        "SUBSCRIPTION_NOT_FOUND",
    )
}

fn message_dto(env: &Id, m: &Message) -> Value {
    let buttons: Vec<Value> = [
        ("primary", &m.primary_action),
        ("secondary", &m.secondary_action),
    ]
    .into_iter()
    .filter_map(|(kind, action)| {
        action
            .as_ref()
            .map(|a| json!({ "type": kind, "content": a.label }))
    })
    .collect();
    let mut cta = json!({ "type": "redirect", "data": {}, "action": { "buttons": buttons } });
    if let Some(redirect) = &m.redirect {
        cta["data"]["url"] = json!(redirect.url);
    }
    let mut dto = json!({
        "_id": m.id,
        "_environmentId": env,
        "_organizationId": env,
        "_notificationId": m.run_id.as_ref().unwrap_or(&m.id),
        "_subscriberId": m.subscriber_id,
        "templateIdentifier": m.workflow_id,
        "transactionId": m.transaction_id,
        "channel": m.channel.as_str(),
        "content": m.body,
        "read": m.is_read(),
        "seen": m.is_seen(),
        "status": "sent",
        "cta": cta,
        "createdAt": time::iso(m.created_at),
    });
    if let Some(subject) = &m.subject {
        dto["subject"] = json!(subject);
    }
    if let Some(at) = m.seen_at {
        dto["lastSeenDate"] = json!(time::iso(at));
    }
    if let Some(at) = m.read_at {
        dto["lastReadDate"] = json!(time::iso(at));
    }
    if let Some(payload) = &m.data {
        dto["payload"] = json!(payload);
    }
    dto
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarkBody {
    message_id: OneOrMany<String>,
    mark_as: MarkAs,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum MarkAs {
    Read,
    Seen,
    Unread,
    Unseen,
}

async fn mark_messages(
    State(ctx): State<Ctx>,
    caller: Caller,
    Path(subscriber_id): Path<String>,
    JsonBody(payload): JsonBody<MarkBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let request = payload;
    let ids: Vec<Id> = request
        .message_id
        .into_vec()
        .iter()
        .filter_map(|id| Id::parse(id))
        .collect();
    if ids.is_empty() {
        return Err(HttpError::bad_request("messageId is required"));
    }
    if ids.len() > MAX_BATCH {
        return Err(HttpError::bad_request(format!(
            "messageId must contain at most {MAX_BATCH} items"
        )));
    }
    let mark = match request.mark_as {
        MarkAs::Read => Mark::Read,
        MarkAs::Seen => Mark::Seen,
        MarkAs::Unread => Mark::Unread,
        MarkAs::Unseen => Mark::Unseen,
    };
    let store = &ctx.state.store;
    if store.subscriber(&env.id, &subscriber_id).await?.is_none() {
        return Err(HttpError::not_found(format!(
            "Subscriber {subscriber_id} not found"
        )));
    }
    let messages = store
        .mark_messages(&env.id, &subscriber_id, &ids, mark)
        .await?;
    if !messages.is_empty() {
        ctx.state
            .hub
            .publish(&env.id, &subscriber_id, Event::CountsChanged);
    }
    data(
        StatusCode::CREATED,
        messages.iter().map(|m| message_dto(&env.id, m)).collect(),
    )
}

fn channel(value: &str) -> Result<Channel, HttpError> {
    Channel::parse(value)
        .ok_or_else(|| HttpError::bad_request("channel must be one of in_app, email, push"))
}

fn message_query(pairs: Vec<(String, String)>) -> Result<MessageQuery, HttpError> {
    let number = |key: &str, value: &str| {
        value
            .parse::<u32>()
            .map_err(|_| HttpError::bad_request(format!("{key} must be a non-negative integer")))
    };
    let mut query = MessageQuery {
        limit: 10,
        ..MessageQuery::default()
    };
    for (key, value) in pairs {
        if key == "subscriberId" {
            query.subscriber_id = Some(value);
        } else if key == "transactionId" {
            query.transaction_ids.push(value);
        } else if key == "channel" {
            query.channel = Some(channel(&value)?);
        } else if key == "page" {
            query.page = number(&key, &value)?;
        } else if key == "limit" {
            query.limit = number(&key, &value)?.clamp(1, MAX_PAGE_SIZE);
        }
    }
    Ok(query)
}

async fn list_messages(
    State(ctx): State<Ctx>,
    caller: Caller,
    QueryParams(pairs): QueryParams<Vec<(String, String)>>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let query = message_query(pairs)?;
    let store = &ctx.state.store;
    if let Some(id) = &query.subscriber_id
        && store.subscriber(&env.id, id).await?.is_none()
    {
        return Err(HttpError::not_found(format!(
            "Subscriber '{id}' was not found"
        )));
    }
    let (messages, has_more) = store.messages(&env.id, &query).await?;
    let data: Vec<Value> = messages.iter().map(|m| message_dto(&env.id, m)).collect();
    let body = json!({
        "hasMore": has_more,
        "data": data,
        "pageSize": query.limit,
        "page": query.page,
    });
    Ok((StatusCode::OK, Json(body)))
}

#[derive(Deserialize)]
struct DeleteTransactionQuery {
    channel: Option<String>,
}

async fn delete_transaction(
    State(ctx): State<Ctx>,
    caller: Caller,
    Path(transaction_id): Path<String>,
    QueryParams(query): QueryParams<DeleteTransactionQuery>,
) -> Result<StatusCode, HttpError> {
    let env = caller.environment(&ctx.state)?;
    let channel = query.channel.as_deref().map(channel).transpose()?;
    let mut subscribers = ctx
        .state
        .store
        .delete_transaction_messages(&env.id, &transaction_id, channel)
        .await?;
    subscribers.sort();
    subscribers.dedup();
    for subscriber_id in &subscribers {
        ctx.state
            .hub
            .publish(&env.id, subscriber_id, Event::CountsChanged);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Target {
    SubscriberId(String),
    Topic {
        #[serde(rename = "topicKey")]
        topic_key: String,
        #[serde(default)]
        exclude: Vec<String>,
    },
    Subscriber(Box<SubscriberBody>),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TriggerBody {
    name: String,
    to: OneOrMany<Target>,
    payload: Option<Value>,
    transaction_id: Option<String>,
    actor: Option<Target>,
    bridge_url: Option<String>,
}

fn trigger_request(request: TriggerBody, headers: &HeaderMap) -> Result<TriggerRequest, HttpError> {
    let workflow_id = required(Some(request.name), "name")?;
    let to = request
        .to
        .into_vec()
        .into_iter()
        .map(|target| match target {
            Target::SubscriberId(id) => Ok(Recipient::Subscriber {
                subscriber_id: required(Some(id), "subscriberId")?,
                patch: Box::default(),
            }),
            Target::Topic { topic_key, exclude } => Ok(Recipient::Topic {
                key: required(Some(topic_key), "topicKey")?,
                exclude,
            }),
            Target::Subscriber(subscriber) => {
                let (id, patch) = subscriber.into_parts();
                Ok(Recipient::Subscriber {
                    subscriber_id: required(id, "subscriberId")?,
                    patch: Box::new(patch),
                })
            }
        })
        .collect::<Result<Vec<_>, HttpError>>()?;
    if to.is_empty() {
        return Err(HttpError::bad_request("to is required"));
    }
    if to.len() > MAX_BATCH {
        return Err(HttpError::bad_request(format!(
            "to must contain at most {MAX_BATCH} items"
        )));
    }
    let actor = match request.actor {
        None => None,
        Some(Target::SubscriberId(id)) => Some(id),
        Some(Target::Subscriber(subscriber)) => {
            Some(required(subscriber.subscriber_id, "actor.subscriberId")?)
        }
        Some(Target::Topic { .. }) => {
            return Err(HttpError::bad_request("actor must be a subscriber"));
        }
    };
    Ok(TriggerRequest {
        workflow_id,
        to,
        payload: request.payload.unwrap_or_else(|| json!({})),
        transaction_id: request.transaction_id,
        actor,
        bridge_url: request.bridge_url,
        idempotency_key: headers
            .get("idempotency-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
    })
}

async fn trigger(
    State(ctx): State<Ctx>,
    caller: Caller,
    headers: HeaderMap,
    JsonBody(payload): JsonBody<TriggerBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let request = trigger_request(payload, &headers)?;
    let transaction_id = ctx.engine.trigger(env, request).await?;
    data(
        StatusCode::CREATED,
        json!({
            "acknowledged": true,
            "status": "processed",
            "transactionId": transaction_id,
        }),
    )
}

async fn cancel(
    State(ctx): State<Ctx>,
    caller: Caller,
    Path(transaction_id): Path<String>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let cancelled = ctx.engine.cancel(env, &transaction_id).await?;
    data(StatusCode::OK, json!(cancelled))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncBody {
    bridge_url: String,
}

async fn sync(
    State(ctx): State<Ctx>,
    caller: Caller,
    JsonBody(payload): JsonBody<SyncBody>,
) -> Reply {
    let env = caller.environment(&ctx.state)?;
    let bridge_url = required(Some(payload.bridge_url), "bridgeUrl")?;
    let workflows = ctx.engine.sync(env, &bridge_url).await?;
    data(
        StatusCode::CREATED,
        workflows
            .iter()
            .map(|w| json!({ "_id": w.id, "workflowId": w.workflow_id, "name": w.name }))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
    use nanovu_core::Hub;
    use nanovu_core::config::Config;
    use nanovu_core::domain::Severity;
    use nanovu_core::sqlite::SqliteStore;
    use tower::ServiceExt;

    use super::*;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    fn env_id() -> Id {
        Id::for_environment("stg")
    }

    async fn setup() -> (Router, AppState) {
        let dir = std::env::temp_dir().join(format!("nanovu-api-{}", Id::new()));
        fs::create_dir_all(&dir).unwrap();
        let secret = |name: &str, value: &str| {
            let path = dir.join(name);
            fs::write(&path, value).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            path
        };
        let key = secret("key", KEY);
        let fcm = secret("fcm.json", "{}");
        let toml = secret(
            "nanovu.toml",
            &format!(
                "database = \"/tmp/unused.db\"\n[environments.stg]\napplication_identifier = \"app\"\n\
                 secret_key_file = \"{}\"\n[environments.stg.integrations.push]\nprovider = \"fcm\"\n\
                 service_account_file = \"{}\"\n",
                key.display(),
                fcm.display()
            ),
        );
        let config = Config::load(&toml).unwrap();
        fs::remove_dir_all(&dir).unwrap();
        let state = AppState {
            environments: Arc::new(config.environments),
            store: Arc::new(SqliteStore::open_in_memory().await.unwrap()),
            hub: Hub::default(),
        };
        (router(state.clone(), Engine::new(state.clone())), state)
    }

    async fn send(app: &Router, request: Request) -> (StatusCode, Value) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, body)
    }

    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, format!("ApiKey {KEY}"))
            .header(CONTENT_TYPE, "application/json");
        let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
        send(app, request.body(body).unwrap()).await
    }

    async fn insert_message(state: &AppState, subscriber_id: &str, transaction_id: &str) -> Id {
        let store = &state.store;
        store
            .upsert_subscriber(&env_id(), subscriber_id, &SubscriberPatch::default())
            .await
            .unwrap();
        let message = Message {
            id: Id::new(),
            run_id: None,
            transaction_id: transaction_id.into(),
            subscriber_id: subscriber_id.into(),
            workflow_id: "wf".into(),
            step_id: "in-app".into(),
            channel: Channel::InApp,
            subject: Some("Hi".into()),
            body: "Hello".into(),
            avatar: None,
            redirect: None,
            primary_action: None,
            secondary_action: None,
            data: None,
            tags: vec![],
            severity: Severity::None,
            seen_at: None,
            read_at: None,
            archived_at: None,
            created_at: time::now_ms(),
            updated_at: time::now_ms(),
        };
        store.insert_message(&env_id(), &message).await.unwrap();
        message.id
    }

    fn assert_nest_error(body: &Value, status: u16, path: &str, message: &str) {
        assert_eq!(body["statusCode"], status);
        assert_eq!(body["path"], path);
        assert_eq!(body["message"], message);
        assert!(body["timestamp"].as_str().unwrap().ends_with('Z'));
    }

    #[tokio::test]
    async fn rejects_missing_or_wrong_api_key() {
        let (app, _) = setup().await;
        for auth in [None, Some("ApiKey wrong"), Some(&*format!("Bearer {KEY}"))] {
            let mut request =
                Request::post("/v2/subscribers").header(CONTENT_TYPE, "application/json");
            if let Some(auth) = auth {
                request = request.header(AUTHORIZATION, auth);
            }
            let (status, body) = send(&app, request.body(Body::from("{}")).unwrap()).await;
            assert_eq!(status, 401);
            assert_nest_error(&body, 401, "/v2/subscribers", "Unauthorized");
        }
    }

    #[tokio::test]
    async fn bodies_are_read_only_after_auth_and_within_the_route_limit() {
        let (app, _) = setup().await;
        let padded =
            |len: usize| json!({"name": "wf", "to": "s", "payload": {"pad": "x".repeat(len)}});
        let huge = Request::post("/v1/events/trigger")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(padded(1 << 20).to_string()))
            .unwrap();
        assert_eq!(send(&app, huge).await.0, 401);

        let (status, body) = call(&app, "POST", "/v1/events/trigger", Some(padded(33 << 10))).await;
        assert_eq!(status, 413);
        assert_eq!(body["message"], "Request body too large");
        let (status, _) = call(&app, "POST", "/v1/events/trigger", Some(padded(20 << 10))).await;
        assert_ne!(status, 413);
        let subscriber = json!({"subscriberId": "s", "data": {"pad": "x".repeat(17 << 10)}});
        let (status, _) = call(&app, "POST", "/v2/subscribers", Some(subscriber)).await;
        assert_eq!(status, 413);
    }

    #[tokio::test]
    async fn error_bodies_carry_what_the_sdk_requires() {
        let (app, _) = setup().await;
        let (status, body) = call(&app, "POST", "/v2/subscribers", Some(json!({}))).await;
        assert_eq!(status, 400);
        assert_nest_error(&body, 400, "/v2/subscribers", "subscriberId is required");
        assert_eq!(body["type"], "BAD_REQUEST");
        assert_eq!(body["errors"], json!([]));

        let (status, body) = call(
            &app,
            "POST",
            "/v2/subscribers",
            Some(json!({"subscriberId": 1})),
        )
        .await;
        assert_eq!(status, 400);
        assert_eq!(body["message"], "Invalid request body");

        let app = Router::new()
            .route(
                "/v1/x",
                get(|| async { HttpError::unprocessable("Workflow not found") }),
            )
            .layer(middleware::from_fn(render_error));
        let (status, body) = call(&app, "GET", "/v1/x", None).await;
        assert_eq!(status, 422);
        assert_nest_error(&body, 422, "/v1/x", "Workflow not found");
        assert_eq!(body["errors"], json!({}));
        assert!(body.get("type").is_none());
    }

    #[tokio::test]
    async fn subscriber_lifecycle() {
        let (app, state) = setup().await;
        let (status, body) = call(
            &app,
            "POST",
            "/v2/subscribers",
            Some(json!({"subscriberId": "s1", "email": "a@b.c", "data": {"k": 1}})),
        )
        .await;
        assert_eq!(status, 201);
        let dto = &body["data"];
        assert_eq!(body.as_object().unwrap().len(), 1);
        assert_eq!(dto["subscriberId"], "s1");
        assert_eq!(dto["_organizationId"], env_id().as_str());
        assert_eq!(dto["_environmentId"], env_id().as_str());
        assert_eq!(dto["deleted"], false);
        assert_eq!(dto["data"], json!({"k": 1}));
        assert!(Id::parse(dto["_id"].as_str().unwrap()).is_some());
        assert!(dto["createdAt"].as_str().unwrap().ends_with('Z'));
        assert!(dto["updatedAt"].as_str().unwrap().ends_with('Z'));

        let (status, body) = call(
            &app,
            "PATCH",
            "/v2/subscribers/s1",
            Some(json!({"firstName": "Ada"})),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["firstName"], "Ada");
        assert_eq!(body["data"]["email"], "a@b.c");

        let (status, _) = call(&app, "PATCH", "/v2/subscribers/nope", Some(json!({}))).await;
        assert_eq!(status, 404);

        let mut socket = state.hub.subscribe(&env_id(), "s1").unwrap();
        let (status, body) = call(&app, "DELETE", "/v2/subscribers/s1", None).await;
        assert_eq!(status, 200);
        assert_eq!(
            body,
            json!({"data": {"acknowledged": true, "status": "deleted"}})
        );
        assert!(socket.recv().await.is_none());
        let (status, body) = call(&app, "DELETE", "/v2/subscribers/s1", None).await;
        assert_eq!(status, 404);
        assert_nest_error(&body, 404, "/v2/subscribers/s1", "Subscriber not found");
    }

    #[tokio::test]
    async fn credentials_replace_tokens_and_reject_unknown_integrations() {
        let (app, state) = setup().await;
        let uri = "/v1/subscribers/s1/credentials";
        let request = |identifier: Option<&str>, tokens: Value| json!({"providerId": "fcm", "integrationIdentifier": identifier, "credentials": {"deviceTokens": tokens}});

        let (status, body) =
            call(&app, "PUT", uri, Some(request(Some("other"), json!(["t"])))).await;
        assert_eq!(status, 400);
        let message = body["message"].as_str().unwrap();
        let env = env_id();
        assert!(env.as_str().bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(
            message,
            format!("Subscribers environment ({env}) do not have active fcm integration.")
        );

        let (status, body) = call(&app, "PUT", uri, Some(request(None, json!(["t"])))).await;
        assert_eq!(status, 400);
        assert_eq!(body["message"], "SubscriberId: s1 not found");

        call(
            &app,
            "POST",
            "/v2/subscribers",
            Some(json!({"subscriberId": "s1"})),
        )
        .await;
        let (status, body) = call(&app, "PUT", uri, Some(request(None, json!(["t1", "t2"])))).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["subscriberId"], "s1");
        let tokens = state.store.device_tokens(&env, "s1", "push").await.unwrap();
        assert_eq!(tokens.len(), 2);

        let (status, _) = call(
            &app,
            "PUT",
            uri,
            Some(request(Some("push"), json!(vec!["t"; 101]))),
        )
        .await;
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn topics_and_subscriptions() {
        let (app, _) = setup().await;
        let (status, body) = call(
            &app,
            "POST",
            "/v2/topics",
            Some(json!({"key": "k", "name": "Team"})),
        )
        .await;
        assert_eq!(status, 201);
        assert_eq!(body["data"]["key"], "k");
        assert_eq!(body["data"]["name"], "Team");
        assert!(body["data"]["_id"].is_string());

        for id in ["s1", "s2"] {
            call(
                &app,
                "POST",
                "/v2/subscribers",
                Some(json!({"subscriberId": id})),
            )
            .await;
        }
        let (status, body) = call(
            &app,
            "POST",
            "/v2/topics/k/subscriptions",
            Some(json!({"subscriptions": [{"identifier": "s1-in-topic-k", "subscriberId": "s1"}, "s2", "ghost"]})),
        )
        .await;
        assert_eq!(status, 201);
        assert_eq!(
            body["meta"],
            json!({"totalCount": 3, "successful": 2, "failed": 1})
        );
        assert_eq!(body["errors"][0]["subscriberId"], "ghost");
        assert_eq!(body["errors"][0]["code"], "SUBSCRIBER_NOT_FOUND");
        let data = body["data"].as_array().unwrap();
        let identifiers: Vec<&str> = data
            .iter()
            .map(|s| s["identifier"].as_str().unwrap())
            .collect();
        assert!(identifiers.contains(&"s1-in-topic-k"));
        assert!(identifiers.contains(&"tk_k:si_s2"));
        assert_eq!(data[0]["topic"]["key"], "k");
        assert!(data[0]["subscriber"]["_id"].is_string());
        assert!(data[0]["createdAt"].is_string());

        let (status, body) = call(
            &app,
            "DELETE",
            "/v2/topics/k/subscriptions",
            Some(json!({"subscriberIds": ["s1"]})),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            body["meta"],
            json!({"totalCount": 1, "successful": 1, "failed": 0})
        );
        assert_eq!(body["data"][0]["subscriber"]["subscriberId"], "s1");

        let (status, _) = call(
            &app,
            "DELETE",
            "/v2/topics/k/subscriptions",
            Some(json!({"subscriptions": [{"identifier": "x"}]})),
        )
        .await;
        assert_eq!(status, 400);
        let (status, _) = call(
            &app,
            "DELETE",
            "/v2/topics/missing/subscriptions",
            Some(json!({"subscriberIds": ["s1"]})),
        )
        .await;
        assert_eq!(status, 404);
        let (status, _) = call(&app, "POST", "/v2/topics/k/subscriptions", Some(json!({}))).await;
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn mark_as_returns_messages_and_publishes() {
        let (app, state) = setup().await;
        let id = insert_message(&state, "s1", "tx").await;
        let mut events = state.hub.subscribe(&env_id(), "s1").unwrap();

        let (status, body) = call(
            &app,
            "POST",
            "/v1/subscribers/s1/messages/mark-as",
            Some(json!({"messageId": id.as_str(), "markAs": "read"})),
        )
        .await;
        assert_eq!(status, 201);
        let dto = &body["data"][0];
        assert_eq!(dto["_id"], id.as_str());
        assert_eq!(dto["_environmentId"], env_id().as_str());
        assert_eq!(dto["_organizationId"], env_id().as_str());
        assert_eq!(dto["_notificationId"], id.as_str());
        assert_eq!(dto["_subscriberId"], "s1");
        assert_eq!(dto["transactionId"], "tx");
        assert_eq!(dto["channel"], "in_app");
        assert_eq!(dto["content"], "Hello");
        assert_eq!(dto["subject"], "Hi");
        assert_eq!(dto["read"], true);
        assert_eq!(dto["seen"], true);
        assert_eq!(dto["status"], "sent");
        assert_eq!(dto["cta"]["type"], "redirect");
        assert!(dto["lastReadDate"].is_string());
        assert!(dto.get("payload").is_none());
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap();
        assert!(matches!(event, Some(Event::CountsChanged)));

        let (status, body) = call(
            &app,
            "POST",
            "/v1/subscribers/s1/messages/mark-as",
            Some(json!({"messageId": ["bogus"], "markAs": "read"})),
        )
        .await;
        assert_eq!(status, 400);
        assert_eq!(body["message"], "messageId is required");
    }

    #[tokio::test]
    async fn lists_and_deletes_messages() {
        let (app, state) = setup().await;
        insert_message(&state, "s1", "a").await;
        insert_message(&state, "s1", "b").await;
        insert_message(&state, "s2", "c").await;

        let (status, body) = call(
            &app,
            "GET",
            "/v1/messages?transactionId=a&transactionId=b&limit=1",
            None,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["hasMore"], true);
        assert_eq!(body["pageSize"], 1);
        assert_eq!(body["page"], 0);
        assert_eq!(body["data"].as_array().unwrap().len(), 1);

        let (_, body) = call(
            &app,
            "GET",
            "/v1/messages?subscriberId=s2&channel=in_app",
            None,
        )
        .await;
        assert_eq!(body["hasMore"], false);
        assert_eq!(body["pageSize"], 10);
        assert_eq!(body["data"][0]["transactionId"], "c");

        let (status, _) = call(&app, "GET", "/v1/messages?channel=sms", None).await;
        assert_eq!(status, 400);
        let (status, _) = call(&app, "GET", "/v1/messages?page=-1", None).await;
        assert_eq!(status, 400);

        let mut events = state.hub.subscribe(&env_id(), "s1").unwrap();
        let (status, body) = call(&app, "DELETE", "/v1/messages/transaction/a", None).await;
        assert_eq!(status, 204);
        assert_eq!(body, Value::Null);
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap();
        assert!(matches!(event, Some(Event::CountsChanged)));
        let (_, body) = call(&app, "GET", "/v1/messages?transactionId=a", None).await;
        assert_eq!(body["data"], json!([]));
    }

    fn parse_trigger(body: Value, headers: &HeaderMap) -> Result<TriggerRequest, HttpError> {
        trigger_request(serde_json::from_value(body).unwrap(), headers)
    }

    #[test]
    fn message_query_keeps_the_last_value_and_rejects_any_invalid_one() {
        let pairs = |items: &[(&str, &str)]| {
            items
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect::<Vec<_>>()
        };
        let query = message_query(pairs(&[("limit", "5"), ("limit", "7"), ("bogus", "x")]))
            .expect("valid limits");
        assert_eq!(query.limit, 7);
        for order in [
            [("limit", "x"), ("limit", "5")],
            [("limit", "5"), ("limit", "x")],
        ] {
            assert!(message_query(pairs(&order)).is_err());
        }
    }

    #[test]
    fn parses_trigger_bodies() {
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", "idem".parse().unwrap());
        let request = parse_trigger(
            json!({
                "name": "wf",
                "to": ["s1", {"subscriberId": "s2", "email": "a@b.c", "data": {"k": 1}}, {"type": "Topic", "topicKey": "t", "exclude": ["s3"]}],
                "actor": {"subscriberId": "boss"},
                "transactionId": "tx",
                "bridgeUrl": "http://localhost/api/novu",
                "overrides": {},
            }),
            &headers,
        )
        .unwrap();
        assert_eq!(request.workflow_id, "wf");
        assert_eq!(request.payload, json!({}));
        assert_eq!(request.actor.as_deref(), Some("boss"));
        assert_eq!(request.transaction_id.as_deref(), Some("tx"));
        assert_eq!(request.idempotency_key.as_deref(), Some("idem"));
        assert_eq!(
            request.to,
            [
                Recipient::Subscriber {
                    subscriber_id: "s1".into(),
                    patch: Box::default()
                },
                Recipient::Subscriber {
                    subscriber_id: "s2".into(),
                    patch: Box::new(SubscriberPatch {
                        email: Some("a@b.c".into()),
                        data: Some(json!({"k": 1})),
                        ..SubscriberPatch::default()
                    }),
                },
                Recipient::Topic {
                    key: "t".into(),
                    exclude: vec!["s3".into()],
                },
            ]
        );

        let single = parse_trigger(
            json!({"name": "wf", "to": "s1", "actor": "a", "payload": {"x": 1}}),
            &HeaderMap::new(),
        )
        .unwrap();
        assert_eq!(single.to.len(), 1);
        assert_eq!(single.actor.as_deref(), Some("a"));
        assert_eq!(single.payload, json!({"x": 1}));
        assert_eq!(single.idempotency_key, None);

        for (body, message) in [
            (json!({"name": "wf", "to": []}), "to is required"),
            (
                json!({"name": "wf", "to": vec!["s"; MAX_BATCH + 1]}),
                "to must contain at most 100 items",
            ),
            (json!({"name": "", "to": "s1"}), "name is required"),
            (
                json!({"name": "wf", "to": {"email": "a@b.c"}}),
                "subscriberId is required",
            ),
            (
                json!({"name": "wf", "to": "s1", "actor": {"topicKey": "t"}}),
                "actor must be a subscriber",
            ),
        ] {
            assert_eq!(
                parse_trigger(body, &HeaderMap::new()).unwrap_err().message,
                message
            );
        }
    }

    #[tokio::test]
    async fn engine_routes_validate_before_calling_the_engine() {
        let (app, _) = setup().await;
        let (status, body) = call(
            &app,
            "POST",
            "/v1/events/trigger",
            Some(json!({"name": "wf"})),
        )
        .await;
        assert_eq!(status, 400);
        assert_nest_error(&body, 400, "/v1/events/trigger", "Invalid request body");
        let (status, _) = call(
            &app,
            "POST",
            "/v1/bridge/sync",
            Some(json!({"bridgeUrl": ""})),
        )
        .await;
        assert_eq!(status, 400);
        let request = Request::delete("/v1/events/trigger/tx")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(&app, request).await.0, 401);
    }
}
