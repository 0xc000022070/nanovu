use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message as Frame, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use nanovu_core::domain::Channel;
use nanovu_core::{AppState, Event, HttpError, Id, QueryParams, Subscription};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep_until};

use crate::{Context, authenticate, counts};

const IDLE: Duration = Duration::from_secs(60);
const MAX_FRAME: usize = 4 * 1024;

pub(crate) async fn upgrade(
    State(state): State<AppState>,
    QueryParams(query): QueryParams<Vec<(String, String)>>,
    ws: WebSocketUpgrade,
) -> Result<Response, HttpError> {
    let token = query
        .into_iter()
        .find_map(|(key, value)| (key == "token").then_some(value))
        .unwrap_or_default();
    let (env, subscriber_id) = authenticate(&state, &token).await?;
    let subscription = state
        .hub
        .subscribe(&env, &subscriber_id)
        .ok_or_else(HttpError::too_many_requests)?;
    Ok(ws
        .max_frame_size(MAX_FRAME)
        .max_message_size(MAX_FRAME)
        .on_upgrade(move |socket| serve(socket, subscription, state, env, subscriber_id)))
}

async fn serve(
    mut socket: WebSocket,
    mut subscription: Subscription,
    state: AppState,
    env: Id,
    subscriber_id: String,
) {
    let mut deadline = Instant::now() + IDLE;
    loop {
        tokio::select! {
            () = sleep_until(deadline) => break,
            frame = socket.recv() => {
                let Some(Ok(frame)) = frame else { break };
                deadline = Instant::now() + IDLE;
                match frame {
                    Frame::Text(text) if text.as_str() == "ping" => {
                        if socket.send(Frame::Text("pong".into())).await.is_err() {
                            break;
                        }
                    }
                    Frame::Close(_) => break,
                    Frame::Text(_) | Frame::Binary(_) | Frame::Ping(_) | Frame::Pong(_) => {}
                }
            }
            event = subscription.recv() => {
                let Some(event) = event else { break };
                let frames = match frames(&state, &env, &subscriber_id, event).await {
                    Ok(frames) => frames,
                    Err(err) => {
                        tracing::error!(error = %err, "inbox socket event failed");
                        continue;
                    }
                };
                for frame in frames {
                    if socket.send(Frame::Text(frame.to_string().into())).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

async fn frames(
    state: &AppState,
    env: &Id,
    subscriber_id: &str,
    event: Event,
) -> Result<Vec<Value>, HttpError> {
    let mut frames = Vec::with_capacity(3);
    if let Event::MessageCreated(message) = event
        && message.channel == Channel::InApp
    {
        let context = Context::load(state, env, subscriber_id).await?;
        frames.push(json!({
            "event": "notification_received",
            "data": { "message": context.raw(&message) },
        }));
    }
    let counts = counts(state, env, subscriber_id).await?;
    frames.push(json!({
        "event": "unread_count_changed",
        "data": { "counts": counts.unread, "unreadCount": counts.total },
    }));
    frames.push(json!({
        "event": "unseen_count_changed",
        "data": { "unseenCount": counts.unseen },
    }));
    Ok(frames)
}
