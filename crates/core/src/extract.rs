use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, Query, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use crate::HttpError;

const BODY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_QUERY: usize = 8 << 10;

pub struct JsonBody<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for JsonBody<T> {
    type Rejection = HttpError;

    async fn from_request(request: Request, state: &S) -> Result<Self, HttpError> {
        let bytes = tokio::time::timeout(BODY_TIMEOUT, Bytes::from_request(request, state))
            .await
            .map_err(|_| HttpError::new(StatusCode::REQUEST_TIMEOUT, "Request timeout"))?
            .map_err(|rejection| match rejection.status() {
                StatusCode::PAYLOAD_TOO_LARGE => {
                    HttpError::new(StatusCode::PAYLOAD_TOO_LARGE, "Request body too large")
                }
                status => HttpError::new(status, "Invalid request body"),
            })?;
        let bytes: &[u8] = if bytes.is_empty() { b"null" } else { &bytes };
        serde_json::from_slice(bytes)
            .map(Self)
            .map_err(|_| HttpError::bad_request("Invalid request body"))
    }
}

pub struct QueryParams<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequestParts<S> for QueryParams<T> {
    type Rejection = HttpError;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, HttpError> {
        if parts.uri.query().is_some_and(|q| q.len() > MAX_QUERY) {
            return Err(HttpError::new(StatusCode::URI_TOO_LONG, "Query too long"));
        }
        Query::try_from_uri(&parts.uri)
            .map(|Query(query)| Self(query))
            .map_err(|_| HttpError::bad_request("Invalid query"))
    }
}
