use std::borrow::Cow;
use std::fmt;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::store::StoreError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpError {
    pub status: StatusCode,
    pub message: Cow<'static, str>,
}

impl HttpError {
    pub fn new(status: StatusCode, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "Unauthorized")
    }

    pub fn not_found(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    pub fn unprocessable(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, message)
    }

    pub fn too_many_requests() -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, "Too many requests")
    }

    pub fn internal() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
}

impl std::error::Error for HttpError {}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let mut response = self.status.into_response();
        response.extensions_mut().insert(self);
        response
    }
}

impl From<StoreError> for HttpError {
    fn from(err: StoreError) -> Self {
        tracing::error!(error = %err, "storage failure");
        Self::internal()
    }
}
