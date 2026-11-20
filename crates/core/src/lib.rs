pub mod auth;
pub mod config;
pub mod domain;
pub mod env;
pub mod error;
pub mod extract;
pub mod hub;
pub mod id;
pub mod sqlite;
pub mod store;
pub mod time;

use std::sync::Arc;

pub use env::{Environment, Environments};
pub use error::HttpError;
pub use extract::{JsonBody, QueryParams};
pub use hub::{Event, Hub, Subscription};
pub use id::Id;
pub use store::{Store, StoreError};

#[derive(Clone)]
pub struct AppState {
    pub environments: Arc<Environments>,
    pub store: Arc<dyn Store>,
    pub hub: Hub,
}
