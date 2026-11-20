use std::fmt;

use async_trait::async_trait;

use crate::domain::{
    ChannelPrefs, DigestEvent, DigestJoin, InboxPage, Mark, Message, MessageFilter, MessageQuery,
    Run, Subscriber, SubscriberPatch, SubscriberPreferences, Topic, TopicMember,
    TopicSubscriptions, Workflow,
};
use crate::id::Id;

#[derive(Debug)]
pub struct StoreError(Box<dyn std::error::Error + Send + Sync>);

impl StoreError {
    pub fn new(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(err.into())
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for StoreError {}

pub type Result<T> = std::result::Result<T, StoreError>;

#[async_trait]
pub trait Store: Send + Sync {
    async fn upsert_subscriber(
        &self,
        env: &Id,
        subscriber_id: &str,
        patch: &SubscriberPatch,
    ) -> Result<Subscriber>;
    async fn update_subscriber(
        &self,
        env: &Id,
        subscriber_id: &str,
        patch: &SubscriberPatch,
    ) -> Result<Option<Subscriber>>;
    async fn subscriber(&self, env: &Id, subscriber_id: &str) -> Result<Option<Subscriber>>;
    async fn delete_subscriber(&self, env: &Id, subscriber_id: &str) -> Result<bool>;

    async fn set_device_tokens(
        &self,
        env: &Id,
        subscriber_id: &str,
        integration: &str,
        tokens: &[String],
    ) -> Result<bool>;
    async fn device_tokens(
        &self,
        env: &Id,
        subscriber_id: &str,
        integration: &str,
    ) -> Result<Vec<String>>;
    async fn remove_device_tokens(
        &self,
        env: &Id,
        integration: &str,
        tokens: &[String],
    ) -> Result<()>;

    async fn upsert_topic(&self, env: &Id, key: &str, name: Option<&str>) -> Result<Topic>;
    async fn subscribe_topic(
        &self,
        env: &Id,
        key: &str,
        members: &[TopicMember],
    ) -> Result<TopicSubscriptions>;
    async fn unsubscribe_topic(
        &self,
        env: &Id,
        key: &str,
        subscriber_ids: &[String],
    ) -> Result<Option<TopicSubscriptions>>;
    async fn topic_subscriber_ids(&self, env: &Id, key: &str, limit: u32) -> Result<Vec<String>>;

    async fn replace_workflows(&self, env: &Id, workflows: &[Workflow]) -> Result<Vec<Workflow>>;
    async fn workflow(&self, env: &Id, key: &str) -> Result<Option<Workflow>>;
    async fn workflows(&self, env: &Id) -> Result<Vec<Workflow>>;

    async fn subscriber_preferences(
        &self,
        env: &Id,
        subscriber_id: &str,
    ) -> Result<SubscriberPreferences>;
    async fn update_preferences(
        &self,
        env: &Id,
        subscriber_id: &str,
        workflow_id: Option<&str>,
        patch: &ChannelPrefs,
    ) -> Result<ChannelPrefs>;

    async fn insert_message(&self, env: &Id, message: &Message) -> Result<()>;
    async fn message(&self, env: &Id, subscriber_id: &str, id: &Id) -> Result<Option<Message>>;
    async fn inbox(
        &self,
        env: &Id,
        subscriber_id: &str,
        filter: &MessageFilter,
        page: &InboxPage,
    ) -> Result<(Vec<Message>, bool)>;
    async fn inbox_counts(
        &self,
        env: &Id,
        subscriber_id: &str,
        filters: &[MessageFilter],
    ) -> Result<Vec<u64>>;
    async fn mark_messages(
        &self,
        env: &Id,
        subscriber_id: &str,
        ids: &[Id],
        mark: Mark,
    ) -> Result<Vec<Message>>;
    async fn mark_inbox(
        &self,
        env: &Id,
        subscriber_id: &str,
        filter: &MessageFilter,
        mark: Mark,
    ) -> Result<u64>;
    async fn messages(&self, env: &Id, query: &MessageQuery) -> Result<(Vec<Message>, bool)>;
    async fn delete_transaction_messages(
        &self,
        env: &Id,
        transaction_id: &str,
    ) -> Result<Vec<String>>;
    async fn run_messages(&self, env: &Id, run_id: &Id) -> Result<Vec<Message>>;

    async fn create_runs(
        &self,
        env: &Id,
        transaction_id: &str,
        idempotency_key: Option<&str>,
        runs: &[Run],
    ) -> Result<Option<String>>;
    async fn claim_due_runs(&self, now: i64, limit: u32) -> Result<Vec<Run>>;
    async fn next_run_at(&self) -> Result<Option<i64>>;
    async fn requeue_running_runs(&self) -> Result<u64>;
    async fn save_run(&self, run: &Run) -> Result<bool>;
    async fn join_digest(&self, run: &Run, key: &str, window_end: i64) -> Result<DigestJoin>;
    async fn digest_events(
        &self,
        env: &Id,
        owner: &Id,
        limit: u32,
        max_bytes: u32,
    ) -> Result<(Vec<DigestEvent>, u64)>;
    async fn cancel_transaction(&self, env: &Id, transaction_id: &str) -> Result<u64>;
    async fn purge_expired(&self, now: i64) -> Result<()>;
}
