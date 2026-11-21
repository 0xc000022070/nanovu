use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::id::Id;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    InApp,
    Email,
    Push,
}

impl Channel {
    pub const ALL: [Channel; 3] = [Channel::InApp, Channel::Email, Channel::Push];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::InApp => "in_app",
            Self::Email => "email",
            Self::Push => "push",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == s)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    High,
    Medium,
    Low,
    #[default]
    None,
}

impl Severity {
    pub const ALL: [Severity; 4] = [
        Severity::High,
        Severity::Medium,
        Severity::Low,
        Severity::None,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
            Self::None => "none",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    InApp,
    Email,
    Push,
    Digest,
    Delay,
    #[serde(other)]
    Unsupported,
}

impl StepKind {
    pub fn channel(self) -> Option<Channel> {
        match self {
            Self::InApp => Some(Channel::InApp),
            Self::Email => Some(Channel::Email),
            Self::Push => Some(Channel::Push),
            Self::Digest | Self::Delay | Self::Unsupported => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStep {
    pub step_id: String,
    pub kind: StepKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelPrefs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_app: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push: Option<bool>,
}

impl ChannelPrefs {
    pub fn get(&self, channel: Channel) -> Option<bool> {
        match channel {
            Channel::InApp => self.in_app,
            Channel::Email => self.email,
            Channel::Push => self.push,
        }
    }

    pub fn set(&mut self, channel: Channel, value: Option<bool>) {
        match channel {
            Channel::InApp => self.in_app = value,
            Channel::Email => self.email = value,
            Channel::Push => self.push = value,
        }
    }

    pub fn overlay(mut self, other: &ChannelPrefs) -> Self {
        for channel in Channel::ALL {
            if let Some(v) = other.get(channel) {
                self.set(channel, Some(v));
            }
        }
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowPreferences {
    pub enabled: bool,
    pub read_only: bool,
    pub channels: ChannelPrefs,
}

impl Default for WorkflowPreferences {
    fn default() -> Self {
        Self {
            enabled: true,
            read_only: false,
            channels: ChannelPrefs::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Workflow {
    pub id: Id,
    pub workflow_id: String,
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub severity: Severity,
    pub preferences: WorkflowPreferences,
    pub payload_schema: Option<Value>,
    pub steps: Vec<WorkflowStep>,
    pub bridge_url: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Workflow {
    pub fn is_critical(&self) -> bool {
        self.preferences.read_only
    }

    pub fn channels(&self) -> Vec<Channel> {
        Channel::ALL
            .into_iter()
            .filter(|c| self.steps.iter().any(|s| s.kind.channel() == Some(*c)))
            .collect()
    }

    pub fn resolve_channels(&self, global: &ChannelPrefs, own: &ChannelPrefs) -> ChannelPrefs {
        let mut resolved = ChannelPrefs::default();
        for channel in self.channels() {
            let default = self
                .preferences
                .channels
                .get(channel)
                .unwrap_or(self.preferences.enabled);
            let value = if self.is_critical() {
                default
            } else {
                own.get(channel).or(global.get(channel)).unwrap_or(default)
            };
            resolved.set(channel, Some(value));
        }
        resolved
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SubscriberPreferences {
    pub global: ChannelPrefs,
    pub workflows: BTreeMap<String, ChannelPrefs>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Subscriber {
    pub id: Id,
    pub subscriber_id: String,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone: Option<String>,
    pub avatar: Option<String>,
    pub locale: Option<String>,
    pub timezone: Option<String>,
    pub data: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SubscriberPatch {
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone: Option<String>,
    pub avatar: Option<String>,
    pub locale: Option<String>,
    pub timezone: Option<String>,
    pub data: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SubscriberUpdate {
    pub email: Option<Option<String>>,
    pub first_name: Option<Option<String>>,
    pub last_name: Option<Option<String>>,
    pub phone: Option<Option<String>>,
    pub avatar: Option<Option<String>>,
    pub locale: Option<Option<String>>,
    pub timezone: Option<Option<String>>,
    pub data: Option<Option<Value>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Topic {
    pub id: Id,
    pub key: String,
    pub name: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicMember {
    pub subscriber_id: String,
    pub identifier: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicSubscription {
    pub id: Id,
    pub identifier: String,
    pub subscriber: Subscriber,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicSubscriptions {
    pub topic: Topic,
    pub subscriptions: Vec<TopicSubscription>,
    pub missing: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redirect {
    pub url: String,
    pub target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    pub label: String,
    pub redirect: Option<Redirect>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub id: Id,
    pub run_id: Option<Id>,
    pub transaction_id: String,
    pub subscriber_id: String,
    pub workflow_id: String,
    pub step_id: String,
    pub channel: Channel,
    pub subject: Option<String>,
    pub body: String,
    pub avatar: Option<String>,
    pub redirect: Option<Redirect>,
    pub primary_action: Option<Action>,
    pub secondary_action: Option<Action>,
    pub data: Option<Map<String, Value>>,
    pub tags: Vec<String>,
    pub severity: Severity,
    pub seen_at: Option<i64>,
    pub read_at: Option<i64>,
    pub archived_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Message {
    pub fn is_seen(&self) -> bool {
        self.seen_at.is_some()
    }

    pub fn is_read(&self) -> bool {
        self.read_at.is_some()
    }

    pub fn is_archived(&self) -> bool {
        self.archived_at.is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mark {
    Seen,
    Read,
    Archive,
    Unseen,
    Unread,
    Unarchive,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MessageFilter {
    pub tags: Vec<Vec<String>>,
    pub read: Option<bool>,
    pub seen: Option<bool>,
    pub archived: Option<bool>,
    pub snoozed: Option<bool>,
    pub data: Vec<(Vec<String>, Vec<Value>)>,
    pub severity: Vec<Severity>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InboxPage {
    pub limit: u32,
    pub offset: u32,
    pub after: Option<Id>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MessageQuery {
    pub subscriber_id: Option<String>,
    pub transaction_ids: Vec<String>,
    pub channel: Option<Channel>,
    pub page: u32,
    pub limit: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Pending,
    Running,
    Merged,
    Done,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Merged => "merged",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        [
            Self::Pending,
            Self::Running,
            Self::Merged,
            Self::Done,
            Self::Failed,
            Self::Cancelled,
        ]
        .into_iter()
        .find(|v| v.as_str() == s)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Run {
    pub id: Id,
    pub env_id: Id,
    pub workflow_id: String,
    pub subscriber_id: String,
    pub transaction_id: String,
    pub bridge_url: String,
    pub payload: Arc<Value>,
    pub actor: Option<String>,
    pub step_index: u32,
    pub state: Value,
    pub status: RunStatus,
    pub run_at: i64,
    pub attempts: u32,
    pub digest_key: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DigestEvent {
    pub id: Id,
    pub run_id: Id,
    pub payload: Value,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DigestJoin {
    Owner,
    Merged { owner: Id },
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workflow(preferences: WorkflowPreferences) -> Workflow {
        let step = |id: &str, kind| WorkflowStep {
            step_id: id.into(),
            kind,
        };
        Workflow {
            id: Id::new(),
            workflow_id: "wf".into(),
            name: "wf".into(),
            description: None,
            tags: vec![],
            severity: Severity::None,
            preferences,
            payload_schema: None,
            steps: vec![
                step("d", StepKind::Digest),
                step("a", StepKind::InApp),
                step("p", StepKind::Push),
                step("p2", StepKind::Push),
            ],
            bridge_url: "http://localhost/api/novu".into(),
            created_at: 0,
            updated_at: 0,
        }
    }

    fn prefs(in_app: Option<bool>, push: Option<bool>) -> ChannelPrefs {
        ChannelPrefs {
            in_app,
            push,
            email: None,
        }
    }

    #[test]
    fn channels_are_distinct_and_only_from_steps() {
        assert_eq!(
            workflow(WorkflowPreferences::default()).channels(),
            [Channel::InApp, Channel::Push]
        );
    }

    #[test]
    fn preference_merge_later_wins() {
        let wf = workflow(WorkflowPreferences {
            enabled: true,
            read_only: false,
            channels: prefs(None, Some(false)),
        });
        let none = ChannelPrefs::default();
        assert_eq!(
            wf.resolve_channels(&none, &none),
            prefs(Some(true), Some(false))
        );
        assert_eq!(
            wf.resolve_channels(&prefs(Some(false), Some(true)), &none),
            prefs(Some(false), Some(true))
        );
        assert_eq!(
            wf.resolve_channels(&prefs(Some(false), Some(true)), &prefs(Some(true), None)),
            prefs(Some(true), Some(true))
        );
        let email_only = ChannelPrefs {
            email: Some(false),
            ..none
        };
        assert_eq!(wf.resolve_channels(&email_only, &none).email, None);
    }

    #[test]
    fn all_disabled_is_the_default_for_unlisted_channels() {
        let wf = workflow(WorkflowPreferences {
            enabled: false,
            read_only: false,
            channels: prefs(Some(true), None),
        });
        let none = ChannelPrefs::default();
        assert_eq!(
            wf.resolve_channels(&none, &none),
            prefs(Some(true), Some(false))
        );
    }

    #[test]
    fn critical_workflow_ignores_subscriber_settings() {
        let wf = workflow(WorkflowPreferences {
            enabled: true,
            read_only: true,
            channels: ChannelPrefs::default(),
        });
        let off = prefs(Some(false), Some(false));
        assert!(wf.is_critical());
        assert_eq!(
            wf.resolve_channels(&off, &off),
            prefs(Some(true), Some(true))
        );
    }

    #[test]
    fn unknown_step_types_parse_as_unsupported() {
        let step: WorkflowStep = serde_json::from_str(r#"{"step_id":"s","kind":"sms"}"#).unwrap();
        assert_eq!(step.kind, StepKind::Unsupported);
    }
}
