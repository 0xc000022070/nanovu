use std::fmt;

use crate::domain::Channel;
use crate::id::Id;

#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(..)")
    }
}

#[derive(Debug)]
pub struct Environment {
    pub id: Id,
    pub name: String,
    pub application_identifier: String,
    pub secret_key: Secret,
    pub bridge_hosts: Vec<String>,
    pub integrations: Vec<Integration>,
    pub(crate) key_digest: [u8; 32],
    pub(crate) session_key: [u8; 32],
}

#[derive(Debug)]
pub struct Integration {
    pub identifier: String,
    pub provider: Provider,
}

#[derive(Debug)]
pub enum Provider {
    Fcm(Fcm),
    Smtp { url: Secret, from: String },
}

#[derive(Debug)]
pub struct Fcm {
    pub service_account: Secret,
    pub endpoint: Option<String>,
}

impl Provider {
    pub fn id(&self) -> &'static str {
        match self {
            Self::Fcm(_) => "fcm",
            Self::Smtp { .. } => "smtp",
        }
    }

    pub fn channel(&self) -> Channel {
        match self {
            Self::Fcm(_) => Channel::Push,
            Self::Smtp { .. } => Channel::Email,
        }
    }
}

impl Environment {
    pub fn integration(&self, provider_id: &str, identifier: Option<&str>) -> Option<&Integration> {
        self.integrations.iter().find(|i| {
            i.provider.id() == provider_id && identifier.is_none_or(|id| id == i.identifier)
        })
    }

    pub fn channel_integrations(&self, channel: Channel) -> impl Iterator<Item = &Integration> {
        self.integrations
            .iter()
            .filter(move |i| i.provider.channel() == channel)
    }
}

#[derive(Debug)]
pub struct Environments(pub(crate) Vec<Environment>);

impl Environments {
    fn find(&self, matches: impl Fn(&Environment) -> bool) -> Option<&Environment> {
        self.0.iter().find(|e| matches(e))
    }

    pub fn get(&self, id: &Id) -> Option<&Environment> {
        self.find(|e| &e.id == id)
    }

    pub fn by_application_identifier(&self, application_identifier: &str) -> Option<&Environment> {
        self.find(|e| e.application_identifier == application_identifier)
    }

    pub fn by_name(&self, name: &str) -> Option<&Environment> {
        self.find(|e| e.name == name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Environment> {
        self.0.iter()
    }
}
