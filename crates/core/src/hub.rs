use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::broadcast::{self, error::RecvError};

use crate::domain::Message;
use crate::id::Id;

const PER_SUBSCRIBER: usize = 5;
const PER_ENVIRONMENT: usize = 128;
const GLOBAL: usize = 256;
const BUFFER: usize = 16;

#[derive(Clone, Debug)]
pub enum Event {
    MessageCreated(Arc<Message>),
    CountsChanged,
}

type Key = (Id, String);

#[derive(Default)]
struct Inner {
    channels: HashMap<Key, broadcast::Sender<Event>>,
    connections: usize,
    per_environment: HashMap<Id, usize>,
}

#[derive(Clone, Default)]
pub struct Hub(Arc<Mutex<Inner>>);

impl Hub {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn subscribe(&self, env: &Id, subscriber_id: &str) -> Option<Subscription> {
        let mut inner = self.lock();
        if inner.connections >= GLOBAL
            || inner.per_environment.get(env).copied().unwrap_or(0) >= PER_ENVIRONMENT
        {
            return None;
        }
        let key = (env.clone(), subscriber_id.to_owned());
        let sender = inner
            .channels
            .entry(key.clone())
            .or_insert_with(|| broadcast::channel(BUFFER).0);
        if sender.receiver_count() >= PER_SUBSCRIBER {
            return None;
        }
        let receiver = sender.subscribe();
        inner.connections += 1;
        *inner.per_environment.entry(env.clone()).or_default() += 1;
        Some(Subscription {
            hub: self.clone(),
            key,
            receiver,
        })
    }

    pub fn publish(&self, env: &Id, subscriber_id: &str, event: Event) {
        let inner = self.lock();
        if let Some(sender) = inner.channels.get(&(env.clone(), subscriber_id.to_owned())) {
            let _ = sender.send(event);
        }
    }

    pub fn close(&self, env: &Id, subscriber_id: &str) {
        self.lock()
            .channels
            .remove(&(env.clone(), subscriber_id.to_owned()));
    }
}

pub struct Subscription {
    hub: Hub,
    key: Key,
    receiver: broadcast::Receiver<Event>,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<Event> {
        match self.receiver.recv().await {
            Ok(event) => Some(event),
            Err(RecvError::Lagged(_)) => Some(Event::CountsChanged),
            Err(RecvError::Closed) => None,
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut inner = self.hub.lock();
        inner.connections = inner.connections.saturating_sub(1);
        if let Some(count) = inner.per_environment.get_mut(&self.key.0) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                inner.per_environment.remove(&self.key.0);
            }
        }
        if !self.receiver.is_closed()
            && inner
                .channels
                .get(&self.key)
                .is_some_and(|s| s.receiver_count() <= 1)
        {
            inner.channels.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delivers_and_cleans_up() {
        let hub = Hub::default();
        let env = Id::new();
        hub.publish(&env, "a", Event::CountsChanged);
        assert!(hub.lock().channels.is_empty());

        let mut first = hub.subscribe(&env, "a").unwrap();
        let second = hub.subscribe(&env, "a").unwrap();
        hub.publish(&env, "a", Event::CountsChanged);
        hub.publish(&env, "b", Event::CountsChanged);
        assert!(matches!(first.recv().await, Some(Event::CountsChanged)));

        drop(second);
        assert_eq!(hub.lock().channels.len(), 1);
        drop(first);
        assert!(hub.lock().channels.is_empty());
        assert_eq!(hub.lock().connections, 0);
    }

    #[test]
    fn enforces_caps() {
        let hub = Hub::default();
        let env = Id::new();
        let held: Vec<_> = (0..PER_SUBSCRIBER)
            .map(|_| hub.subscribe(&env, "a").unwrap())
            .collect();
        assert!(hub.subscribe(&env, "a").is_none());
        drop(held);
        assert!(hub.lock().channels.is_empty());

        let held: Vec<_> = (0..PER_ENVIRONMENT)
            .map(|i| hub.subscribe(&env, &i.to_string()).unwrap())
            .collect();
        assert!(hub.subscribe(&env, "x").is_none());
        let other = Id::new();
        let more: Vec<_> = (0..GLOBAL - PER_ENVIRONMENT)
            .map(|i| hub.subscribe(&other, &i.to_string()).unwrap())
            .collect();
        assert!(hub.subscribe(&Id::new(), "x").is_none());
        drop((held, more));
        assert!(hub.lock().per_environment.is_empty());
    }

    #[tokio::test]
    async fn close_ends_the_subscriber_and_spares_a_later_subscription() {
        let hub = Hub::default();
        let env = Id::new();
        let mut old = hub.subscribe(&env, "a").unwrap();
        hub.close(&env, "a");
        assert!(old.recv().await.is_none());
        let mut new = hub.subscribe(&env, "a").unwrap();
        drop(old);
        hub.publish(&env, "a", Event::CountsChanged);
        assert!(matches!(new.recv().await, Some(Event::CountsChanged)));
    }

    #[tokio::test]
    async fn lag_becomes_counts_changed() {
        let hub = Hub::default();
        let env = Id::new();
        let mut sub = hub.subscribe(&env, "a").unwrap();
        for _ in 0..BUFFER + 4 {
            hub.publish(&env, "a", Event::CountsChanged);
        }
        assert!(matches!(sub.recv().await, Some(Event::CountsChanged)));
    }
}
