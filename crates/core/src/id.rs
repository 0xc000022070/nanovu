use std::fmt;
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Id(String);

static LAST: Mutex<(u64, u64)> = Mutex::new((0, 0));

impl Id {
    pub fn new() -> Self {
        let now = u64::try_from(crate::time::now_ms()).unwrap_or(0);
        let mut last = LAST.lock().unwrap_or_else(PoisonError::into_inner);
        let (ms, seq) = if now > last.0 {
            (now, getrandom::u64().unwrap_or(0) & 0x7fff_ffff)
        } else if last.1 < 0xffff_ffff_ffff {
            (last.0, last.1 + 1)
        } else {
            (last.0 + 1, 0)
        };
        *last = (ms, seq);
        Self(format!("{:012x}{:012x}", ms & 0xffff_ffff_ffff, seq))
    }

    pub fn for_environment(name: &str) -> Self {
        let mut hex = hex::encode(Sha256::digest(name.as_bytes()));
        hex.truncate(24);
        Self(hex)
    }

    pub fn parse(s: &str) -> Option<Self> {
        (s.len() == 24 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
            .then(|| Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).ok_or_else(|| serde::de::Error::custom("expected a 24 hex char id"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_hex_and_strictly_increasing() {
        let ids: Vec<Id> = (0..10_000).map(|_| Id::new()).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]));
        assert!(ids.iter().all(|id| Id::parse(id.as_str()).is_some()));
    }

    #[test]
    fn environment_id_is_stable_prefix_of_sha256() {
        let id = Id::for_environment("stg");
        assert_eq!(id.as_str().len(), 24);
        assert_eq!(id, Id::for_environment("stg"));
        assert_ne!(id, Id::for_environment("prod"));
    }

    #[test]
    fn parse_rejects_non_ids() {
        assert!(Id::parse("ABCDEF000000000000000000").is_none());
        assert!(Id::parse("abc").is_none());
        assert!(Id::parse("0123456789abcdef01234567").is_some());
    }
}
