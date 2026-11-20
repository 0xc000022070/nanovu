use hmac::{Hmac, KeyInit, Mac};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::{ConditionallySelectable, ConstantTimeEq};

use crate::env::{Environment, Environments};
use crate::error::HttpError;
use crate::id::Id;
use crate::time;

const SESSION_ISSUER: &str = "novu_api";
const SESSION_AUDIENCE: &str = "widget_user";
const SESSION_TTL_MS: i64 = 15 * time::DAY;

pub(crate) fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let Ok(mut mac) = <Hmac<Sha256> as KeyInit>::new_from_slice(key) else {
        unreachable!("HMAC accepts keys of any length")
    };
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

fn hkdf_sha256(ikm: &[u8], info: &[u8]) -> [u8; 32] {
    let prk = hmac_sha256(&[0; 32], &[ikm]);
    hmac_sha256(&prk, &[info, &[1]])
}

/// JWT key for inbox sessions. Not an HMAC keyed by the secret, so no `subscriber_hash` equals it.
pub(crate) fn session_key(secret: &[u8]) -> [u8; 32] {
    hkdf_sha256(secret, b"nanovu session jwt v1")
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionClaims {
    environment_id: Id,
    subscriber_id: String,
    iss: String,
    aud: String,
    iat: i64,
    exp: i64,
}

impl Environment {
    pub fn subscriber_hash(&self, subscriber_id: &str) -> String {
        hex::encode(hmac_sha256(
            self.secret_key.expose().as_bytes(),
            &[subscriber_id.as_bytes()],
        ))
    }

    pub fn verify_subscriber_hash(&self, subscriber_id: &str, hash: &str) -> bool {
        let expected = hmac_sha256(
            self.secret_key.expose().as_bytes(),
            &[subscriber_id.as_bytes()],
        );
        let mut given = [0u8; 32];
        let decoded = hex::decode_to_slice(hash, &mut given).is_ok();
        decoded & bool::from(expected.ct_eq(&given))
    }

    pub fn issue_session(&self, subscriber_id: &str) -> Result<String, HttpError> {
        let iat = time::now_ms() / 1000;
        let claims = SessionClaims {
            environment_id: self.id.clone(),
            subscriber_id: subscriber_id.to_owned(),
            iss: SESSION_ISSUER.to_owned(),
            aud: SESSION_AUDIENCE.to_owned(),
            iat,
            exp: iat + SESSION_TTL_MS / 1000,
        };
        EncodingKey::from_secret(&self.session_key)
            .and_then(|key| jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &key))
            .map_err(|err| {
                tracing::error!(error = %err, "session token encoding failed");
                HttpError::internal()
            })
    }

    pub fn sign_bridge(&self, timestamp_ms: i64, body: &[u8]) -> String {
        let t = timestamp_ms.to_string();
        let mac = hmac_sha256(
            self.secret_key.expose().as_bytes(),
            &[t.as_bytes(), b".", body],
        );
        format!("t={t},v1={}", hex::encode(mac))
    }

    pub fn verify_bridge_signature(&self, header: &str, body: &[u8]) -> bool {
        let field = |name: &str| {
            header
                .split(',')
                .find_map(|kv| kv.trim().strip_prefix(name)?.strip_prefix('='))
        };
        let (Some(t), Some(v1)) = (field("t"), field("v1")) else {
            return false;
        };
        let Ok(t) = t.parse::<i64>() else {
            return false;
        };
        let expected = self.sign_bridge(t, body);
        let expected = expected.rsplit_once("v1=").map_or("", |(_, v)| v);
        bool::from(expected.as_bytes().ct_eq(v1.as_bytes()))
    }
}

impl Environments {
    pub fn authenticate_api_key(&self, key: &str) -> Option<&Environment> {
        let digest: [u8; 32] = Sha256::digest(key.as_bytes()).into();
        let mut found = u32::MAX;
        for (i, env) in self.0.iter().enumerate() {
            let index = u32::try_from(i).unwrap_or(u32::MAX);
            found.conditional_assign(&index, env.key_digest.ct_eq(&digest));
        }
        self.0.get(usize::try_from(found).ok()?)
    }

    pub fn verify_session(&self, token: &str) -> Option<(&Environment, String)> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[SESSION_ISSUER]);
        validation.set_audience(&[SESSION_AUDIENCE]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        let now = time::now_ms() / 1000;
        self.0.iter().find_map(|env| {
            let key = DecodingKey::from_secret(&env.session_key).ok()?;
            let claims = jsonwebtoken::decode::<SessionClaims>(token, &key, &validation)
                .ok()?
                .claims;
            let lifetime = claims.exp.checked_sub(claims.iat)?;
            (claims.environment_id == env.id
                && claims.iat <= now + validation.leeway.cast_signed()
                && lifetime <= SESSION_TTL_MS / 1000)
                .then_some((env, claims.subscriber_id))
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::config::tests::{TempDir, sample};

    #[test]
    fn subscriber_hash_matches_node_crypto() {
        let dir = TempDir::new();
        let config = sample(&dir);
        let env = config.environments.by_name("stg").unwrap();
        let hash = env.subscriber_hash("sub-1");
        assert_eq!(
            hash,
            "bce0d8f0e8107a138d694354d7d14ca34d0bf8ed0c276c7f90fe348dfbb154db"
        );
        assert!(env.verify_subscriber_hash("sub-1", &hash));
        assert!(env.verify_subscriber_hash("sub-1", &hash.to_uppercase()));
        assert!(!env.verify_subscriber_hash("sub-2", &hash));
        assert!(!env.verify_subscriber_hash("sub-1", "zz"));
    }

    #[test]
    fn api_key_lookup_finds_owner_only() {
        let dir = TempDir::new();
        let config = sample(&dir);
        let envs = &config.environments;
        let env = envs.by_name("stg").unwrap();
        assert_eq!(
            envs.authenticate_api_key(env.secret_key.expose())
                .unwrap()
                .name,
            "stg"
        );
        assert!(envs.authenticate_api_key("nope").is_none());
        assert!(envs.authenticate_api_key("").is_none());
    }

    #[test]
    fn session_round_trip_and_tamper() {
        let dir = TempDir::new();
        let config = sample(&dir);
        let envs = &config.environments;
        let env = envs.by_name("stg").unwrap();
        let token = env.issue_session("sub-1").unwrap();
        let (found, subscriber) = envs.verify_session(&token).unwrap();
        assert_eq!((found.name.as_str(), subscriber.as_str()), ("stg", "sub-1"));

        let mut tampered = token.clone();
        tampered.pop();
        tampered.push(if token.ends_with('A') { 'B' } else { 'A' });
        assert!(envs.verify_session(&tampered).is_none());
        assert!(envs.verify_session("not.a.jwt").is_none());
    }

    #[test]
    fn hkdf_matches_rfc5869_test_case_3() {
        assert_eq!(
            hex::encode(hkdf_sha256(&[0x0b; 22], b"")),
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d"
        );
    }

    #[test]
    fn sessions_need_the_derived_key_and_a_bounded_lifetime() {
        let dir = TempDir::new();
        let config = sample(&dir);
        let envs = &config.environments;
        let env = envs.by_name("stg").unwrap();
        let now = time::now_ms() / 1000;
        let sign = |key: &[u8], iat: i64, exp: i64| {
            let claims = SessionClaims {
                environment_id: env.id.clone(),
                subscriber_id: "victim".into(),
                iss: SESSION_ISSUER.into(),
                aud: SESSION_AUDIENCE.into(),
                iat,
                exp,
            };
            let key = EncodingKey::from_secret(key).unwrap();
            jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &key).unwrap()
        };
        let ttl = SESSION_TTL_MS / 1000;
        assert!(
            envs.verify_session(&sign(&env.session_key, now, now + ttl))
                .is_some()
        );

        for label in ["nanovu/session", "nanovu session jwt v1"] {
            let hash = hex::decode(env.subscriber_hash(label)).unwrap();
            assert!(envs.verify_session(&sign(&hash, now, now + ttl)).is_none());
        }
        let long = sign(&env.session_key, now, now + ttl + 1);
        assert!(envs.verify_session(&long).is_none());
        let future = sign(&env.session_key, now + 3600, now + 3600 + ttl);
        assert!(envs.verify_session(&future).is_none());
        let no_iat = json!({
            "environmentId": env.id,
            "subscriberId": "victim",
            "iss": SESSION_ISSUER,
            "aud": SESSION_AUDIENCE,
            "exp": now + ttl,
        });
        let key = EncodingKey::from_secret(&env.session_key).unwrap();
        let no_iat = jsonwebtoken::encode(&Header::new(Algorithm::HS256), &no_iat, &key).unwrap();
        assert!(envs.verify_session(&no_iat).is_none());
    }

    #[test]
    fn bridge_signature_round_trip() {
        let dir = TempDir::new();
        let config = sample(&dir);
        let env = config.environments.by_name("stg").unwrap();
        let header = env.sign_bridge(1_700_000_000_000, br#"{"a":1}"#);
        assert_eq!(
            header,
            "t=1700000000000,v1=14d903a7968191213f1336d145a46fb179d74631812ab6f38f0018433c500c39"
        );
        assert!(env.verify_bridge_signature(&header, br#"{"a":1}"#));
        assert!(!env.verify_bridge_signature(&header, br#"{"a":2}"#));
        assert!(!env.verify_bridge_signature("t=1,v1=00", br#"{"a":1}"#));
    }
}
