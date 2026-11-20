use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::auth::session_key;
use crate::env::{Environment, Environments, Fcm, Integration, Provider, Secret};
use crate::id::Id;

const MIN_SECRET_KEY_LEN: usize = 32;

#[derive(Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub database: PathBuf,
    pub environments: Environments,
}

#[derive(Debug)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

fn fail<T>(message: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError(message.into()))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_listen")]
    listen: SocketAddr,
    database: PathBuf,
    #[serde(default)]
    environments: BTreeMap<String, RawEnvironment>,
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 4010))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEnvironment {
    application_identifier: String,
    secret_key_file: PathBuf,
    #[serde(default)]
    bridge_hosts: Vec<String>,
    #[serde(default)]
    integrations: BTreeMap<String, RawIntegration>,
}

#[derive(Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase", deny_unknown_fields)]
enum RawIntegration {
    Fcm {
        service_account_file: PathBuf,
        endpoint: Option<String>,
    },
    Smtp {
        url_file: PathBuf,
        from: String,
    },
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)
            .or_else(|e| fail(format!("cannot read config {}: {e}", path.display())))?;
        Self::parse(&text)
    }

    fn parse(text: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig =
            toml::from_str(text).or_else(|e| fail(format!("invalid config: {e}")))?;
        if raw.environments.is_empty() {
            return fail("no environments configured");
        }

        let mut app_ids = HashSet::new();
        let mut key_digests = HashSet::new();
        let mut environments = Vec::with_capacity(raw.environments.len());
        for (name, env) in raw.environments {
            if !app_ids.insert(env.application_identifier.clone()) {
                return fail(format!(
                    "environment {name}: duplicate application_identifier"
                ));
            }
            let secret_key = read_secret(&env.secret_key_file)?;
            if secret_key.expose().chars().count() < MIN_SECRET_KEY_LEN {
                return fail(format!(
                    "environment {name}: secret key shorter than {MIN_SECRET_KEY_LEN} chars"
                ));
            }
            let key_digest: [u8; 32] = Sha256::digest(secret_key.expose().as_bytes()).into();
            if !key_digests.insert(key_digest) {
                return fail(format!(
                    "environment {name}: secret key reused by another environment"
                ));
            }
            let integrations = env
                .integrations
                .into_iter()
                .map(|(identifier, raw)| {
                    let provider = match raw {
                        RawIntegration::Fcm {
                            service_account_file,
                            endpoint,
                        } => Provider::Fcm(Fcm {
                            service_account: read_secret(&service_account_file)?,
                            endpoint,
                        }),
                        RawIntegration::Smtp { url_file, from } => Provider::Smtp {
                            url: read_secret(&url_file)?,
                            from,
                        },
                    };
                    Ok(Integration {
                        identifier,
                        provider,
                    })
                })
                .collect::<Result<Vec<Integration>, ConfigError>>()?;
            if integrations
                .iter()
                .filter(|i| matches!(i.provider, Provider::Smtp { .. }))
                .count()
                > 1
            {
                return fail(format!(
                    "environment {name}: more than one smtp integration (each would send every email)"
                ));
            }
            environments.push(Environment {
                id: Id::for_environment(&name),
                session_key: session_key(secret_key.expose().as_bytes()),
                key_digest,
                name,
                application_identifier: env.application_identifier,
                secret_key,
                bridge_hosts: env.bridge_hosts,
                integrations,
            });
        }

        Ok(Self {
            listen: raw.listen,
            database: raw.database,
            environments: Environments(environments),
        })
    }
}

fn read_secret(path: &Path) -> Result<Secret, ConfigError> {
    let meta = std::fs::metadata(path)
        .or_else(|e| fail(format!("secret file {}: {e}", path.display())))?;
    if meta.permissions().mode() & 0o044 != 0 {
        return fail(format!(
            "secret file {} is readable by group or others (chmod 0400 or 0600)",
            path.display()
        ));
    }
    let value = std::fs::read_to_string(path)
        .or_else(|e| fail(format!("secret file {}: {e}", path.display())))?;
    let value = value.trim_end_matches(['\n', '\r']);
    if value.is_empty() {
        return fail(format!("secret file {} is empty", path.display()));
    }
    Ok(Secret::new(value.to_owned()))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::fs;

    pub(crate) struct TempDir(pub PathBuf);

    impl TempDir {
        pub(crate) fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("nanovu-test-{}", Id::new()));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        pub(crate) fn secret(&self, name: &str, value: &str, mode: u32) -> PathBuf {
            let path = self.0.join(name);
            fs::write(&path, value).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const KEY_A: &str = "0123456789abcdef0123456789abcdef";
    const KEY_B: &str = "fedcba9876543210fedcba9876543210";

    fn env_stanza(name: &str, app: &str, key: &Path) -> String {
        format!(
            "[environments.{name}]\napplication_identifier = \"{app}\"\nsecret_key_file = \"{}\"\n",
            key.display()
        )
    }

    fn smtp_stanza(env: &str, identifier: &str, url: &Path) -> String {
        format!(
            "[environments.{env}.integrations.{identifier}]\nprovider = \"smtp\"\nurl_file = \"{}\"\nfrom = \"a@b.c\"\n",
            url.display()
        )
    }

    pub(crate) fn sample(dir: &TempDir) -> Config {
        let key = dir.secret("stg-key", &format!("{KEY_A}\n"), 0o400);
        let fcm = dir.secret("fcm.json", "{}", 0o600);
        let text = format!(
            "database = \"/tmp/x.db\"\n{}bridge_hosts = [\"localhost\"]\n\
             [environments.stg.integrations.push]\nprovider = \"fcm\"\nservice_account_file = \"{}\"\n",
            env_stanza("stg", "app-stg", &key),
            fcm.display()
        );
        Config::parse(&text).unwrap()
    }

    #[test]
    fn parses_environments_and_integrations() {
        let dir = TempDir::new();
        let config = sample(&dir);
        assert_eq!(config.listen, default_listen());
        let env = config.environments.by_name("stg").unwrap();
        assert_eq!(env.id, Id::for_environment("stg"));
        assert_eq!(env.secret_key.expose(), KEY_A);
        assert_eq!(env.integration("fcm", None).unwrap().identifier, "push");
        assert!(env.integration("fcm", Some("other")).is_none());
        assert!(!format!("{config:?}").contains(KEY_A));
    }

    #[test]
    fn refuses_unsafe_or_ambiguous_configs() {
        let dir = TempDir::new();
        let a = dir.secret("a", KEY_A, 0o600);
        let b = dir.secret("b", KEY_B, 0o600);
        let a_again = dir.secret("a2", KEY_A, 0o400);
        let open = dir.secret("open", KEY_B, 0o640);
        let short = dir.secret("short", "too-short", 0o600);
        let smtp = dir.secret("smtp", "smtp://localhost:25", 0o600);
        let db = "database = \"/tmp/x.db\"\n";
        let cases = [
            (db.to_string(), "no environments"),
            (
                format!("{db}{}", env_stanza("x", "app", &open)),
                "readable by group",
            ),
            (
                format!("{db}{}", env_stanza("x", "app", &dir.0.join("missing"))),
                "secret file",
            ),
            (
                format!("{db}{}", env_stanza("x", "app", &short)),
                "shorter than 32",
            ),
            (
                format!(
                    "{db}{}{}",
                    env_stanza("x", "app", &a),
                    env_stanza("y", "app", &b)
                ),
                "duplicate application_identifier",
            ),
            (
                format!(
                    "{db}{}{}",
                    env_stanza("x", "a1", &a),
                    env_stanza("y", "a2", &a_again)
                ),
                "secret key reused",
            ),
            (
                format!(
                    "{db}{}{}{}",
                    env_stanza("x", "app", &a),
                    smtp_stanza("x", "one", &smtp),
                    smtp_stanza("x", "two", &smtp)
                ),
                "more than one smtp integration",
            ),
            (format!("{db}bogus = 1\n"), "invalid config"),
        ];
        for (text, expected) in cases {
            let err = Config::parse(&text).unwrap_err().to_string();
            assert!(
                err.contains(expected),
                "{err:?} should contain {expected:?}"
            );
            assert!(!err.contains(KEY_A) && !err.contains(KEY_B));
        }
    }
}
