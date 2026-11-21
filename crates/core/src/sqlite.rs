use std::collections::BTreeSet;
use std::fs::{OpenOptions, Permissions};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePool, SqlitePoolOptions,
    SqliteRow, SqliteSynchronous,
};
use sqlx::{QueryBuilder, Row, Sqlite};

use crate::domain::{
    Channel, ChannelPrefs, DigestEvent, DigestJoin, InboxPage, Mark, Message, MessageFilter,
    MessageQuery, Run, RunStatus, Severity, Subscriber, SubscriberPatch, SubscriberPreferences,
    SubscriberUpdate, Topic, TopicMember, TopicSubscription, TopicSubscriptions, Workflow,
};
use crate::id::Id;
use crate::store::{Result, Store, StoreError};
use crate::time::{self, DAY, HOUR};

const MIGRATIONS: &[&str] = &[include_str!("../migrations/0001_init.sql")];

const MESSAGE_RETENTION: i64 = 90 * DAY;
const RUN_RETENTION: i64 = 7 * DAY;
const IDEMPOTENCY_WINDOW: i64 = 24 * HOUR;
macro_rules! released_run {
    () => {
        "status IN ('done', 'failed', 'cancelled', 'merged')
    AND NOT EXISTS (SELECT 1 FROM digest_events e JOIN runs o ON o.env_id = e.env_id AND o.id = e.owner_run_id
      WHERE e.env_id = runs.env_id AND e.run_id = runs.id AND o.status IN ('pending', 'running'))"
    };
}

impl From<sqlx::Error> for StoreError {
    fn from(err: sqlx::Error) -> Self {
        Self::new(err)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(err: serde_json::Error) -> Self {
        Self::new(err)
    }
}

pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub async fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path);
        file.and_then(|_| std::fs::set_permissions(path, Permissions::from_mode(0o600)))
            .map_err(StoreError::new)?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);
        Self::connect(options).await
    }

    pub async fn open_in_memory() -> Result<Self> {
        Self::connect(SqliteConnectOptions::from_str("sqlite::memory:")?).await
    }

    async fn connect(options: SqliteConnectOptions) -> Result<Self> {
        let options = options
            .busy_timeout(Duration::from_secs(5))
            .statement_cache_capacity(32)
            .pragma("cache_size", "-512")
            .pragma("temp_store", "memory");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(options)
            .await?;
        migrate(&pool).await?;
        Ok(Self { pool })
    }
}

async fn migrate(pool: &SqlitePool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *conn)
        .await?;
    for (index, sql) in MIGRATIONS
        .iter()
        .enumerate()
        .skip(usize::try_from(version).unwrap_or(0))
    {
        let mut tx = sqlx::Connection::begin(&mut *conn).await?;
        sqlx::raw_sql(*sql).execute(&mut *tx).await?;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "PRAGMA user_version = {}",
            index + 1
        )))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(())
}

fn to_json<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn opt_json<T: Serialize>(value: &Option<T>) -> Result<Option<String>> {
    value.as_ref().map(to_json).transpose()
}

fn from_json<T: DeserializeOwned>(row: &SqliteRow, column: &str) -> Result<T> {
    Ok(serde_json::from_str(row.try_get(column)?)?)
}

fn from_opt_json<T: DeserializeOwned>(row: &SqliteRow, column: &str) -> Result<Option<T>> {
    let text: Option<&str> = row.try_get(column)?;
    Ok(text.map(serde_json::from_str).transpose()?)
}

fn id(row: &SqliteRow, column: &str) -> Result<Id> {
    let text: &str = row.try_get(column)?;
    Id::parse(text).ok_or_else(|| StoreError::new(format!("malformed id in {column}")))
}

fn opt_id(row: &SqliteRow, column: &str) -> Result<Option<Id>> {
    let text: Option<&str> = row.try_get(column)?;
    text.map(|t| Id::parse(t).ok_or_else(|| StoreError::new(format!("malformed id in {column}"))))
        .transpose()
}

fn parsed<T>(row: &SqliteRow, column: &str, parse: fn(&str) -> Option<T>) -> Result<T> {
    let text: &str = row.try_get(column)?;
    parse(text).ok_or_else(|| StoreError::new(format!("malformed {column}")))
}

fn cols<'r, T, const N: usize>(row: &'r SqliteRow, names: [&str; N]) -> Result<[T; N]>
where
    T: sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite>,
{
    let values = names
        .iter()
        .map(|c| row.try_get(*c))
        .collect::<std::result::Result<Vec<T>, _>>()?;
    values
        .try_into()
        .map_err(|_| StoreError::new("column count"))
}

fn subscriber_from(row: &SqliteRow) -> Result<Subscriber> {
    Ok(Subscriber {
        id: id(row, "id")?,
        subscriber_id: row.try_get("subscriber_id")?,
        email: row.try_get("email")?,
        first_name: row.try_get("first_name")?,
        last_name: row.try_get("last_name")?,
        phone: row.try_get("phone")?,
        avatar: row.try_get("avatar")?,
        locale: row.try_get("locale")?,
        timezone: row.try_get("timezone")?,
        data: from_opt_json(row, "data")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn topic_from(row: &SqliteRow) -> Result<Topic> {
    Ok(Topic {
        id: id(row, "id")?,
        key: row.try_get("key")?,
        name: row.try_get("name")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn workflow_from(row: &SqliteRow) -> Result<Workflow> {
    Ok(Workflow {
        id: id(row, "id")?,
        workflow_id: row.try_get("workflow_id")?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        tags: from_json(row, "tags")?,
        severity: parsed(row, "severity", Severity::parse)?,
        preferences: from_json(row, "preferences")?,
        payload_schema: from_opt_json(row, "payload_schema")?,
        steps: from_json(row, "steps")?,
        bridge_url: row.try_get("bridge_url")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn message_from(row: &SqliteRow) -> Result<Message> {
    let [transaction_id, subscriber_id, workflow_id, step_id] = cols(
        row,
        ["transaction_id", "subscriber_id", "workflow_id", "step_id"],
    )?;
    let [subject, avatar] = cols(row, ["subject", "avatar"])?;
    let [seen_at, read_at, archived_at] = cols(row, ["seen_at", "read_at", "archived_at"])?;
    let [created_at, updated_at] = cols(row, ["created_at", "updated_at"])?;
    Ok(Message {
        id: id(row, "id")?,
        run_id: opt_id(row, "run_id")?,
        transaction_id,
        subscriber_id,
        workflow_id,
        step_id,
        channel: parsed(row, "channel", Channel::parse)?,
        subject,
        body: row.try_get("body")?,
        avatar,
        redirect: from_opt_json(row, "redirect")?,
        primary_action: from_opt_json(row, "primary_action")?,
        secondary_action: from_opt_json(row, "secondary_action")?,
        data: from_opt_json(row, "data")?,
        tags: from_json(row, "tags")?,
        severity: parsed(row, "severity", Severity::parse)?,
        seen_at,
        read_at,
        archived_at,
        created_at,
        updated_at,
    })
}

fn run_from(row: &SqliteRow) -> Result<Run> {
    let [workflow_id, subscriber_id, transaction_id, bridge_url] = cols(
        row,
        [
            "workflow_id",
            "subscriber_id",
            "transaction_id",
            "bridge_url",
        ],
    )?;
    let [actor, digest_key] = cols(row, ["actor", "digest_key"])?;
    let [step_index, attempts]: [i64; 2] = cols(row, ["step_index", "attempts"])?;
    let [run_at, created_at, updated_at] = cols(row, ["run_at", "created_at", "updated_at"])?;
    Ok(Run {
        id: id(row, "id")?,
        env_id: id(row, "env_id")?,
        workflow_id,
        subscriber_id,
        transaction_id,
        bridge_url,
        payload: Arc::new(from_json(row, "payload")?),
        actor,
        step_index: u32::try_from(step_index).map_err(StoreError::new)?,
        state: from_json(row, "state")?,
        status: parsed(row, "status", RunStatus::parse)?,
        run_at,
        attempts: u32::try_from(attempts).map_err(StoreError::new)?,
        digest_key,
        created_at,
        updated_at,
    })
}

fn digest_event_from(row: &SqliteRow) -> Result<DigestEvent> {
    Ok(DigestEvent {
        id: id(row, "id")?,
        run_id: id(row, "run_id")?,
        payload: from_json(row, "payload")?,
        created_at: row.try_get("created_at")?,
    })
}

fn mark_assignments(mark: Mark, now: i64) -> String {
    let seen = format!("seen_at = COALESCE(seen_at, {now})");
    let read = format!("read_at = COALESCE(read_at, {now})");
    let archived = format!("archived_at = COALESCE(archived_at, {now})");
    let set = match mark {
        Mark::Seen => seen,
        Mark::Read => format!("{seen}, {read}"),
        Mark::Archive => format!("{seen}, {read}, {archived}"),
        Mark::Unseen => "seen_at = NULL".to_owned(),
        Mark::Unread => format!("{seen}, read_at = NULL, archived_at = NULL"),
        Mark::Unarchive => format!("{seen}, {read}, archived_at = NULL"),
    };
    format!("{set}, updated_at = {now}")
}

fn push_nullness(qb: &mut QueryBuilder<Sqlite>, column: &str, set: Option<bool>) {
    match set {
        Some(true) => qb.push(format_args!(" AND {column} IS NOT NULL")),
        Some(false) => qb.push(format_args!(" AND {column} IS NULL")),
        None => qb,
    };
}

fn push_filter(qb: &mut QueryBuilder<Sqlite>, filter: &MessageFilter) -> Result<()> {
    qb.push(" AND channel = 'in_app'");
    for group in &filter.tags {
        qb.push(" AND EXISTS (SELECT 1 FROM json_each(messages.tags) t WHERE t.value IN (SELECT value FROM json_each(")
            .push_bind(to_json(group)?)
            .push(")))");
    }
    push_nullness(qb, "read_at", filter.read);
    push_nullness(qb, "seen_at", filter.seen);
    push_nullness(qb, "archived_at", filter.archived);
    if filter.snoozed == Some(true) {
        qb.push(" AND 0");
    }
    if !filter.severity.is_empty() {
        let names: Vec<&str> = filter.severity.iter().map(|s| s.as_str()).collect();
        qb.push(" AND severity IN (SELECT value FROM json_each(")
            .push_bind(to_json(&names)?)
            .push("))");
    }
    for (keys, candidates) in &filter.data {
        if keys.iter().any(|key| key.contains('"')) {
            qb.push(" AND 0");
            continue;
        }
        let path: String = keys.iter().map(|key| format!(".\"{key}\"")).collect();
        let path = format!("${path}");
        // json_each yields the value at the path, or its elements when it is an array; an
        // object would yield its members, hence the json_type guard. Types keep true apart from 1.
        qb.push(" AND EXISTS (SELECT 1 FROM json_each(messages.data, ")
            .push_bind(path.clone())
            .push(") d, json_each(")
            .push_bind(to_json(candidates)?)
            .push(") c WHERE d.value IS c.value AND replace(d.type, 'real', 'integer') = replace(c.type, 'real', 'integer') AND json_type(messages.data, ")
            .push_bind(path)
            .push(") != 'object')");
    }
    Ok(())
}

async fn subscriber_in(
    conn: &mut SqliteConnection,
    env: &Id,
    subscriber_id: &str,
) -> Result<Option<Subscriber>> {
    sqlx::query("SELECT * FROM subscribers WHERE env_id = ? AND subscriber_id = ?")
        .bind(env.as_str())
        .bind(subscriber_id)
        .fetch_optional(conn)
        .await?
        .as_ref()
        .map(subscriber_from)
        .transpose()
}

async fn upsert_topic_in(
    conn: &mut SqliteConnection,
    env: &Id,
    key: &str,
    name: Option<&str>,
) -> Result<Topic> {
    let now = time::now_ms();
    let row = sqlx::query(
        "INSERT INTO topics (env_id, key, id, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (env_id, key) DO UPDATE SET
           name = COALESCE(excluded.name, name),
           updated_at = CASE WHEN excluded.name IS NULL THEN updated_at ELSE excluded.updated_at END
         RETURNING *",
    )
    .bind(env.as_str())
    .bind(key)
    .bind(Id::new().as_str())
    .bind(name)
    .bind(now)
    .bind(now)
    .fetch_one(conn)
    .await?;
    topic_from(&row)
}

async fn hand_over_digest(
    conn: &mut SqliteConnection,
    env: &Id,
    run: &Run,
    now: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM digest_events WHERE env_id = ? AND owner_run_id = ? AND run_id = ?")
        .bind(env.as_str())
        .bind(run.id.as_str())
        .bind(run.id.as_str())
        .execute(&mut *conn)
        .await?;
    let heir: Option<String> = sqlx::query_scalar(
        "SELECT run_id FROM digest_events WHERE env_id = ? AND owner_run_id = ? ORDER BY id LIMIT 1",
    )
    .bind(env.as_str())
    .bind(run.id.as_str())
    .fetch_optional(&mut *conn)
    .await?;
    if let Some(heir) = heir {
        sqlx::query(
            "UPDATE digest_events SET owner_run_id = ? WHERE env_id = ? AND owner_run_id = ?",
        )
        .bind(&heir)
        .bind(env.as_str())
        .bind(run.id.as_str())
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "UPDATE runs SET status = 'pending', run_at = ?, digest_key = ?, updated_at = ?
             WHERE env_id = ? AND id = ?",
        )
        .bind(run.run_at)
        .bind(run.digest_key.as_deref())
        .bind(now)
        .bind(env.as_str())
        .bind(&heir)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

async fn save_run_in(conn: &mut SqliteConnection, run: &Run) -> Result<bool> {
    let saved = sqlx::query(
        "UPDATE runs SET step_index = ?, state = ?, status = ?, run_at = ?, attempts = ?, digest_key = ?,
           updated_at = ? WHERE env_id = ? AND id = ? AND status != 'cancelled'",
    )
    .bind(i64::from(run.step_index))
    .bind(to_json(&run.state)?)
    .bind(run.status.as_str())
    .bind(run.run_at)
    .bind(i64::from(run.attempts))
    .bind(run.digest_key.as_deref())
    .bind(time::now_ms())
    .bind(run.env_id.as_str())
    .bind(run.id.as_str())
    .execute(conn)
    .await?;
    Ok(saved.rows_affected() > 0)
}

async fn insert_digest_event(conn: &mut SqliteConnection, run: &Run, owner: &Id) -> Result<()> {
    sqlx::query(
        "INSERT INTO digest_events (env_id, id, owner_run_id, run_id, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(run.env_id.as_str())
    .bind(Id::new().as_str())
    .bind(owner.as_str())
    .bind(run.id.as_str())
    .bind(to_json(&*run.payload)?)
    .bind(time::now_ms())
    .execute(conn)
    .await?;
    Ok(())
}

async fn patch_subscriber_in(
    conn: &mut SqliteConnection,
    env: &Id,
    subscriber_id: &str,
    patch: &SubscriberPatch,
) -> Result<Option<Subscriber>> {
    sqlx::query(
        "UPDATE subscribers SET email = COALESCE(?, email), first_name = COALESCE(?, first_name),
           last_name = COALESCE(?, last_name), phone = COALESCE(?, phone), avatar = COALESCE(?, avatar),
           locale = COALESCE(?, locale), timezone = COALESCE(?, timezone), data = COALESCE(?, data),
           updated_at = ?
         WHERE env_id = ? AND subscriber_id = ? RETURNING *",
    )
    .bind(patch.email.as_deref())
    .bind(patch.first_name.as_deref())
    .bind(patch.last_name.as_deref())
    .bind(patch.phone.as_deref())
    .bind(patch.avatar.as_deref())
    .bind(patch.locale.as_deref())
    .bind(patch.timezone.as_deref())
    .bind(opt_json(&patch.data)?)
    .bind(time::now_ms())
    .bind(env.as_str())
    .bind(subscriber_id)
    .fetch_optional(conn)
    .await?
    .as_ref()
    .map(subscriber_from)
    .transpose()
}

#[async_trait]
impl Store for SqliteStore {
    async fn upsert_subscriber(
        &self,
        env: &Id,
        subscriber_id: &str,
        patch: &SubscriberPatch,
    ) -> Result<Subscriber> {
        let now = time::now_ms();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query(
            "INSERT INTO subscribers (env_id, subscriber_id, id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .bind(Id::new().as_str())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let subscriber = patch_subscriber_in(&mut tx, env, subscriber_id, patch)
            .await?
            .ok_or_else(|| StoreError::new("upserted subscriber missing"))?;
        tx.commit().await?;
        Ok(subscriber)
    }

    async fn update_subscriber(
        &self,
        env: &Id,
        subscriber_id: &str,
        update: &SubscriberUpdate,
    ) -> Result<Option<Subscriber>> {
        fn text(v: &Option<Option<String>>) -> Option<&str> {
            v.as_ref().and_then(Option::as_deref)
        }
        let data = update.data.as_ref().and_then(Option::as_ref);
        sqlx::query(
            "UPDATE subscribers SET
               email = CASE WHEN ? THEN ? ELSE email END,
               first_name = CASE WHEN ? THEN ? ELSE first_name END,
               last_name = CASE WHEN ? THEN ? ELSE last_name END,
               phone = CASE WHEN ? THEN ? ELSE phone END,
               avatar = CASE WHEN ? THEN ? ELSE avatar END,
               locale = CASE WHEN ? THEN ? ELSE locale END,
               timezone = CASE WHEN ? THEN ? ELSE timezone END,
               data = CASE WHEN ? THEN ? ELSE data END,
               updated_at = ?
             WHERE env_id = ? AND subscriber_id = ? RETURNING *",
        )
        .bind(update.email.is_some())
        .bind(text(&update.email))
        .bind(update.first_name.is_some())
        .bind(text(&update.first_name))
        .bind(update.last_name.is_some())
        .bind(text(&update.last_name))
        .bind(update.phone.is_some())
        .bind(text(&update.phone))
        .bind(update.avatar.is_some())
        .bind(text(&update.avatar))
        .bind(update.locale.is_some())
        .bind(text(&update.locale))
        .bind(update.timezone.is_some())
        .bind(text(&update.timezone))
        .bind(update.data.is_some())
        .bind(data.map(to_json).transpose()?)
        .bind(time::now_ms())
        .bind(env.as_str())
        .bind(subscriber_id)
        .fetch_optional(&self.pool)
        .await?
        .as_ref()
        .map(subscriber_from)
        .transpose()
    }

    async fn subscriber(&self, env: &Id, subscriber_id: &str) -> Result<Option<Subscriber>> {
        let mut conn = self.pool.acquire().await?;
        subscriber_in(&mut conn, env, subscriber_id).await
    }

    async fn delete_subscriber(&self, env: &Id, subscriber_id: &str) -> Result<bool> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let deleted = sqlx::query("DELETE FROM subscribers WHERE env_id = ? AND subscriber_id = ?")
            .bind(env.as_str())
            .bind(subscriber_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        sqlx::query(
            "DELETE FROM digest_events WHERE env_id = ?1 AND run_id IN
               (SELECT id FROM runs WHERE env_id = ?1 AND subscriber_id = ?2)",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .execute(&mut *tx)
        .await?;
        for table in [
            "device_tokens",
            "preferences",
            "topic_subscribers",
            "messages",
            "runs",
        ] {
            let sql = format!("DELETE FROM {table} WHERE env_id = ? AND subscriber_id = ?");
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(env.as_str())
                .bind(subscriber_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(deleted > 0)
    }

    async fn set_device_tokens(
        &self,
        env: &Id,
        subscriber_id: &str,
        integration: &str,
        tokens: &[String],
    ) -> Result<bool> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if subscriber_in(&mut tx, env, subscriber_id).await?.is_none() {
            return Ok(false);
        }
        sqlx::query(
            "DELETE FROM device_tokens WHERE env_id = ? AND subscriber_id = ? AND integration = ?",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .bind(integration)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO device_tokens (env_id, subscriber_id, integration, token, created_at)
             SELECT ?, ?, ?, value, ? FROM json_each(?)",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .bind(integration)
        .bind(time::now_ms())
        .bind(to_json(tokens)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn device_tokens(
        &self,
        env: &Id,
        subscriber_id: &str,
        integration: &str,
    ) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT token FROM device_tokens WHERE env_id = ? AND subscriber_id = ? AND integration = ?
             ORDER BY created_at, token",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .bind(integration)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn remove_device_tokens(
        &self,
        env: &Id,
        integration: &str,
        tokens: &[String],
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM device_tokens WHERE env_id = ? AND integration = ?
               AND token IN (SELECT value FROM json_each(?))",
        )
        .bind(env.as_str())
        .bind(integration)
        .bind(to_json(tokens)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn upsert_topic(&self, env: &Id, key: &str, name: Option<&str>) -> Result<Topic> {
        let mut conn = self.pool.acquire().await?;
        upsert_topic_in(&mut conn, env, key, name).await
    }

    async fn subscribe_topic(
        &self,
        env: &Id,
        key: &str,
        members: &[TopicMember],
    ) -> Result<TopicSubscriptions> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let topic = upsert_topic_in(&mut tx, env, key, None).await?;
        let mut subscriptions = Vec::new();
        let mut missing = Vec::new();
        for member in members {
            let Some(subscriber) = subscriber_in(&mut tx, env, &member.subscriber_id).await? else {
                missing.push(member.subscriber_id.clone());
                continue;
            };
            let now = time::now_ms();
            let row = sqlx::query(
                "INSERT INTO topic_subscribers (env_id, topic_key, subscriber_id, id, identifier, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (env_id, topic_key, subscriber_id) DO UPDATE SET
                   identifier = excluded.identifier, updated_at = excluded.updated_at
                 RETURNING id, identifier, created_at, updated_at",
            )
            .bind(env.as_str())
            .bind(key)
            .bind(&member.subscriber_id)
            .bind(Id::new().as_str())
            .bind(&member.identifier)
            .bind(now)
            .bind(now)
            .fetch_one(&mut *tx)
            .await?;
            subscriptions.push(TopicSubscription {
                id: id(&row, "id")?,
                identifier: row.try_get("identifier")?,
                subscriber,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            });
        }
        tx.commit().await?;
        Ok(TopicSubscriptions {
            topic,
            subscriptions,
            missing,
        })
    }

    async fn unsubscribe_topic(
        &self,
        env: &Id,
        key: &str,
        subscriber_ids: &[String],
    ) -> Result<Option<TopicSubscriptions>> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some(row) = sqlx::query("SELECT * FROM topics WHERE env_id = ? AND key = ?")
            .bind(env.as_str())
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?
        else {
            return Ok(None);
        };
        let topic = topic_from(&row)?;
        let mut subscriptions = Vec::new();
        let mut missing = Vec::new();
        for subscriber_id in subscriber_ids {
            let removed = sqlx::query(
                "DELETE FROM topic_subscribers WHERE env_id = ? AND topic_key = ? AND subscriber_id = ?
                 RETURNING id, identifier, created_at, updated_at",
            )
            .bind(env.as_str())
            .bind(key)
            .bind(subscriber_id)
            .fetch_optional(&mut *tx)
            .await?;
            let subscriber = subscriber_in(&mut tx, env, subscriber_id).await?;
            match (removed, subscriber) {
                (Some(row), Some(subscriber)) => subscriptions.push(TopicSubscription {
                    id: id(&row, "id")?,
                    identifier: row.try_get("identifier")?,
                    subscriber,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                }),
                _ => missing.push(subscriber_id.clone()),
            }
        }
        tx.commit().await?;
        Ok(Some(TopicSubscriptions {
            topic,
            subscriptions,
            missing,
        }))
    }

    async fn topic_subscriber_ids(&self, env: &Id, key: &str, limit: u32) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT subscriber_id FROM topic_subscribers WHERE env_id = ? AND topic_key = ? ORDER BY subscriber_id LIMIT ?",
        )
        .bind(env.as_str())
        .bind(key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn replace_workflows(&self, env: &Id, workflows: &[Workflow]) -> Result<Vec<Workflow>> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = time::now_ms();
        let mut stored = Vec::with_capacity(workflows.len());
        for wf in workflows {
            let row = sqlx::query(
                "INSERT INTO workflows (env_id, workflow_id, id, name, description, tags, severity, preferences,
                   payload_schema, steps, bridge_url, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (env_id, workflow_id) DO UPDATE SET
                   name = excluded.name, description = excluded.description, tags = excluded.tags,
                   severity = excluded.severity, preferences = excluded.preferences,
                   payload_schema = excluded.payload_schema, steps = excluded.steps,
                   bridge_url = excluded.bridge_url, updated_at = excluded.updated_at
                 RETURNING *",
            )
            .bind(env.as_str())
            .bind(&wf.workflow_id)
            .bind(Id::new().as_str())
            .bind(&wf.name)
            .bind(wf.description.as_deref())
            .bind(to_json(&wf.tags)?)
            .bind(wf.severity.as_str())
            .bind(to_json(&wf.preferences)?)
            .bind(opt_json(&wf.payload_schema)?)
            .bind(to_json(&wf.steps)?)
            .bind(&wf.bridge_url)
            .bind(now)
            .bind(now)
            .fetch_one(&mut *tx)
            .await?;
            stored.push(workflow_from(&row)?);
        }
        let keep: Vec<&str> = workflows.iter().map(|w| w.workflow_id.as_str()).collect();
        sqlx::query(
            "DELETE FROM workflows WHERE env_id = ? AND workflow_id NOT IN (SELECT value FROM json_each(?))",
        )
        .bind(env.as_str())
        .bind(to_json(&keep)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(stored)
    }

    async fn workflow(&self, env: &Id, key: &str) -> Result<Option<Workflow>> {
        sqlx::query("SELECT * FROM workflows WHERE env_id = ?1 AND (workflow_id = ?2 OR id = ?2)")
            .bind(env.as_str())
            .bind(key)
            .fetch_optional(&self.pool)
            .await?
            .as_ref()
            .map(workflow_from)
            .transpose()
    }

    async fn workflows(&self, env: &Id) -> Result<Vec<Workflow>> {
        sqlx::query("SELECT * FROM workflows WHERE env_id = ? ORDER BY id")
            .bind(env.as_str())
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(workflow_from)
            .collect()
    }

    async fn subscriber_preferences(
        &self,
        env: &Id,
        subscriber_id: &str,
    ) -> Result<SubscriberPreferences> {
        let rows = sqlx::query(
            "SELECT workflow_id, channels FROM preferences WHERE env_id = ? AND subscriber_id = ?",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .fetch_all(&self.pool)
        .await?;
        let mut prefs = SubscriberPreferences::default();
        for row in &rows {
            let workflow_id: String = row.try_get("workflow_id")?;
            let channels: ChannelPrefs = from_json(row, "channels")?;
            if workflow_id.is_empty() {
                prefs.global = channels;
            } else {
                prefs.workflows.insert(workflow_id, channels);
            }
        }
        Ok(prefs)
    }

    async fn update_preferences(
        &self,
        env: &Id,
        subscriber_id: &str,
        workflow_id: Option<&str>,
        patch: &ChannelPrefs,
    ) -> Result<ChannelPrefs> {
        let workflow_id = workflow_id.unwrap_or("");
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current: Option<String> = sqlx::query_scalar(
            "SELECT channels FROM preferences WHERE env_id = ? AND subscriber_id = ? AND workflow_id = ?",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .bind(workflow_id)
        .fetch_optional(&mut *tx)
        .await?;
        let current: ChannelPrefs = current
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or_default();
        let merged = current.overlay(patch);
        if workflow_id.is_empty() {
            for channel in Channel::ALL {
                if patch.get(channel).is_none() {
                    continue;
                }
                sqlx::query(
                    "UPDATE preferences SET channels = json_remove(channels, ?), updated_at = ?
                     WHERE env_id = ? AND subscriber_id = ? AND workflow_id != ''",
                )
                .bind(format!("$.{}", channel.as_str()))
                .bind(time::now_ms())
                .bind(env.as_str())
                .bind(subscriber_id)
                .execute(&mut *tx)
                .await?;
            }
        }
        sqlx::query(
            "INSERT INTO preferences (env_id, subscriber_id, workflow_id, channels, updated_at) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT (env_id, subscriber_id, workflow_id) DO UPDATE SET
               channels = excluded.channels, updated_at = excluded.updated_at",
        )
        .bind(env.as_str())
        .bind(subscriber_id)
        .bind(workflow_id)
        .bind(to_json(&merged)?)
        .bind(time::now_ms())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(merged)
    }

    async fn insert_message(&self, env: &Id, m: &Message) -> Result<()> {
        sqlx::query(
            "INSERT INTO messages (env_id, id, run_id, transaction_id, subscriber_id, workflow_id, step_id, channel,
               subject, body, avatar, redirect, primary_action, secondary_action, data, tags, severity,
               seen_at, read_at, archived_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(env.as_str())
        .bind(m.id.as_str())
        .bind(m.run_id.as_ref().map(Id::as_str))
        .bind(&m.transaction_id)
        .bind(&m.subscriber_id)
        .bind(&m.workflow_id)
        .bind(&m.step_id)
        .bind(m.channel.as_str())
        .bind(m.subject.as_deref())
        .bind(&m.body)
        .bind(m.avatar.as_deref())
        .bind(opt_json(&m.redirect)?)
        .bind(opt_json(&m.primary_action)?)
        .bind(opt_json(&m.secondary_action)?)
        .bind(opt_json(&m.data)?)
        .bind(to_json(&m.tags)?)
        .bind(m.severity.as_str())
        .bind(m.seen_at)
        .bind(m.read_at)
        .bind(m.archived_at)
        .bind(m.created_at)
        .bind(m.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn message(&self, env: &Id, subscriber_id: &str, id: &Id) -> Result<Option<Message>> {
        sqlx::query("SELECT * FROM messages WHERE env_id = ? AND subscriber_id = ? AND id = ?")
            .bind(env.as_str())
            .bind(subscriber_id)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await?
            .as_ref()
            .map(message_from)
            .transpose()
    }

    async fn inbox(
        &self,
        env: &Id,
        subscriber_id: &str,
        filter: &MessageFilter,
        page: &InboxPage,
    ) -> Result<(Vec<Message>, bool)> {
        let mut qb = QueryBuilder::<Sqlite>::new("SELECT * FROM messages WHERE env_id = ");
        qb.push_bind(env.as_str())
            .push(" AND subscriber_id = ")
            .push_bind(subscriber_id.to_owned());
        push_filter(&mut qb, filter)?;
        if let Some(after) = &page.after {
            qb.push(" AND id < ").push_bind(after.as_str().to_owned());
        }
        qb.push(" ORDER BY id DESC LIMIT ")
            .push_bind(i64::from(page.limit) + 1)
            .push(" OFFSET ")
            .push_bind(i64::from(page.offset));
        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > page.limit as usize;
        let messages = rows
            .iter()
            .take(page.limit as usize)
            .map(message_from)
            .collect::<Result<_>>()?;
        Ok((messages, has_more))
    }

    async fn inbox_counts(
        &self,
        env: &Id,
        subscriber_id: &str,
        filters: &[MessageFilter],
    ) -> Result<Vec<u64>> {
        let mut conn = self.pool.acquire().await?;
        let mut counts = Vec::with_capacity(filters.len());
        for filter in filters {
            let mut qb =
                QueryBuilder::<Sqlite>::new("SELECT COUNT(*) FROM messages WHERE env_id = ");
            qb.push_bind(env.as_str())
                .push(" AND subscriber_id = ")
                .push_bind(subscriber_id.to_owned());
            push_filter(&mut qb, filter)?;
            let count: i64 = qb.build_query_scalar().fetch_one(&mut *conn).await?;
            counts.push(u64::try_from(count).unwrap_or(0));
        }
        Ok(counts)
    }

    async fn mark_messages(
        &self,
        env: &Id,
        subscriber_id: &str,
        ids: &[Id],
        mark: Mark,
    ) -> Result<Vec<Message>> {
        let ids: Vec<&str> = ids.iter().map(Id::as_str).collect();
        let sql = format!(
            "UPDATE messages SET {} WHERE env_id = ? AND subscriber_id = ?
               AND id IN (SELECT value FROM json_each(?)) RETURNING *",
            mark_assignments(mark, time::now_ms())
        );
        let mut messages = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(env.as_str())
            .bind(subscriber_id)
            .bind(to_json(&ids)?)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(message_from)
            .collect::<Result<Vec<_>>>()?;
        messages.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(messages)
    }

    async fn mark_inbox(
        &self,
        env: &Id,
        subscriber_id: &str,
        filter: &MessageFilter,
        mark: Mark,
    ) -> Result<u64> {
        let mut qb = QueryBuilder::<Sqlite>::new("UPDATE messages SET ");
        qb.push(mark_assignments(mark, time::now_ms()))
            .push(" WHERE env_id = ");
        qb.push_bind(env.as_str())
            .push(" AND subscriber_id = ")
            .push_bind(subscriber_id.to_owned());
        push_filter(&mut qb, filter)?;
        Ok(qb.build().execute(&self.pool).await?.rows_affected())
    }

    async fn messages(&self, env: &Id, query: &MessageQuery) -> Result<(Vec<Message>, bool)> {
        let mut qb = QueryBuilder::<Sqlite>::new("SELECT * FROM messages WHERE env_id = ");
        qb.push_bind(env.as_str());
        if let Some(subscriber_id) = &query.subscriber_id {
            qb.push(" AND subscriber_id = ")
                .push_bind(subscriber_id.clone());
        }
        if !query.transaction_ids.is_empty() {
            qb.push(" AND transaction_id IN (SELECT value FROM json_each(")
                .push_bind(to_json(&query.transaction_ids)?)
                .push("))");
        }
        if let Some(channel) = query.channel {
            qb.push(" AND channel = ").push_bind(channel.as_str());
        }
        qb.push(" ORDER BY id DESC LIMIT ")
            .push_bind(i64::from(query.limit) + 1)
            .push(" OFFSET ")
            .push_bind(i64::from(query.page) * i64::from(query.limit));
        let rows = qb.build().fetch_all(&self.pool).await?;
        let has_more = rows.len() > query.limit as usize;
        let messages = rows
            .iter()
            .take(query.limit as usize)
            .map(message_from)
            .collect::<Result<_>>()?;
        Ok((messages, has_more))
    }

    async fn delete_transaction_messages(
        &self,
        env: &Id,
        transaction_id: &str,
        channel: Option<Channel>,
    ) -> Result<Vec<String>> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "DELETE FROM messages WHERE env_id = ? AND transaction_id = ? AND (?3 IS NULL OR channel = ?3)
             RETURNING subscriber_id",
        )
        .bind(env.as_str())
        .bind(transaction_id)
        .bind(channel.map(Channel::as_str))
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query(concat!(
            "DELETE FROM runs WHERE env_id = ?1 AND transaction_id = ?2 AND NOT EXISTS
               (SELECT 1 FROM messages m WHERE m.env_id = ?1 AND m.transaction_id = ?2) AND ",
            released_run!()
        ))
        .bind(env.as_str())
        .bind(transaction_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    async fn run_messages(&self, env: &Id, run_id: &Id) -> Result<Vec<Message>> {
        sqlx::query("SELECT * FROM messages WHERE env_id = ? AND run_id = ? ORDER BY id")
            .bind(env.as_str())
            .bind(run_id.as_str())
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(message_from)
            .collect()
    }

    async fn create_runs(
        &self,
        env: &Id,
        transaction_id: &str,
        idempotency_key: Option<&str>,
        runs: &[Run],
    ) -> Result<Option<String>> {
        let now = time::now_ms();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let seen: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM messages WHERE env_id = ?1 AND transaction_id = ?2)
               OR EXISTS (SELECT 1 FROM runs WHERE env_id = ?1 AND transaction_id = ?2 AND status != 'cancelled')",
        )
        .bind(env.as_str())
        .bind(transaction_id)
        .fetch_one(&mut *tx)
        .await?;
        if seen {
            return Ok(Some(transaction_id.to_owned()));
        }
        if let Some(key) = idempotency_key {
            let earlier: Option<String> = sqlx::query_scalar(
                "SELECT transaction_id FROM idempotency_keys WHERE env_id = ? AND key = ? AND created_at > ?",
            )
            .bind(env.as_str())
            .bind(key)
            .bind(now - IDEMPOTENCY_WINDOW)
            .fetch_optional(&mut *tx)
            .await?;
            if earlier.is_some() {
                return Ok(earlier);
            }
            sqlx::query("INSERT OR REPLACE INTO idempotency_keys (env_id, key, transaction_id, created_at) VALUES (?, ?, ?, ?)")
                .bind(env.as_str())
                .bind(key)
                .bind(transaction_id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }
        for run in runs {
            sqlx::query(
                "INSERT INTO runs (env_id, id, workflow_id, subscriber_id, transaction_id, bridge_url, payload, actor,
                   step_index, state, status, run_at, attempts, digest_key, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(env.as_str())
            .bind(run.id.as_str())
            .bind(&run.workflow_id)
            .bind(&run.subscriber_id)
            .bind(&run.transaction_id)
            .bind(&run.bridge_url)
            .bind(to_json(&*run.payload)?)
            .bind(run.actor.as_deref())
            .bind(i64::from(run.step_index))
            .bind(to_json(&run.state)?)
            .bind(run.status.as_str())
            .bind(run.run_at)
            .bind(i64::from(run.attempts))
            .bind(run.digest_key.as_deref())
            .bind(run.created_at)
            .bind(run.updated_at)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(None)
    }

    async fn claim_due_runs(&self, now: i64, limit: u32) -> Result<Vec<Run>> {
        let mut runs = sqlx::query(
            "UPDATE runs SET status = 'running', updated_at = ?1 WHERE rowid IN
               (SELECT rowid FROM runs WHERE status = 'pending' AND run_at <= ?1 ORDER BY run_at, id LIMIT ?2)
             RETURNING *",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?
        .iter()
        .map(run_from)
        .collect::<Result<Vec<_>>>()?;
        runs.sort_by(|a, b| (a.run_at, &a.id).cmp(&(b.run_at, &b.id)));
        Ok(runs)
    }

    async fn next_run_at(&self) -> Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT MIN(run_at) FROM runs WHERE status = 'pending'")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn requeue_running_runs(&self) -> Result<u64> {
        Ok(
            sqlx::query("UPDATE runs SET status = 'pending' WHERE status = 'running'")
                .execute(&self.pool)
                .await?
                .rows_affected(),
        )
    }

    async fn save_run(&self, run: &Run) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        save_run_in(&mut conn, run).await
    }

    async fn join_digest(&self, run: &Run, key: &str, window_end: i64) -> Result<DigestJoin> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM runs WHERE env_id = ? AND id = ?")
                .bind(run.env_id.as_str())
                .bind(run.id.as_str())
                .fetch_optional(&mut *tx)
                .await?;
        if status.as_deref() == Some(RunStatus::Cancelled.as_str()) {
            return Ok(DigestJoin::Cancelled);
        }
        let owner: Option<String> = sqlx::query_scalar(
            "SELECT id FROM runs WHERE env_id = ? AND workflow_id = ? AND subscriber_id = ? AND step_index = ?
               AND digest_key = ? AND status = 'pending' AND id != ? ORDER BY id LIMIT 1",
        )
        .bind(run.env_id.as_str())
        .bind(&run.workflow_id)
        .bind(&run.subscriber_id)
        .bind(i64::from(run.step_index))
        .bind(key)
        .bind(run.id.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let mut run = run.clone();
        let joined = match owner.as_deref().and_then(Id::parse) {
            Some(owner) => {
                run.status = RunStatus::Merged;
                run.digest_key = None;
                insert_digest_event(&mut tx, &run, &owner).await?;
                DigestJoin::Merged { owner }
            }
            None => {
                run.status = RunStatus::Pending;
                run.run_at = window_end;
                run.digest_key = Some(key.to_owned());
                insert_digest_event(&mut tx, &run, &run.id).await?;
                DigestJoin::Owner
            }
        };
        save_run_in(&mut tx, &run).await?;
        tx.commit().await?;
        Ok(joined)
    }

    async fn digest_events(
        &self,
        env: &Id,
        owner: &Id,
        limit: u32,
        max_bytes: u32,
    ) -> Result<(Vec<DigestEvent>, u64)> {
        let mut tx = self.pool.begin().await?;
        let events = sqlx::query(
            "SELECT * FROM (SELECT *, SUM(length(payload)) OVER (ORDER BY id) AS bytes
               FROM digest_events WHERE env_id = ? AND owner_run_id = ?)
             WHERE bytes <= ? ORDER BY id LIMIT ?",
        )
        .bind(env.as_str())
        .bind(owner.as_str())
        .bind(max_bytes)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?
        .iter()
        .map(digest_event_from)
        .collect::<Result<_>>()?;
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM digest_events WHERE env_id = ? AND owner_run_id = ?",
        )
        .bind(env.as_str())
        .bind(owner.as_str())
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok((events, total.unsigned_abs()))
    }

    async fn cancel_transaction(&self, env: &Id, transaction_id: &str) -> Result<u64> {
        let now = time::now_ms();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let merged: Vec<String> = sqlx::query_scalar(
            "UPDATE runs SET status = 'cancelled', updated_at = ?1
             WHERE env_id = ?2 AND transaction_id = ?3 AND status = 'merged' AND id IN
               (SELECT e.run_id FROM digest_events e JOIN runs o ON o.env_id = e.env_id AND o.id = e.owner_run_id
                WHERE e.env_id = ?2 AND o.status = 'pending')
             RETURNING id",
        )
        .bind(now)
        .bind(env.as_str())
        .bind(transaction_id)
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM digest_events WHERE env_id = ? AND run_id IN (SELECT value FROM json_each(?))")
            .bind(env.as_str())
            .bind(to_json(&merged)?)
            .execute(&mut *tx)
            .await?;

        let pending = sqlx::query(
            "SELECT * FROM runs WHERE env_id = ? AND transaction_id = ? AND status IN ('pending', 'running')",
        )
        .bind(env.as_str())
        .bind(transaction_id)
        .fetch_all(&mut *tx)
        .await?
        .iter()
        .map(run_from)
        .collect::<Result<Vec<_>>>()?;
        let cancelled = merged.len() + pending.len();
        for mut run in pending {
            if run.digest_key.is_some() {
                hand_over_digest(&mut tx, env, &run, now).await?;
            }
            run.status = RunStatus::Cancelled;
            run.digest_key = None;
            save_run_in(&mut tx, &run).await?;
        }
        tx.commit().await?;
        Ok(cancelled as u64)
    }

    async fn purge_expired(&self, now: i64) -> Result<()> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM messages WHERE created_at < ?")
            .bind(now - MESSAGE_RETENTION)
            .execute(&mut *tx)
            .await?;
        sqlx::query(concat!(
            "DELETE FROM runs WHERE updated_at < ? AND ",
            released_run!()
        ))
        .bind(now - RUN_RETENTION)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "DELETE FROM digest_events WHERE NOT EXISTS
               (SELECT 1 FROM runs r WHERE r.env_id = digest_events.env_id AND r.id = digest_events.owner_run_id)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM idempotency_keys WHERE created_at < ?")
            .bind(now - IDEMPOTENCY_WINDOW)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::domain::{StepKind, WorkflowPreferences, WorkflowStep};

    async fn store() -> (SqliteStore, Id) {
        (SqliteStore::open_in_memory().await.unwrap(), Id::new())
    }

    fn message(subscriber_id: &str, tx: &str, tags: &[&str], data: Value) -> Message {
        let now = time::now_ms();
        Message {
            id: Id::new(),
            run_id: None,
            transaction_id: tx.to_owned(),
            subscriber_id: subscriber_id.to_owned(),
            workflow_id: "wf".to_owned(),
            step_id: "in-app".to_owned(),
            channel: Channel::InApp,
            subject: None,
            body: "hi".to_owned(),
            avatar: None,
            redirect: None,
            primary_action: None,
            secondary_action: None,
            data: data.as_object().cloned(),
            tags: tags.iter().map(|t| (*t).to_owned()).collect(),
            severity: Severity::None,
            seen_at: None,
            read_at: None,
            archived_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn run(env: &Id, subscriber_id: &str, tx: &str) -> Run {
        let now = time::now_ms();
        Run {
            id: Id::new(),
            env_id: env.clone(),
            workflow_id: "wf".to_owned(),
            subscriber_id: subscriber_id.to_owned(),
            transaction_id: tx.to_owned(),
            bridge_url: "http://localhost/api/novu".to_owned(),
            payload: Arc::new(json!({ "tx": tx })),
            actor: None,
            step_index: 1,
            state: json!({}),
            status: RunStatus::Running,
            run_at: now,
            attempts: 0,
            digest_key: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn status(store: &SqliteStore, env: &Id, id: &Id) -> (String, i64, Option<String>) {
        let row =
            sqlx::query("SELECT status, run_at, digest_key FROM runs WHERE env_id = ? AND id = ?")
                .bind(env.as_str())
                .bind(id.as_str())
                .fetch_one(&store.pool)
                .await
                .unwrap();
        (row.get("status"), row.get("run_at"), row.get("digest_key"))
    }

    #[tokio::test]
    async fn file_is_private_and_reopens() {
        let dir = crate::config::tests::TempDir::new();
        let path = dir.0.join("nanovu.db");
        let env = Id::new();
        let store = SqliteStore::open(&path).await.unwrap();
        store
            .upsert_subscriber(&env, "a", &SubscriberPatch::default())
            .await
            .unwrap();
        drop(store);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let store = SqliteStore::open(&path).await.unwrap();
        assert!(store.subscriber(&env, "a").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn subscriber_upsert_patch_and_delete_cascade() {
        let (store, env) = store().await;
        let patch = SubscriberPatch {
            email: Some("a@x".into()),
            ..Default::default()
        };
        let created = store.upsert_subscriber(&env, "a", &patch).await.unwrap();
        let patch = SubscriberPatch {
            first_name: Some("Ann".into()),
            ..Default::default()
        };
        let updated = store.upsert_subscriber(&env, "a", &patch).await.unwrap();
        assert_eq!(updated.id, created.id);
        assert_eq!(
            (updated.email.as_deref(), updated.first_name.as_deref()),
            (Some("a@x"), Some("Ann"))
        );
        let update = SubscriberUpdate {
            email: Some(None),
            last_name: Some(Some("Lee".into())),
            ..Default::default()
        };
        let updated = store
            .update_subscriber(&env, "a", &update)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                updated.email.as_deref(),
                updated.first_name.as_deref(),
                updated.last_name.as_deref()
            ),
            (None, Some("Ann"), Some("Lee"))
        );
        assert!(
            store
                .update_subscriber(&env, "zz", &update)
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.subscriber(&Id::new(), "a").await.unwrap().is_none());

        assert!(
            store
                .set_device_tokens(&env, "a", "push", &["t1".into(), "t2".into()])
                .await
                .unwrap()
        );
        assert!(
            !store
                .set_device_tokens(&env, "zz", "push", &["t1".into()])
                .await
                .unwrap()
        );
        store
            .subscribe_topic(
                &env,
                "team",
                &[TopicMember {
                    subscriber_id: "a".into(),
                    identifier: "i".into(),
                }],
            )
            .await
            .unwrap();
        store
            .insert_message(&env, &message("a", "tx", &[], json!({})))
            .await
            .unwrap();
        store
            .create_runs(&env, "tx", None, &[run(&env, "a", "tx")])
            .await
            .unwrap();

        assert!(store.delete_subscriber(&env, "a").await.unwrap());
        assert!(!store.delete_subscriber(&env, "a").await.unwrap());
        assert!(
            store
                .device_tokens(&env, "a", "push")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .topic_subscriber_ids(&env, "team", 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .messages(
                    &env,
                    &MessageQuery {
                        limit: 10,
                        ..Default::default()
                    }
                )
                .await
                .unwrap()
                .0
                .is_empty()
        );
        assert_eq!(store.next_run_at().await.unwrap(), None);
    }

    #[tokio::test]
    async fn device_tokens_replace_and_remove() {
        let (store, env) = store().await;
        store
            .upsert_subscriber(&env, "a", &SubscriberPatch::default())
            .await
            .unwrap();
        store
            .set_device_tokens(&env, "a", "push", &["t1".into(), "t2".into()])
            .await
            .unwrap();
        store
            .set_device_tokens(&env, "a", "push", &["t2".into(), "t3".into(), "t3".into()])
            .await
            .unwrap();
        let mut tokens = store.device_tokens(&env, "a", "push").await.unwrap();
        tokens.sort();
        assert_eq!(tokens, ["t2", "t3"]);
        store
            .remove_device_tokens(&env, "push", &["t2".into()])
            .await
            .unwrap();
        assert_eq!(
            store.device_tokens(&env, "a", "push").await.unwrap(),
            ["t3"]
        );
    }

    #[tokio::test]
    async fn topics_subscribe_and_unsubscribe() {
        let (store, env) = store().await;
        store
            .upsert_subscriber(&env, "a", &SubscriberPatch::default())
            .await
            .unwrap();
        let members = [
            TopicMember {
                subscriber_id: "a".into(),
                identifier: "ia".into(),
            },
            TopicMember {
                subscriber_id: "ghost".into(),
                identifier: "ig".into(),
            },
        ];
        let out = store.subscribe_topic(&env, "team", &members).await.unwrap();
        assert_eq!(out.subscriptions.len(), 1);
        assert_eq!(out.missing, ["ghost"]);
        assert_eq!(out.topic.name, None);
        let renamed = store
            .upsert_topic(&env, "team", Some("Team"))
            .await
            .unwrap();
        assert_eq!(
            (renamed.id, renamed.name.as_deref()),
            (out.topic.id, Some("Team"))
        );
        assert_eq!(
            store
                .upsert_topic(&env, "team", None)
                .await
                .unwrap()
                .name
                .as_deref(),
            Some("Team")
        );
        assert_eq!(
            store.topic_subscriber_ids(&env, "team", 10).await.unwrap(),
            ["a"]
        );

        assert!(
            store
                .unsubscribe_topic(&env, "nope", &["a".into()])
                .await
                .unwrap()
                .is_none()
        );
        let out = store
            .unsubscribe_topic(&env, "team", &["a".into(), "ghost".into()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (out.subscriptions.len(), out.missing.as_slice()),
            (1, ["ghost".to_owned()].as_slice())
        );
        assert!(
            store
                .topic_subscriber_ids(&env, "team", 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn workflows_replace_keeps_ids() {
        let (store, env) = store().await;
        let wf = |id: &str| Workflow {
            id: Id::new(),
            workflow_id: id.to_owned(),
            name: id.to_owned(),
            description: None,
            tags: vec!["t".into()],
            severity: Severity::High,
            preferences: WorkflowPreferences::default(),
            payload_schema: Some(json!({ "type": "object" })),
            steps: vec![WorkflowStep {
                step_id: "s".into(),
                kind: StepKind::InApp,
            }],
            bridge_url: "http://localhost/api/novu".into(),
            created_at: 0,
            updated_at: 0,
        };
        let first = store
            .replace_workflows(&env, &[wf("a"), wf("b")])
            .await
            .unwrap();
        let second = store.replace_workflows(&env, &[wf("a")]).await.unwrap();
        assert_eq!(second[0].id, first[0].id);
        assert_eq!(second[0].created_at, first[0].created_at);
        assert_eq!(store.workflows(&env).await.unwrap().len(), 1);
        assert_eq!(
            store
                .workflow(&env, first[0].id.as_str())
                .await
                .unwrap()
                .unwrap()
                .workflow_id,
            "a"
        );
        assert_eq!(
            store.workflow(&env, "a").await.unwrap().unwrap().severity,
            Severity::High
        );
        assert!(store.workflow(&env, "b").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn preferences_merge() {
        let (store, env) = store().await;
        let off = ChannelPrefs {
            email: Some(false),
            ..Default::default()
        };
        let on = ChannelPrefs {
            push: Some(true),
            ..Default::default()
        };
        store
            .update_preferences(&env, "a", None, &off)
            .await
            .unwrap();
        let merged = store
            .update_preferences(&env, "a", None, &on)
            .await
            .unwrap();
        assert_eq!((merged.email, merged.push), (Some(false), Some(true)));
        store
            .update_preferences(&env, "a", Some("wf"), &on)
            .await
            .unwrap();
        let prefs = store.subscriber_preferences(&env, "a").await.unwrap();
        assert_eq!(prefs.global, merged);
        assert_eq!(prefs.workflows["wf"], on);

        let both = ChannelPrefs {
            email: Some(true),
            push: Some(true),
            ..Default::default()
        };
        store
            .update_preferences(&env, "a", Some("wf"), &both)
            .await
            .unwrap();
        store
            .update_preferences(&env, "b", Some("wf"), &on)
            .await
            .unwrap();
        let push_off = ChannelPrefs {
            push: Some(false),
            ..Default::default()
        };
        store
            .update_preferences(&env, "a", None, &push_off)
            .await
            .unwrap();
        let prefs = store.subscriber_preferences(&env, "a").await.unwrap();
        assert_eq!(prefs.global.push, Some(false));
        assert_eq!(
            (prefs.workflows["wf"].email, prefs.workflows["wf"].push),
            (Some(true), None)
        );
        let other = store.subscriber_preferences(&env, "b").await.unwrap();
        assert_eq!(other.workflows["wf"], on);
    }

    #[tokio::test]
    async fn inbox_filters_paging_and_marks() {
        let (store, env) = store().await;
        let mut ids = Vec::new();
        for (tags, data) in [
            (
                vec!["a"],
                json!({ "kind": "x", "n": 1, "meta": { "team": "t1" }, "labels": ["p", "q"], "gone": null, "flag": true }),
            ),
            (vec!["b"], json!({ "kind": "y" })),
            (
                vec!["a", "b"],
                json!({ "kind": "x", "n": 2, "meta": { "team": "t2" } }),
            ),
        ] {
            let m = message("s", "tx", &tags, data);
            ids.push(m.id.clone());
            store.insert_message(&env, &m).await.unwrap();
        }
        let mut email = message("s", "tx", &[], json!({}));
        email.channel = Channel::Email;
        store.insert_message(&env, &email).await.unwrap();
        store
            .insert_message(&env, &message("other", "tx", &["a"], json!({})))
            .await
            .unwrap();

        let page = InboxPage {
            limit: 2,
            offset: 0,
            after: None,
        };
        let all = MessageFilter::default();
        let (first, more) = store.inbox(&env, "s", &all, &page).await.unwrap();
        assert_eq!(
            (first.iter().map(|m| &m.id).collect::<Vec<_>>(), more),
            (vec![&ids[2], &ids[1]], true)
        );
        let next = InboxPage {
            after: Some(ids[1].clone()),
            ..page
        };
        let (rest, more) = store.inbox(&env, "s", &all, &next).await.unwrap();
        assert_eq!((rest.len(), &rest[0].id, more), (1, &ids[0], false));

        let tags = |groups: &[&[&str]]| MessageFilter {
            tags: groups
                .iter()
                .map(|g| g.iter().map(|t| (*t).to_owned()).collect())
                .collect(),
            ..Default::default()
        };
        let tag_a = tags(&[&["a"]]);
        let data = |clauses: &[(&[&str], Value)]| MessageFilter {
            data: clauses
                .iter()
                .map(|(path, values)| {
                    (
                        path.iter().map(|k| (*k).to_owned()).collect(),
                        values.as_array().unwrap().clone(),
                    )
                })
                .collect(),
            ..Default::default()
        };
        let snoozed = MessageFilter {
            snoozed: Some(true),
            ..Default::default()
        };
        let high = MessageFilter {
            severity: vec![Severity::High],
            ..Default::default()
        };
        let filters = [
            all.clone(),
            tag_a.clone(),
            tags(&[&["a", "b"]]),
            tags(&[&["a"], &["b", "z"]]),
            data(&[(&["kind"], json!(["x"]))]),
            data(&[(&["kind"], json!(["y", "z"]))]),
            data(&[(&["kind"], json!(["x", "y"])), (&["n"], json!([2.0]))]),
            data(&[(&["kind"], json!(["x", "y"])), (&["kind"], json!(["y"]))]),
            data(&[(&["bad\"key"], json!([1]))]),
            data(&[(&["meta", "team"], json!(["t1", "t3"]))]),
            data(&[(&["meta"], json!(["t1"]))]),
            data(&[(&["labels"], json!(["q"]))]),
            data(&[(&["gone"], json!([null]))]),
            data(&[(&["flag"], json!([true]))]),
            data(&[(&["flag"], json!([1]))]),
            snoozed,
            high,
        ];
        assert_eq!(
            store.inbox_counts(&env, "s", &filters).await.unwrap(),
            [3, 2, 3, 1, 2, 1, 1, 1, 0, 1, 0, 1, 1, 1, 0, 0, 0]
        );

        let read = store
            .mark_messages(&env, "s", &[ids[0].clone(), Id::new()], Mark::Read)
            .await
            .unwrap();
        assert_eq!(read.len(), 1);
        assert!(read[0].is_seen() && read[0].is_read() && !read[0].is_archived());
        let unread = store
            .mark_messages(&env, "s", &[ids[0].clone()], Mark::Unread)
            .await
            .unwrap();
        assert!(unread[0].is_seen() && !unread[0].is_read());
        let unseen = MessageFilter {
            seen: Some(false),
            ..Default::default()
        };
        assert_eq!(
            store
                .mark_inbox(&env, "s", &tag_a, Mark::Archive)
                .await
                .unwrap(),
            2
        );
        assert_eq!(store.inbox_counts(&env, "s", &[unseen]).await.unwrap(), [1]);
        let archived = store.message(&env, "s", &ids[2]).await.unwrap().unwrap();
        assert!(archived.is_read() && archived.is_archived());
        let unarchived = store
            .mark_messages(&env, "s", &[ids[2].clone()], Mark::Unarchive)
            .await
            .unwrap();
        assert!(unarchived[0].is_read() && !unarchived[0].is_archived());
        let cleared = store
            .mark_messages(&env, "s", &[ids[2].clone()], Mark::Unseen)
            .await
            .unwrap();
        assert!(!cleared[0].is_seen() && cleared[0].is_read());

        let query = MessageQuery {
            transaction_ids: vec!["tx".into()],
            limit: 10,
            ..Default::default()
        };
        assert_eq!(store.messages(&env, &query).await.unwrap().0.len(), 5);
        let affected = store
            .delete_transaction_messages(&env, "tx", None)
            .await
            .unwrap();
        assert_eq!(affected, ["other", "s"]);
    }

    #[tokio::test]
    async fn idempotency_and_claim() {
        let (store, env) = store().await;
        let mut first = run(&env, "a", "tx1");
        first.status = RunStatus::Pending;
        assert_eq!(
            store
                .create_runs(&env, "tx1", Some("k"), &[first.clone()])
                .await
                .unwrap(),
            None
        );
        let again = run(&env, "a", "tx2");
        assert_eq!(
            store
                .create_runs(&env, "tx2", Some("k"), &[again])
                .await
                .unwrap()
                .as_deref(),
            Some("tx1")
        );

        assert_eq!(store.next_run_at().await.unwrap(), Some(first.run_at));
        assert!(
            store
                .claim_due_runs(first.run_at - 1, 10)
                .await
                .unwrap()
                .is_empty()
        );
        let claimed = store.claim_due_runs(first.run_at, 10).await.unwrap();
        assert_eq!((claimed.len(), claimed[0].status), (1, RunStatus::Running));
        assert!(
            store
                .claim_due_runs(first.run_at, 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.requeue_running_runs().await.unwrap(), 1);

        store.purge_expired(time::now_ms() + 2 * DAY).await.unwrap();
        assert_eq!(
            store
                .create_runs(&env, "tx3", Some("k"), &[])
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn digest_join_cancel_and_handoff() {
        let (store, env) = store().await;
        let (a, b, c) = (
            run(&env, "s", "t1"),
            run(&env, "s", "t2"),
            run(&env, "s", "t3"),
        );
        store
            .create_runs(&env, "t", None, &[a.clone(), b.clone(), c.clone()])
            .await
            .unwrap();

        assert_eq!(
            store.join_digest(&a, "k", 1_000).await.unwrap(),
            DigestJoin::Owner
        );
        let merged = DigestJoin::Merged {
            owner: a.id.clone(),
        };
        assert_eq!(store.join_digest(&b, "k", 2_000).await.unwrap(), merged);
        assert_eq!(store.join_digest(&c, "k", 3_000).await.unwrap(), merged);
        let (events, total) = store
            .digest_events(&env, &a.id, 10, u32::MAX)
            .await
            .unwrap();
        assert_eq!(
            events.iter().map(|e| &e.run_id).collect::<Vec<_>>(),
            [&a.id, &b.id, &c.id]
        );
        assert_eq!(total, 3);
        let (events, total) = store.digest_events(&env, &a.id, 2, u32::MAX).await.unwrap();
        assert_eq!(
            (events.iter().map(|e| &e.run_id).collect::<Vec<_>>(), total),
            (vec![&a.id, &b.id], 3)
        );
        assert_eq!(events[1].payload, json!({ "tx": "t2" }));
        let first = store
            .digest_events(&env, &a.id, 1, u32::MAX)
            .await
            .unwrap()
            .0;
        let one = u32::try_from(to_json(&first[0].payload).unwrap().len()).unwrap();
        let (events, total) = store
            .digest_events(&env, &a.id, 10, one * 2 - 1)
            .await
            .unwrap();
        assert_eq!((events.len(), total), (1, 3));

        assert_eq!(store.cancel_transaction(&env, "t2").await.unwrap(), 1);
        assert_eq!(store.cancel_transaction(&env, "t1").await.unwrap(), 1);
        assert_eq!(status(&store, &env, &a.id).await.0, "cancelled");
        assert_eq!(
            status(&store, &env, &c.id).await,
            ("pending".into(), 1_000, Some("k".into()))
        );
        let (events, _) = store
            .digest_events(&env, &c.id, 10, u32::MAX)
            .await
            .unwrap();
        assert_eq!(
            events.iter().map(|e| &e.run_id).collect::<Vec<_>>(),
            [&c.id]
        );

        let d = run(&env, "s", "t4");
        store
            .create_runs(&env, "t4", None, std::slice::from_ref(&d))
            .await
            .unwrap();
        assert_eq!(
            store.join_digest(&d, "k", 9_000).await.unwrap(),
            DigestJoin::Merged {
                owner: c.id.clone()
            }
        );
        assert_eq!(
            store.join_digest(&d, "other", 9_000).await.unwrap(),
            DigestJoin::Owner
        );

        assert_eq!(store.cancel_transaction(&env, "t3").await.unwrap(), 1);
        assert_eq!(store.cancel_transaction(&env, "nope").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cancelled_running_run_stays_cancelled() {
        let (store, env) = store().await;
        let a = run(&env, "s", "t1");
        store
            .create_runs(&env, "t1", None, std::slice::from_ref(&a))
            .await
            .unwrap();
        let mut claimed = a.clone();
        assert_eq!(store.cancel_transaction(&env, "t1").await.unwrap(), 1);
        claimed.status = RunStatus::Pending;
        assert!(!store.save_run(&claimed).await.unwrap());
        assert_eq!(
            store.join_digest(&claimed, "k", 0).await.unwrap(),
            DigestJoin::Cancelled
        );
        assert_eq!(status(&store, &env, &a.id).await.0, "cancelled");
        assert_eq!(
            store
                .digest_events(&env, &a.id, 10, u32::MAX)
                .await
                .unwrap()
                .1,
            0
        );
    }

    #[tokio::test]
    async fn purge_keeps_runs_with_live_events() {
        let (store, env) = store().await;
        let (a, b) = (run(&env, "s", "t1"), run(&env, "s", "t2"));
        store
            .create_runs(&env, "t", None, &[a.clone(), b.clone()])
            .await
            .unwrap();
        store.join_digest(&a, "k", 1_000).await.unwrap();
        store.join_digest(&b, "k", 1_000).await.unwrap();
        let mut old = message("s", "t1", &[], json!({}));
        old.created_at = 0;
        store.insert_message(&env, &old).await.unwrap();

        store.purge_expired(time::now_ms() + 8 * DAY).await.unwrap();
        assert_eq!(status(&store, &env, &b.id).await.0, "merged");
        assert_eq!(
            store
                .digest_events(&env, &a.id, 10, u32::MAX)
                .await
                .unwrap()
                .1,
            2
        );
        assert!(store.message(&env, "s", &old.id).await.unwrap().is_none());

        let mut done = a.clone();
        done.status = RunStatus::Done;
        assert!(store.save_run(&done).await.unwrap());
        store.purge_expired(time::now_ms() + 8 * DAY).await.unwrap();
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(left, 0);
        assert_eq!(
            store
                .digest_events(&env, &a.id, 10, u32::MAX)
                .await
                .unwrap()
                .1,
            0
        );
    }

    #[tokio::test]
    async fn a_transaction_id_is_used_once_until_cancelled_or_deleted() {
        let (store, env) = store().await;
        let count = async |store: &SqliteStore| -> i64 {
            sqlx::query_scalar("SELECT COUNT(*) FROM runs")
                .fetch_one(&store.pool)
                .await
                .unwrap()
        };
        let mut first = run(&env, "s", "tx");
        first.status = RunStatus::Pending;
        let create = async |r: &Run| {
            store
                .create_runs(&env, "tx", None, std::slice::from_ref(r))
                .await
                .unwrap()
        };
        assert_eq!(create(&first).await, None);
        assert_eq!(create(&run(&env, "s", "tx")).await.as_deref(), Some("tx"));
        assert_eq!(count(&store).await, 1);

        assert_eq!(store.cancel_transaction(&env, "tx").await.unwrap(), 1);
        let second = run(&env, "s", "tx");
        assert_eq!(create(&second).await, None);
        let mut done = second.clone();
        done.status = RunStatus::Done;
        assert!(store.save_run(&done).await.unwrap());
        assert_eq!(create(&run(&env, "s", "tx")).await.as_deref(), Some("tx"));

        store
            .delete_transaction_messages(&env, "tx", None)
            .await
            .unwrap();
        assert_eq!(count(&store).await, 0);
        assert_eq!(create(&run(&env, "s", "tx")).await, None);

        store
            .insert_message(&env, &message("s", "tx2", &[], json!({})))
            .await
            .unwrap();
        let other = store
            .create_runs(&env, "tx2", None, &[run(&env, "s", "tx2")])
            .await
            .unwrap();
        assert_eq!(other.as_deref(), Some("tx2"));

        let mut done = run(&env, "s", "tx3");
        let created = store
            .create_runs(&env, "tx3", None, std::slice::from_ref(&done))
            .await
            .unwrap();
        assert_eq!(created, None);
        done.status = RunStatus::Done;
        assert!(store.save_run(&done).await.unwrap());
        store
            .insert_message(&env, &message("s", "tx3", &[], json!({})))
            .await
            .unwrap();
        let push = store
            .delete_transaction_messages(&env, "tx3", Some(Channel::Push))
            .await
            .unwrap();
        assert!(push.is_empty());
        assert_eq!(count(&store).await, 2);
        let in_app = store
            .delete_transaction_messages(&env, "tx3", Some(Channel::InApp))
            .await
            .unwrap();
        assert_eq!(in_app, ["s"]);
        assert_eq!(count(&store).await, 1);
    }
}
