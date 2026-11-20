CREATE TABLE subscribers (
    env_id TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    id TEXT NOT NULL,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    phone TEXT,
    avatar TEXT,
    locale TEXT,
    timezone TEXT,
    data TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, subscriber_id)
) STRICT;

CREATE TABLE device_tokens (
    env_id TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    integration TEXT NOT NULL,
    token TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, subscriber_id, integration, token)
) STRICT, WITHOUT ROWID;

CREATE INDEX device_tokens_by_token ON device_tokens (env_id, integration, token);

CREATE TABLE topics (
    env_id TEXT NOT NULL,
    key TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, key)
) STRICT, WITHOUT ROWID;

CREATE TABLE topic_subscribers (
    env_id TEXT NOT NULL,
    topic_key TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    id TEXT NOT NULL,
    identifier TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, topic_key, subscriber_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX topic_subscribers_by_subscriber ON topic_subscribers (env_id, subscriber_id);

CREATE TABLE workflows (
    env_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    tags TEXT NOT NULL,
    severity TEXT NOT NULL,
    preferences TEXT NOT NULL,
    payload_schema TEXT,
    steps TEXT NOT NULL,
    bridge_url TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, workflow_id)
) STRICT;

-- workflow_id '' holds the subscriber's global settings.
CREATE TABLE preferences (
    env_id TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    channels TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, subscriber_id, workflow_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE messages (
    env_id TEXT NOT NULL,
    id TEXT NOT NULL,
    run_id TEXT,
    transaction_id TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    subject TEXT,
    body TEXT NOT NULL,
    avatar TEXT,
    redirect TEXT,
    primary_action TEXT,
    secondary_action TEXT,
    data TEXT,
    tags TEXT NOT NULL,
    severity TEXT NOT NULL,
    seen_at INTEGER,
    read_at INTEGER,
    archived_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, id)
) STRICT;

CREATE INDEX messages_inbox ON messages (env_id, subscriber_id, channel, id);
CREATE INDEX messages_by_transaction ON messages (env_id, transaction_id);
CREATE INDEX messages_by_run ON messages (env_id, run_id);
CREATE INDEX messages_by_age ON messages (created_at);

CREATE TABLE runs (
    env_id TEXT NOT NULL,
    id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    bridge_url TEXT NOT NULL,
    payload TEXT NOT NULL,
    actor TEXT,
    step_index INTEGER NOT NULL,
    state TEXT NOT NULL,
    status TEXT NOT NULL,
    run_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL,
    digest_key TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, id)
) STRICT;

CREATE INDEX runs_due ON runs (status, run_at);
CREATE INDEX runs_by_transaction ON runs (env_id, transaction_id);
CREATE INDEX runs_by_subscriber ON runs (env_id, subscriber_id);
CREATE INDEX runs_open_digest ON runs (env_id, workflow_id, subscriber_id, step_index, digest_key)
    WHERE status = 'pending' AND digest_key IS NOT NULL;

CREATE TABLE digest_events (
    env_id TEXT NOT NULL,
    id TEXT NOT NULL,
    owner_run_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, id)
) STRICT;

CREATE INDEX digest_events_by_owner ON digest_events (env_id, owner_run_id);
CREATE INDEX digest_events_by_run ON digest_events (env_id, run_id);

CREATE TABLE idempotency_keys (
    env_id TEXT NOT NULL,
    key TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (env_id, key)
) STRICT, WITHOUT ROWID;
