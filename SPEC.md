# nanovu

Self-hosted notification server that speaks the wire protocol of the Novu SDKs.
A client keeps `@novu/api`, `@novu/framework` and `@novu/js` / `@novu/react-native`
unchanged and only swaps the URL. Only the subset below is implemented. No web UI:
workflows are code-first and arrive through bridge sync.

## Hard constraints

- Rust, one static binary. Targets: `x86_64-linux`, `aarch64-linux` (Raspberry Pi 5), musl.
- Idle RSS at most 20 MB with one environment loaded and no connections. Checked by a script.
- SQLite now. All SQL lives behind one `Store` trait so a Postgres impl can be added later.
  Do not write the Postgres impl.
- TLS client stack: rustls + ring only. No openssl, no aws-lc.
- tokio `current_thread` runtime.
- No web UI, no dashboard, no workflow editor, no Novu features outside this document.

## Pinned SDK versions (contract)

- `@novu/api` 3.20.0 (server side, zod-validates every response)
- `@novu/framework` 2.14.0 (bridge)
- `@novu/js` 3.19.0 via `@novu/react-native` 3.19.0 (inbox, REST + WebSocket)

Sources to read when a shape is unclear (read-only, never edit):

- the pinned packages under `tests/contract/node_modules/@novu/` (after `npm ci`)
- upstream `novuhq/novu`: `apps/api`, `apps/worker`, `apps/ws`, `libs`
- real callers: `gate-k9/services/novu/**`, `gate-k9/services/*/workflows/**`, `sevastopol/src`

The SDK source is the contract. When this document and the SDK disagree, the SDK wins;
fix this document.

## Layout

Cargo workspace. Each crate has one owner during the build.

| crate | owns |
| --- | --- |
| `crates/core` (`nanovu-core`) | config, environments, domain types, ids, `Store` trait + SQLite impl + migrations, auth (api key, subscriber hash, session JWT, bridge signature), `Hub` (in-process realtime fan-out, WS connection caps), HTTP error type, preference merge (`Workflow::resolve_channels`), `AppState` |
| `crates/engine` (`nanovu-engine`) | bridge client (discover, execute), trigger intake, durable job runner, digest, delay, applying resolved preferences, delivery (in-app, push FCM, email SMTP) |
| `crates/api` (`nanovu-api`) | server-to-server REST used by `@novu/api` and `@novu/framework` |
| `crates/inbox` (`nanovu-inbox`) | subscriber REST used by `@novu/js` + WebSocket |
| `crates/nanovu` (bin `nanovu`) | CLI, router assembly, limits, shutdown, `GET /healthz`, startup requeue of in-progress runs, daily retention purge |
| `tests/contract` | Node project driving the real SDKs against the binary |
| `nix/`, `flake.nix` | package, devShell, NixOS module |

Dependency direction: `nanovu` → `api`, `inbox` → `engine` → `core`. `core` depends on no
other workspace crate. `api` and `inbox` never depend on each other.

Entry points: `nanovu_api::router(AppState, Engine)` and `nanovu_inbox::router(AppState)`
return `axum::Router`; `nanovu_engine::Engine::{new, spawn, trigger, cancel, sync}`. Handlers
return `core::HttpError`; each router renders it in its own error shape.

Design rules (these are the review criteria at the end):

- Pieces that change together live together; a typical change lands in one place.
- Wire DTOs (serde structs shaped for an SDK) live in the crate that serves them, next to the
  handler. Domain types in `core` are not shaped after any SDK.
- No special cases per workflow, per client or per environment name.
- No pass-through wrappers, no traits with one impl except `Store`, no config for things
  that do not vary, no dead code, no `unwrap`/`expect` outside tests and startup.
- Few files: one file per module until it clearly outgrows it.
- Comments only for facts the code cannot show (protocol quirks, SDK behavior). At most two
  lines each.
- Every secret is compared in constant time and never logged.

## Configuration and setup

Declarative TOML, secrets by file path only (fits systemd `LoadCredential`, sops, agenix).
Nothing secret is stored in the database, so the DB can be copied without leaking keys.

```toml
listen = "127.0.0.1:4010"
database = "/var/lib/nanovu/nanovu.db"

[environments.stg]
application_identifier = "abc123"            # what the app passes to NovuProvider
secret_key_file = "/run/credentials/nanovu.service/stg-secret-key"
bridge_hosts = ["api.nshard.com"]            # allowlist for bridge URLs (SSRF)

[environments.stg.integrations.cuentacero-stg]
provider = "fcm"
service_account_file = "/run/credentials/nanovu.service/stg-fcm.json"

[environments.stg.integrations.mail]
provider = "smtp"
url_file = "/run/credentials/nanovu.service/stg-smtp-url"   # smtps://user:pass@host:465
from = "Cuenta Cero <no-reply@example.com>"
```

- Environments are config stanzas: add or remove a stanza to attach or detach one. Many
  per instance or one instance per environment both work.
- Environment id: first 24 hex chars of `sha256(name)`. Stable, hex (a client regex needs it).
- The secret key is the existing Novu secret key, reused, so the client changes nothing but
  the URL. It authenticates `Authorization: ApiKey <key>`, signs bridge requests, and
  verifies subscriber hashes. The session JWT key is HKDF-SHA256 (RFC 5869, empty salt) of
  the secret key with info `nanovu session jwt v1`, so no subscriber hash can serve as it.
- CLI: `nanovu serve --config <path>`, `nanovu check --config <path>` (validates config,
  secret files readable and not world-readable, prints environment ids, never secrets),
  `nanovu hash --config <path> --env <name> <subscriberId>` (debug helper).
- Startup refuses: no environments, missing, empty, world-readable or non-root-group-readable
  secret files (systemd credentials are root:root 0440),
  duplicate `application_identifier`, duplicate secret keys, secret key shorter than 32 chars,
  more than one smtp integration in an environment (each would send every email), unknown
  config keys.

## Security

- Bind to localhost by default; TLS is the reverse proxy's job.
- API key lookup: constant-time compare against every environment (few), no early exit.
- Subscriber hash is required on `POST /v1/inbox/session`:
  `hex(HMAC-SHA256(secret_key, subscriberId))`, constant-time. No unsecured mode.
- Session JWT: HS256, `iss: "novu_api"`, `aud: "widget_user"`, 15 days, claims carry
  environment id and subscriber id. `iat` is required, not in the future, and `exp - iat` is
  at most 15 days. Verified on every inbox request and on WS upgrade, together with the
  subscriber still existing (401 otherwise).
- The browser holds only the subscriber hash, so the session creates a missing subscriber and
  sets `timezone`/`locale`; other profile fields it sends are ignored.
- Bridge requests are signed: header `novu-signature: t=<unix ms>,v1=<hex>` where
  `v1 = HMAC-SHA256(secret_key, "${t}.${body}")` and `body` is the exact bytes sent.
- Bridge URL must be https (http allowed only for loopback), host in `bridge_hosts`,
  redirects disabled, 5 s timeout, response size capped.
- Authentication runs before any body is read. Request bodies: inbox 8 KiB, server API
  16 KiB, trigger 32 KiB (413 above, 408 when not sent within 5 s); query strings 8 KiB (414).
- At most 8 requests are handled at once; others wait unread for a slot. `GET /healthz` is
  outside the limit.
  With 8 trigger bodies of 32 KiB each, parsed JSON stays in the low MiB for real payloads
  (pathological all-scalar JSON can reach ~54x its size, ~14 MiB). The reverse proxy must cap
  header size and connections and should buffer request bodies.
- Rate limit per client IP (IPv6 by /64): token bucket, burst 100, 50/s, in memory, at most
  4096 buckets; when full, the stalest bucket is evicted. The IP is the rightmost
  `X-Forwarded-For` entry only when the peer is loopback.
- WebSocket connections capped at 5 per subscriber, 128 per environment and 256 globally
  (`Hub`); frames capped at 4 KiB; idle WS closed when pings stop.
- Fan-out: `to` at most 100 entries, at most 2000 subscribers per trigger after topic
  expansion (400 above); the runner holds at most 4 runs at once.
- SQLite file created 0600; WAL mode; `synchronous=NORMAL`.
- Errors never echo secrets, tokens or SQL.
- systemd unit (NixOS module): `DynamicUser`, `StateDirectory`, `LoadCredential`,
  `ProtectSystem=strict`, `NoNewPrivileges`, `MemoryDenyWriteExecute`, `SystemCallFilter=@system-service`,
  `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX`, `CapabilityBoundingSet=`, `MemoryHigh=18M`, `MemoryMax=24M`.

## Server API (crate `api`)

Auth: `Authorization: ApiKey <secret key>`. Errors use
`{statusCode, timestamp, path, message}`; 400 adds `type` and `errors: []` (trigger's
400 model requires them), 422 adds `errors: {}` (every route's 422 model requires it). `@novu/api` unwraps single-key `{data}` bodies and
zod-validates the result, so every required field of its response model must be present.
It retries 408/409/429/5xx and connection errors for up to an hour: never answer 5xx for a
permanent failure, and honor the `idempotency-key` header on `POST /v1/events/trigger`
(same key within 24 h returns the same response, built from the stored transaction id,
without re-triggering).

| route | notes |
| --- | --- |
| `POST /v1/bridge/sync` | body `{bridgeUrl}`. GET `<bridgeUrl>?action=discover` (signed), replace this environment's workflows with the result, delete the ones missing. Response `{data:[...workflows]}` (caller reads `.data.length`). No workflow count limit. |
| `POST /v1/events/trigger` | body `{name, to, payload, transactionId?, actor?, bridgeUrl?}`. `to` is one or an array (max 100) of: `"id"`, `{subscriberId, email?, ...}`, `{type:"Topic", topicKey}`; more than 2000 subscribers after expansion is 400. 201 `{data:{acknowledged:true, status:"processed", transactionId}}`. Unknown workflow 422. Payload failing the workflow's JSON schema 400. Processing is asynchronous. A `transactionId` that already has messages or runs that were not cancelled is acknowledged the same way and dropped (as upstream); deleting its messages releases it. |
| `DELETE /v1/events/trigger/:transactionId` | cancels pending jobs of that transaction. 200 `{data:true|false}`. |
| `POST /v2/subscribers` | upsert `{subscriberId, email?, locale?, firstName?, lastName?, phone?, avatar?, timezone?, data?}`. 201 subscriber DTO. `failIfExists` is ignored (its 409 is retried by the SDK); null fields leave stored values untouched. |
| `PATCH /v2/subscribers/:id` | partial update. 200 subscriber DTO. 404 when missing. |
| `DELETE /v2/subscribers/:id` | removes subscriber, tokens, preferences, topic memberships, messages, runs and their digest events, and closes the subscriber's WebSockets. 200 `{data:{acknowledged:true,status:"deleted"}}`. 404 when missing. |
| `PUT /v1/subscribers/:id/credentials` | body `{providerId, integrationIdentifier?, credentials:{deviceTokens:[...]}}`. Replaces the token set (max 100). 200 subscriber DTO. When the environment has no integration for that provider (or identifier), 400 (as upstream) with message exactly `Subscribers environment (<envId>) do not have active <providerId> integration.` Unknown subscriber 400 `SubscriberId: <id> not found`. |
| `POST /v2/topics` | `{key, name?}` upsert. 201 `{data:{_id, key, name?, createdAt, updatedAt}}` (`name` omitted when unset: the SDK rejects null). |
| `POST /v2/topics/:key/subscriptions` | `{subscriptions:[{identifier, subscriberId}]}` (also accept plain `subscriberIds`). Entries may be plain subscriber ids; a missing `identifier` defaults to upstream's `tk_<key>:si_<subscriberId>`. Max 100. Creates the topic when missing. 201 `{data:[{_id, identifier, topic, subscriber, createdAt, updatedAt}], meta:{totalCount, successful, failed}, errors:[{subscriberId, code:"SUBSCRIBER_NOT_FOUND", message}]}` (not unwrapped: two keys). |
| `DELETE /v2/topics/:key/subscriptions` | same body and response, 200. Removal is by `subscriberId`; an entry with only `identifier` is 400. Unknown topic 404. |
| `POST /v1/subscribers/:id/messages/mark-as` | `{messageId: string | string[], markAs: "read"|"unread"|"seen"|"unseen"}`. Max 100 ids; ids that do not parse are dropped, none left is 400. 201 `{data: MessageResponseDto[]}`. Pushes realtime count events. |
| `GET /v1/messages` | query `subscriberId`, `transactionId` (repeated key: the SDK explodes arrays), `channel`, `page` (zero-based), `limit` (default 10, max 1000). `{data:[...], hasMore, pageSize, page}`. |
| `DELETE /v1/messages/transaction/:transactionId` | deletes messages and finished runs of that transaction (so the id can be triggered again), pushes realtime count events. 204. |

Wire names carry underscores (`_id`, `_organizationId`, `_environmentId`); the SDK remaps them.
Subscriber DTO needs at least `subscriberId, _organizationId, _environmentId, deleted,
createdAt, updatedAt` (`_id` too). `_organizationId` is the environment id. MessageResponseDto
needs `_environmentId, _organizationId, _notificationId, _subscriberId, transactionId, channel,
read, seen, cta, status, createdAt`; optional strings are omitted, never null.

## Inbox API (crate `inbox`)

Headers from the SDK: `Novu-API-Version: 2024-06-26`, `Novu-Client-Version`,
`Authorization: Bearer <session token>`. The SDK appends `/v1` to `apiUrl`. Errors: non-2xx
with JSON `{message}`. CORS: allow any origin, no credentials.

| route | notes |
| --- | --- |
| `POST /v1/inbox/session` | `{applicationIdentifier, subscriberHash, subscriber:{subscriberId, timezone?, ...}}` (legacy top-level `subscriberId` too). Creates the subscriber when missing and sets `timezone`/`locale`; other fields are ignored. 201 `{data:{token, totalUnreadCount, unreadCount:{total, severity:{high, medium, low, none}}, removeNovuBranding:true, isDevelopmentMode:false, maxSnoozeDurationHours:0, applicationIdentifier, contextKeys:[]}}`. Unknown application identifier: 400. Bad or missing hash: 401. |
| `GET /v1/inbox/notifications` | query `limit` (default 10, max 100), `after` (id cursor), `offset`, `tags[]`, `read`, `archived`, `snoozed`, `seen`, `data` (JSON object, all keys must match), `severity[]` or `severity`. `createdGte`/`createdLte` answer 400 (not implemented). `{data:[InboxNotification], hasMore, filter}`; `filter` echoes the given filter fields. Newest first. |
| `GET /v1/inbox/notifications/count` | query `filters=<JSON array of filters>`, at most 30 (400 above). `{data:[{count, filter}]}`. Echo each filter exactly as received: the SDK matches with strict equality, so no added keys and no nulls. |
| `PATCH /v1/inbox/notifications/:id/{read,unread,archive,unarchive}` | empty or `{}` body. `{data: InboxNotification}` (full object). |
| `POST /v1/inbox/notifications/{read,seen,archive,read-archive}` | `{tags?, data?}` bulk, `data` is a `JSON.stringify`-ed object. `seen` also takes `notificationIds` (the SDK's single `seen(id)` uses it). `read-archive` archives the read ones. 204. |
| `GET /v1/inbox/preferences` | query `tags[]`, `severity[]`, `criticality` (`nonCritical` default, `critical`, `all`). `{data:[Preference]}`: one `level:"global"` then one `level:"template"` per workflow, oldest first. Global lists the channels the environment can deliver (`in_app`, plus `push`/`email` with an fcm/smtp integration). |
| `PATCH /v1/inbox/preferences` and `/v1/inbox/preferences/:workflowId` | body is channel booleans, e.g. `{push:false}`; other keys (`schedule`, `enabled`) are ignored. `:workflowId` is the internal workflow id (what the SDK sends) or the identifier. `{data: Preference}`. Critical (read-only) workflows refuse with 400. |

`InboxNotification`: `id, transactionId, subject?, body, to:{subscriberId,...}, isRead, isSeen,
isArchived, isSnoozed` (real booleans, always present), `snoozedUntil?, deliveredAt?, createdAt`
(ISO), `readAt?, firstSeenAt?, archivedAt?, avatar?, primaryAction?:{label,isCompleted,redirect?},
secondaryAction?, channelType, tags` (array, always), `data?, redirect?:{url,target?}, workflow?,
severity`.

`Preference`: `{level, enabled, channels:{in_app?,push?,email?,...}, workflow?:{id, identifier,
name, critical, tags?, severity}, schedule?}`. Only channels the workflow has steps for.

Snooze is not implemented (`isSnoozed` always false; snooze routes answer 400). Not served
(unused by the app): `/notifications/:id/{complete,revert}`, deletes, `/preferences/global`,
`/preferences/bulk`.

### WebSocket

Plain WebSocket (the SDK uses partysocket). The client connects to
`<socketUrl>?token=<session token>`, keeping the path of `socketUrl`; the upgrade is served
on `/` and `/ws`. Bad token or deleted subscriber: 401, connection cap reached: 429, both
before upgrading. The
client sends text `ping` every 25 s; answer `pong`. Connections silent for 60 s are closed;
frames are capped at 4 KiB. Server frames are JSON `{event, data}`:

- `notification_received`: `data.message` in the raw message shape: `_id, content, subject,
  read, seen, archived, snoozedUntil?, createdAt, tags, data, cta:{data:{url,target},
  action:{buttons:[{type,content,url}], status, result:{type}}}` (`cta` always present),
  `avatar, channel, workflow, severity, subscriber, transactionId`.
- `unread_count_changed`: `data.counts` = `{total, severity:{high, medium, low, none}}` (read by
  the SDK), plus `data.unreadCount` (total).
- `unseen_count_changed`: `data.unseenCount`.

A new in-app message sends `notification_received` then both count events; any other change
sends both count events.

There is no polling fallback in the app, so every state change (new message, mark, delete,
archive, from either API) must publish through the `Hub`.

## Engine (crate `engine`)

### Bridge protocol

- Discover: `GET <bridgeUrl>?action=discover` → `{workflows:[{workflowId, name, description,
  severity, tags, preferences, payload:{schema}, steps:[{stepId, type, controls:{schema},
  outputs:{schema}, results:{schema}, options, providers}]}], agents:[]}`.
- Execute: `POST <bridgeUrl>?action=execute&workflowId=&stepId=` with
  `{payload, subscriber, state:[{stepId, outputs, state:{status, error}}], controls:{},
  context:{}, env:{}, actor?, workflowId, action:"execute"}`; the framework merges
  `workflowId`, `stepId` and `action` from the query. `state` holds an entry for every prior
  step, skipped or disabled ones included (the framework throws on a missing one). Response
  `{outputs, providers, options:{skip}, metadata:{status, error, duration}}`.
- `options.skip`, or empty `outputs` (the workflow function returned early), is a skip.
- Timeout 5 s. Transient failure (network, 5xx, 429): retry with backoff, 3 attempts, then
  the run fails and is kept for inspection in logs. 4xx: fail without retry.

Step outputs: `in_app` `{subject?, body, avatar?, redirect?:{url,target?}, primaryAction?,
secondaryAction?, data?}`; `push` `{subject, body}`; `email` `{subject, body}`; `digest`
`{amount, unit, digestKey?}`; `delay` `{type:"regular", amount, unit}`. Only regular
delays and digests are supported; timed, cron or dynamic ones fail the run, and so does a
duration longer than 366 days, negative or not finite.

State entries passed to later steps: `in_app` `{seen, read, lastSeenDate, lastReadDate}`
(read live from the message), `push`/`email` `{}`, `delay` `{duration}` (ms), `digest`
`{events:[{id, time, payload}], eventCount}`: `events` holds the oldest 50 events up to 64 KiB
of payload, `eventCount` counts all of them. Skipped steps get the empty shape
(`seen:false`, `duration:0`, `events:[]`).

### Trigger pipeline

1. Resolve the workflow (bridge-synced, by `workflowId`). Validate payload against its schema.
2. Resolve recipients: direct subscribers are upserted from `to` (email etc. updated when
   given); topics expand to members, deduplicated; the actor is excluded only from topic
   expansion.
3. One run per recipient, persisted before answering. The run is a durable row: workflow,
   subscriber, payload, actor, transaction id, step cursor, accumulated state, `run_at`.
4. The runner walks steps in order. For each step: resolve preferences (below); if the
   channel is disabled, skip without calling the bridge; otherwise execute on the bridge and
   act on the result.
5. `delay`: set `run_at = now + duration`, persist, return.
6. `digest`: key is the `digestKey` output value itself (the code resolves it, as in
   Novu), absent means one group; scoped to workflow + subscriber + step. If an open digest run exists for that key, append this
   event to it and end this run (merged). Otherwise this run becomes the digest owner and
   sleeps until the window ends; on wake its state carries all events.
7. Cancel (`DELETE /v1/events/trigger/:transactionId`): pending runs of that transaction end.
   Cancelling a digest owner hands ownership to the oldest merged event when there is one.

Scheduler: one task sleeping until the earliest `run_at` or a `Notify`. No polling. On
startup, runs left in progress are resumed. A run is executed by one worker at a time
(claimed in the same statement that reads it).

### Preferences

Order, later wins: workflow defaults from code (`preferences.all`, `preferences.channels`)
→ subscriber global → subscriber per-workflow. A workflow whose preferences are read-only
(`all.readOnly`) is critical: subscriber settings are ignored and it is hidden from
`criticality=nonCritical` listings.

### Delivery

- `in_app`: insert message, publish `notification_received` + counts on the `Hub`.
- `push`: FCM HTTP v1 for every device token of the subscriber under the matching
  integration. Tokens FCM reports as unregistered or invalid are deleted. Message carries
  `notification{title, body}` and the workflow payload as string `data`; honor
  `providers.fcm` overrides returned by the bridge.
- `email`: SMTP.
- A delivery failure never blocks later steps of other runs; transient failures retry with
  the run's backoff.

SMTP is `lettre` (rustls + ring). FCM uses no crate: the OAuth/FCM crates (`gcp_auth`,
`yup-oauth2`) bring their own hyper client and TLS setup for two requests. The
service-account assertion is an RS256 JWT signed with `ring` (not `jsonwebtoken`, whose RSA
backend is the `rsa` crate, RUSTSEC-2023-0071), exchanged at the file's `token_uri`; the
access token is cached per integration until a minute before it expires. Sends go over
`reqwest`. The FCM endpoint base is the
integration's optional `endpoint` so tests use a fake.

## Storage

Tables (SQLite, `STRICT`, every table keyed by environment id): subscribers, device_tokens,
topics, topic_subscribers, workflows, preferences, messages, runs, digest_events,
idempotency_keys. Ids are 24 hex chars (time-prefixed, sortable; the inbox cursor relies on
order). Timestamps are unix milliseconds in storage, ISO-8601 UTC with milliseconds on the
wire. Retention: messages older than 90 days, finished runs older than 7 days (kept while
still an event of an open digest) and idempotency keys older than 24 h are purged by a daily
task.

## Verification

- `cargo test --workspace`: unit tests for signature, hash, JWT, preference merge, digest
  merge/cancel, filter matching, cursor paging.
- `tests/contract` (`npm test`, node test runner on TypeScript through Node's type stripping): starts the binary with a temp config, a
  bridge built with the real `@novu/framework` (workflows covering: in_app+push, digest then
  in_app+push, delay then in_app+push with skip, email, early return), a fake FCM + token
  endpoint, a fake SMTP sink. Drives the real `@novu/api` client for every server route and
  the real `@novu/js` client (REST + WebSocket) for every inbox route and realtime event.
  A response the SDK rejects or silently drops fails the test.
- `scripts/check-rss.sh`: starts the release binary with one environment, waits, reads
  `VmRSS` from `/proc/<pid>/status`, fails above 20 MB.
- `nix build`, `nix flake check`.

## Not implemented

Web UI, workflow editor, dashboard-defined workflows, tenants/contexts, layouts,
translations, snooze, SMS, chat, webhooks, activity feed API, subscriber bulk import,
environment promotion, APNs (push is FCM only), Postgres storage.
