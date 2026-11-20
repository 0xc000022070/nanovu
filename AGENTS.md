# nanovu

Self-hosted server speaking the Novu SDK wire protocol. Clients keep `@novu/api`,
`@novu/framework` and `@novu/js` / `@novu/react-native` and change only the URL.

`SPEC.md` is the contract: routes, shapes, limits, security, what is not implemented.
Read it before changing behavior. When the SPEC and the pinned SDK disagree, the SDK wins
and the SPEC gets fixed. `README.md` has the short command list.

## Work on the code

```sh
nix develop
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
(cd tests/contract && npm ci && npm test)   # real SDKs against `cargo build -p nanovu`
scripts/check-rss.sh                         # idle RSS, fails above 20 MB
nix build && nix flake check                 # static musl binary, module checks
```

- Crate boundaries, dependency direction and design rules: `SPEC.md`, "Layout".
- A wire change needs a contract test in `tests/contract`, driven by the real SDK client.
  Unit tests alone do not prove the SDK accepts a response.
- Contract tests are TypeScript run by Node's type stripping (Node 22.18+). No build step.
- Pinned SDK sources to read when a shape is unclear: `tests/contract/node_modules/@novu/`.
- Keep the 20 MB idle RSS budget: no new runtime, TLS stack or heavy crate without
  checking `scripts/check-rss.sh`.

## Set up a server

### 1. Secret key per environment

One random key per environment, at least 32 chars. It authenticates `ApiKey` calls, signs
bridge requests and verifies subscriber hashes, so the client backend must hold the same
value.

```sh
openssl rand -hex 32 | age -r <recipient> -o nanovu-stg-secret-key.age
```

Never print it. Pipe it from the encrypted file into whatever needs it.

### 2. Config

TOML, secrets only as file paths. Full example and startup refusals: `SPEC.md`,
"Configuration and setup".

- `application_identifier`: the value the app passes to the inbox provider. Reusing the
  Novu Cloud identifier means the app config does not change.
- `bridge_hosts`: every host the client backend serves its bridge from. Sync to any other
  host fails with `Bridge host <host> is not allowed`.
- `integrations.<id>`: `provider = "fcm"` with `service_account_file` (the Firebase service
  account JSON), or `provider = "smtp"` with `url_file` and `from`. The integration id is
  what `integrationIdentifier` on `PUT /v1/subscribers/:id/credentials` matches. Push goes
  through every FCM integration of the environment that holds tokens for the subscriber.
- Secret files must not be world-readable; group-readable only when the group is root.

`nanovu check --config nanovu.toml` validates config and prints environment ids.

### 3. NixOS

```nix
{
  inputs.nanovu.url = "github:0xc000022070/nanovu";

  # in a host module
  imports = [ inputs.nanovu.nixosModules.default ];
  services.nanovu = {
    enable = true;
    credentials = {
      stg-secret-key = config.age.secrets.nanovu-stg-secret-key.path;
      fcm-stg = config.age.secrets.nanovu-fcm-stg.path;
    };
    settings = {
      listen = "127.0.0.1:4010";
      environments.stg = {
        application_identifier = "<app identifier>";
        secret_key_file = "/run/credentials/nanovu.service/stg-secret-key";
        bridge_hosts = [ "api.example.com" ];
        integrations.fcm-stg = {
          provider = "fcm";
          service_account_file = "/run/credentials/nanovu.service/fcm-stg";
        };
      };
    };
  };
}
```

- `credentials` maps a name to a path outside the Nix store; systemd `LoadCredential`
  copies it to `/run/credentials/nanovu.service/<name>` and the `*_file` keys point there.
- The unit runs as `DynamicUser` with state in `/var/lib/nanovu` and `MemoryMax=24M`.

### 4. Expose it

Listen on loopback and put TLS in front (Cloudflare tunnel, nginx, Caddy). Proxy rules
(`X-Forwarded-For`, body buffering): `README.md`, "Behind a reverse proxy".

With a Cloudflare tunnel the ingress rule alone is not enough: the hostname also needs a
proxied CNAME to `<tunnel-id>.cfargotunnel.com` (`cloudflared tunnel route dns <tunnel>
<host>` or the DNS API). A machine that looked the name up before the record existed keeps
the NXDOMAIN for the zone's negative TTL (Cloudflare: 1800 s); flush with
`resolvectl flush-caches`.

Check: `curl https://<host>/healthz` is 200, `POST /v1/bridge/sync` without a key is 401.

## Point clients at it

Backend (`@novu/api` + `@novu/framework`):

```ts
new Novu({ secretKey, serverURL: "https://<host>" });

new Client({ secretKey, apiUrl: "https://<host>", strictAuthentication: true });

await fetch("https://<host>/v1/bridge/sync", {
  method: "POST",
  headers: { "Content-Type": "application/json", Authorization: `ApiKey ${secretKey}` },
  body: JSON.stringify({ bridgeUrl: "https://<bridge host>/<bridge path>" }),
});
```

- Set the environment's nanovu key as the backend's Novu secret key. Bridge signatures
  are HMAC'd with it, so a backend still holding the Novu Cloud key answers discover with
  `401 SignatureMismatchError` and sync fails with 400 `Bridge discover failed`.
- Sync on every deploy: it replaces the environment's workflows with what discover returns.
  Triggering a workflow that was never synced is 422.

App (`@novu/js` / `@novu/react-native`):

```ts
<NovuProvider
  applicationIdentifier="<app identifier>"
  subscriberId={id}
  subscriberHash={hmacSha256Hex(secretKey, id)}   // computed by the backend
  apiUrl="https://<host>"
  socketUrl="wss://<host>/ws"
  socketOptions={{ socketType: "cloud" }}
/>
```

`socketType: "cloud"` selects the plain WebSocket client; the default socket.io client does
not talk to nanovu.

### Encore secrets

`encore secret set --type <types>` refuses when the types split an existing secret group
(for example `pr` while one group holds `preview` and `production`) and lists the
conflicting group; nothing is written. Read `encore secret list <name>` first and set types
that match whole groups. A running `encore run` keeps the value it started with: restart it.

## Migrating from Novu Cloud

What moves:

- workflows: nothing to do, bridge sync recreates them
- integrations: copy FCM service accounts and SMTP credentials into secret files
- app identifier: reuse it in `application_identifier`

What does not move: subscribers, device tokens, topics, preferences and inbox history live
in Novu Cloud only. Subscribers and tokens come back as the backend upserts them and the
app registers its token again; history and preferences start empty. SMS, chat, APNs and
Novu's demo email provider have no equivalent (`SPEC.md`, "Not implemented").

## Debug

- Logs: `journalctl -u nanovu`; `RUST_LOG=warn,nanovu_engine=debug` for runner detail.
- `nanovu hash --config <path> --env <name> <subscriberId>` prints the subscriber hash the
  inbox session expects.
- Bridge failures log as `bridge discover failed` / execute errors with the bridge's
  status and body.
- Sign a discover call by hand to test a backend's key without nanovu:
  `v1 = HMAC-SHA256(key, "<t>.{}")`, header `novu-signature: t=<unix ms>,v1=<hex>`,
  `GET <bridgeUrl>?action=discover`.
