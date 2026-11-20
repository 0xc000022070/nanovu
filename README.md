# nanovu

Self-hosted notification server that speaks the Novu SDK wire protocol (`@novu/api`,
`@novu/framework`, `@novu/js`). Clients change only the URL. Scope and contract: `SPEC.md`.

## Run

```sh
nanovu check --config nanovu.toml
nanovu serve --config nanovu.toml
nanovu hash --config nanovu.toml --env stg <subscriberId>
```

Config format and refusals: `SPEC.md`, "Configuration and setup". Logs go to stderr;
`RUST_LOG` takes `tracing` targets syntax (`info`, `warn,nanovu_engine=debug`).

## Behind a reverse proxy

Run behind a TLS proxy on loopback. nanovu trusts only the rightmost `X-Forwarded-For` entry
and only from a loopback peer, so the proxy must append the client address and never
forward a client-supplied header as is (nginx: `proxy_set_header X-Forwarded-For
$remote_addr;`). It should also cap header size and connections and buffer request bodies
(nginx: `client_max_body_size 64k; proxy_request_buffering on;`): nanovu handles 8 requests at
once and a slow sender holds one of them for up to 5 s.

## Build and test

```sh
nix develop
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/check-rss.sh          # idle RSS of the release binary, fails above 20 MB
nix build                     # static musl binary
nix flake check
```

## Contract tests

`cd tests/contract && npm ci && npm test` runs the pinned Novu SDKs against a live `nanovu serve`.
The tests are TypeScript run by Node's own type stripping (Node 22.18 or newer, no build step).
`NANOVU_BIN` picks the binary; the default is the `cargo build -p nanovu` output.

## NixOS

```nix
{
  imports = [ nanovu.nixosModules.default ];
  services.nanovu = {
    enable = true;
    credentials.stg-secret-key = "/run/secrets/nanovu-stg-key";
    settings.environments.stg = {
      application_identifier = "abc123";
      secret_key_file = "/run/credentials/nanovu.service/stg-secret-key";
      bridge_hosts = [ "api.example.com" ];
    };
  };
}
```
