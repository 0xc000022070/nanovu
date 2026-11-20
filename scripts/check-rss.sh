#!/usr/bin/env bash
set -euo pipefail

limit_kb=$((20 * 1024))
bin=${1:-}
if [[ -z $bin ]]; then
  cargo build --release --quiet -p nanovu
  target=$(cargo metadata --format-version 1 --no-deps | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
  bin=$target/release/nanovu
fi

dir=$(mktemp -d)
pid=
cleanup() {
  if [[ -n $pid ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf -- "$dir"
}
trap cleanup EXIT

port=$((20000 + RANDOM % 20000))
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$dir/key"
chmod 600 "$dir/key"
cat >"$dir/config.toml" <<TOML
listen = "127.0.0.1:$port"
database = "$dir/nanovu.db"

[environments.rss]
application_identifier = "rss"
secret_key_file = "$dir/key"
TOML

RUST_LOG=warn "$bin" serve --config "$dir/config.toml" &
pid=$!
for _ in $(seq 50); do
  curl -sf "http://127.0.0.1:$port/healthz" >/dev/null && break
  sleep 0.1
done
sleep 2

rss_kb=$(awk '/^VmRSS:/ { print $2 }' "/proc/$pid/status")
echo "idle RSS: ${rss_kb} kB (limit ${limit_kb} kB), binary: $(stat -c %s "$bin") bytes"
((rss_kb <= limit_kb))
