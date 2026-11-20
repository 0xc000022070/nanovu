use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::error::Error;
use std::hash::BuildHasher;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use nanovu_core::config::Config;
use nanovu_core::sqlite::SqliteStore;
use nanovu_core::{AppState, Hub, Store, time};
use nanovu_engine::Engine;
use serde_json::json;
use tokio::sync::{Notify, Semaphore};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const MAX_IN_FLIGHT: usize = 8;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const RATE_BURST: f64 = 100.0;
const RATE_PER_SEC: f64 = 50.0;
const RATE_MAX_KEYS: usize = 4096;

const USAGE: &str = "usage:
  nanovu serve --config <path>
  nanovu check --config <path>
  nanovu hash --config <path> --env <name> <subscriberId>";

enum Command {
    Serve(PathBuf),
    Check(PathBuf),
    Hash {
        config: PathBuf,
        env: String,
        subscriber_id: String,
    },
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Option<Command> {
    let mut args = args.into_iter();
    let command = args.next()?;
    let (mut config, mut env, mut positional) = (None, None, Vec::new());
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = Some(PathBuf::from(args.next()?)),
            "--env" => env = Some(args.next()?),
            _ => positional.push(arg),
        }
    }
    let config = config?;
    match (command.as_str(), env, positional.as_slice()) {
        ("serve", None, []) => Some(Command::Serve(config)),
        ("check", None, []) => Some(Command::Check(config)),
        ("hash", Some(env), [subscriber_id]) => Some(Command::Hash {
            config,
            env,
            subscriber_id: subscriber_id.clone(),
        }),
        _ => None,
    }
}

fn main() -> ExitCode {
    let Some(command) = parse_args(std::env::args().skip(1)) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let result = match command {
        Command::Serve(path) => serve(path),
        Command::Check(path) => check(path),
        Command::Hash {
            config,
            env,
            subscriber_id,
        } => hash(config, &env, &subscriber_id),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("nanovu: {err}");
            ExitCode::FAILURE
        }
    }
}

fn check(path: PathBuf) -> Result<(), Box<dyn Error>> {
    let config = Config::load(&path)?;
    println!(
        "config ok: listen {}, database {}",
        config.listen,
        config.database.display()
    );
    for env in config.environments.iter() {
        let integrations: Vec<String> = env
            .integrations
            .iter()
            .map(|i| format!("{} ({})", i.identifier, i.provider.id()))
            .collect();
        println!(
            "environment {}: id {}, application_identifier {}, integrations [{}]",
            env.name,
            env.id,
            env.application_identifier,
            integrations.join(", ")
        );
    }
    Ok(())
}

fn hash(path: PathBuf, env: &str, subscriber_id: &str) -> Result<(), Box<dyn Error>> {
    let config = Config::load(&path)?;
    let env = config
        .environments
        .by_name(env)
        .ok_or_else(|| format!("unknown environment {env}"))?;
    println!("{}", env.subscriber_hash(subscriber_id));
    Ok(())
}

fn serve(path: PathBuf) -> Result<(), Box<dyn Error>> {
    let targets: Targets = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| Targets::new().with_default(tracing::Level::INFO));
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(targets)
        .init();
    let config = Config::load(&path)?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(config))
}

async fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let store = Arc::new(SqliteStore::open(&config.database).await?);
    let requeued = store.requeue_running_runs().await?;
    let state = AppState {
        environments: Arc::new(config.environments),
        store,
        hub: Hub::default(),
    };
    for env in state.environments.iter() {
        tracing::info!(name = %env.name, id = %env.id, "environment loaded");
    }

    let engine = Engine::new(state.clone());
    let scheduler = engine.spawn();
    let purge = tokio::spawn(purge_daily(state.store.clone()));

    let limiter = Arc::new(Limiter::default());
    let permits = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let app = Router::new()
        .merge(nanovu_api::router(state.clone(), engine))
        .merge(nanovu_inbox::router(state))
        .layer(middleware::from_fn_with_state(permits, in_flight))
        .layer(middleware::from_fn_with_state(limiter, rate_limit))
        .route("/healthz", get(|| async { "ok" }));

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, requeued, "serving");
    let stopping = Arc::new(Notify::new());
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown({
        let stopping = stopping.clone();
        async move {
            shutdown_signal().await;
            tracing::info!("shutting down");
            stopping.notify_one();
        }
    });
    tokio::select! {
        result = server => result?,
        () = async { stopping.notified().await; tokio::time::sleep(SHUTDOWN_GRACE).await } => {
            tracing::warn!("open connections did not close in time");
        }
    }
    scheduler.abort();
    purge.abort();
    Ok(())
}

async fn shutdown_signal() {
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminate => {}
    }
}

async fn purge_daily(store: Arc<dyn Store>) {
    let mut interval = tokio::time::interval(Duration::from_millis(time::DAY.unsigned_abs()));
    loop {
        interval.tick().await;
        if let Err(err) = store.purge_expired(time::now_ms()).await {
            tracing::error!(error = %err, "retention purge failed");
        }
    }
}

struct Bucket {
    tokens: f64,
    at: Instant,
}

#[derive(Default)]
struct Limiter {
    hasher: RandomState,
    buckets: Mutex<HashMap<u64, Bucket>>,
}

impl Limiter {
    fn allow(&self, ip: IpAddr, now: Instant) -> bool {
        let hash = self.hasher.hash_one(rate_key(ip));
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        if !buckets.contains_key(&hash)
            && buckets.len() >= RATE_MAX_KEYS
            && let Some(stalest) = buckets.iter().min_by_key(|(_, b)| b.at).map(|(k, _)| *k)
        {
            buckets.remove(&stalest);
        }
        let bucket = buckets.entry(hash).or_insert(Bucket {
            tokens: RATE_BURST,
            at: now,
        });
        let elapsed = now.duration_since(bucket.at).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * RATE_PER_SEC).min(RATE_BURST);
        bucket.at = now;
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }
}

fn rate_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => IpAddr::V6((u128::from(v6) & !u128::from(u64::MAX)).into()),
    }
}

fn client_ip(request: &Request, peer: IpAddr) -> IpAddr {
    if !peer.is_loopback() {
        return peer;
    }
    request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.rsplit(',').next())
        .and_then(|ip| ip.trim().parse().ok())
        .unwrap_or(peer)
}

async fn rate_limit(
    State(limiter): State<Arc<Limiter>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if limiter.allow(client_ip(&request, peer.ip()), Instant::now()) {
        return next.run(request).await;
    }
    let body = json!({
        "statusCode": 429,
        "timestamp": time::iso(time::now_ms()),
        "path": request.uri().path(),
        "message": "Too many requests",
    });
    (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response()
}

async fn in_flight(
    State(permits): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Response {
    match permits.acquire().await {
        Ok(_permit) => next.run(request).await,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Option<Command> {
        parse_args(line.split_whitespace().map(str::to_owned))
    }

    #[test]
    fn cli_parsing() {
        assert!(matches!(
            args("serve --config c.toml"),
            Some(Command::Serve(_))
        ));
        assert!(matches!(
            args("check --config c.toml"),
            Some(Command::Check(_))
        ));
        assert!(matches!(
            args("hash --config c.toml --env stg sub-1"),
            Some(Command::Hash { env, subscriber_id, .. }) if env == "stg" && subscriber_id == "sub-1"
        ));
        assert!(args("serve").is_none());
        assert!(args("hash --config c.toml sub-1").is_none());
        assert!(args("serve --config c.toml extra").is_none());
        assert!(args("serve --config").is_none());
    }

    #[test]
    fn limiter_burst_refill_and_bound() {
        let limiter = Limiter::default();
        let start = Instant::now();
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        assert!((0..100).all(|_| limiter.allow(ip, start)));
        assert!(!limiter.allow(ip, start));
        assert!(limiter.allow(ip, start + Duration::from_millis(40)));

        let v6 = |s: &str| s.parse::<IpAddr>().unwrap();
        assert!((0..100).all(|_| limiter.allow(v6("2001:db8::1"), start)));
        assert!(!limiter.allow(v6("2001:db8::2"), start));
        assert!(limiter.allow(v6("2001:db8:0:1::1"), start));
    }

    #[test]
    fn a_full_limiter_evicts_the_stalest_bucket_instead_of_denying() {
        let limiter = Limiter::default();
        let start = Instant::now();
        let legit: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(limiter.allow(legit, start));
        let flood = start + Duration::from_secs(1);
        for i in 0..u32::try_from(RATE_MAX_KEYS).unwrap() * 2 {
            assert!(limiter.allow(IpAddr::V4((0x0b00_0000 + i).into()), flood));
        }
        assert!(limiter.buckets.lock().unwrap().len() <= RATE_MAX_KEYS);
        assert!(limiter.allow(legit, flood));
    }
}
