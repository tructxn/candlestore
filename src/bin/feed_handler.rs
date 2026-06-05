//! Feed handler — simulates an exchange data feed and writes candles to a
//! shared-memory SPSC ring, consumed by `market_hub`.
//!
//! Usage:
//!   cargo run --release --bin feed_handler
//!
//! The process pins itself to core 0, generates synthetic candles via a
//! geometric Brownian motion simulation, and pushes them at ~10k candles/sec.
//!
//! Environment (all optional):
//!   FEED_SHM_NAME   POSIX shm name        default: /tradekern_feed
//!   FEED_SHM_CAP    Ring capacity (pow2)  default: 65536
//!   FEED_CORE       CPU core to pin to    default: 0
//!   FEED_SYMBOL     Symbol name           default: BTCUSDT:1m
//!   FEED_RATE       Candles / second      default: 10000
//!   METRICS_PORT    /metrics HTTP port    default: 9090
//!   RUST_LOG        tracing filter        default: info

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use candlestore::{Candle, ShmRingWriter, available_cores, pin_to_core};
use signal_hook::consts::{SIGINT, SIGTERM};
use tracing::{error, info, warn};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

/// LCG-based price simulation — no rand dependency needed.
fn next_price(prev: f64, seed: &mut u64) -> f64 {
    *seed = seed.wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
    let u = (*seed >> 32) as u32 as f64;
    let change = u / u32::MAX as f64 * 0.003 - 0.0015;
    (prev * (1.0 + change)).max(1.0)
}

// ── observability ────────────────────────────────────────────────────────────

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .with_target(false)
        .compact()
        .init();
}

/// Install the global Prometheus recorder and start the /metrics HTTP server.
fn install_metrics_endpoint() {
    let port: u16 = std::env::var("METRICS_PORT")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(9090);
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    let result = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install();

    match result {
        Ok(()) => info!(%addr, "prometheus /metrics endpoint up"),
        Err(e) => error!(%addr, error = %e, "failed to install prometheus exporter"),
    }
}

/// Spawn a 1-Hz metrics-publishing thread reading from `candles_pushed`.
/// Exits cleanly when `shutdown` is set.
fn spawn_metrics_poller(candles_pushed: Arc<AtomicU64>, shutdown: Arc<AtomicBool>) {
    std::thread::Builder::new()
        .name("metrics-poller".into())
        .spawn(move || {
            metrics::describe_counter!(
                "candlestore_feed_candles_pushed_total",
                "Total candles pushed by feed_handler to the SHM ring."
            );
            while !shutdown.load(Ordering::Relaxed) {
                // Park for up to 1 s so shutdown is observed within that window.
                std::thread::park_timeout(Duration::from_secs(1));
                let n = candles_pushed.load(Ordering::Relaxed);
                metrics::counter!("candlestore_feed_candles_pushed_total").absolute(n);
            }
            info!("metrics poller exiting");
        })
        .expect("spawn metrics-poller");
}

/// Register SIGINT + SIGTERM handlers that flip `shutdown` to `true`.
/// Returns `Err` if registration fails (typically only in extremely
/// constrained environments, e.g. seccomp-locked sandboxes).
fn install_signal_handlers(shutdown: &Arc<AtomicBool>) -> std::io::Result<()> {
    signal_hook::flag::register(SIGINT, Arc::clone(shutdown))?;
    signal_hook::flag::register(SIGTERM, Arc::clone(shutdown))?;
    Ok(())
}

/// Spin-push that periodically checks the shutdown flag. Without this, a
/// downstream consumer (market_hub) that exits leaves the SHM ring full;
/// `ShmRingWriter::push` would spin forever and the feed_handler would
/// ignore SIGTERM until the ring drained (i.e. never).
///
/// Returns `true` if pushed, `false` if shutdown signal observed.
#[inline]
fn push_or_shutdown(
    writer:   &ShmRingWriter,
    candle:   Candle,
    shutdown: &AtomicBool,
) -> bool {
    while !writer.try_push(candle) {
        if shutdown.load(Ordering::Relaxed) {
            return false;
        }
        std::hint::spin_loop();
    }
    true
}

// ── main ─────────────────────────────────────────────────────────────────────

fn main() {
    init_tracing();
    install_metrics_endpoint();

    // ── graceful shutdown wiring ─────────────────────────────────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    if let Err(e) = install_signal_handlers(&shutdown) {
        error!(error = %e, "failed to install signal handlers — SIGTERM will be ungraceful");
    }

    let shm_name = env_str("FEED_SHM_NAME", "/tradekern_feed");
    let shm_cap  = env_usize("FEED_SHM_CAP", 65_536);
    let core_id  = env_usize("FEED_CORE",    0);
    let symbol   = env_str("FEED_SYMBOL",   "BTCUSDT:1m");
    let rate     = env_usize("FEED_RATE",    10_000);

    // ── pin to core ──────────────────────────────────────────────────────────
    let cores = available_cores();
    let pinned = pin_to_core(core_id % cores);
    let pin_note = if cfg!(target_os = "linux") { "hard" } else { "soft-hint" };
    info!(
        cores, core = core_id, affinity = pin_note, pinned,
        %symbol, %shm_name, shm_cap, rate,
        "feed_handler starting"
    );

    // ── create SHM ring ──────────────────────────────────────────────────────
    let writer = match ShmRingWriter::create(&shm_name, shm_cap) {
        Ok(w) => w,
        Err(e) => {
            error!(%shm_name, error = %e, "failed to create SHM ring (is another feed running?)");
            std::process::exit(1);
        }
    };
    info!(%shm_name, "shm ring created — start market_hub in another terminal (Ctrl-C to stop)");

    // ── metrics: 1-Hz poller fed by the hot-loop counter ─────────────────────
    let candles_pushed = Arc::new(AtomicU64::new(0));
    spawn_metrics_poller(Arc::clone(&candles_pushed), Arc::clone(&shutdown));

    // ── generate candles ─────────────────────────────────────────────────────
    let interval  = Duration::from_nanos(1_000_000_000 / rate as u64);
    let mut price = 50_000.0_f64;
    let mut seed  = 0xDEAD_BEEF_1234_5678u64;
    let mut count = 0u64;
    let mut next_tick = Instant::now();
    let report_every = (rate * 5) as u64; // info log every 5 seconds
    let mut last_log_count = 0u64;
    let mut last_log_at = Instant::now();

    while !shutdown.load(Ordering::Relaxed) {
        // Rate-limit: sleep until next scheduled tick
        let now = Instant::now();
        if now < next_tick {
            std::thread::sleep(next_tick - now);
        } else if now > next_tick + interval * 100 {
            warn!(
                behind_ticks = (now - next_tick).as_nanos() / interval.as_nanos(),
                "feed loop fell behind schedule — resyncing"
            );
            next_tick = now;
        }
        next_tick += interval;

        let open   = price;
        let close  = next_price(price, &mut seed);
        let r1     = (seed >> 32) as u32 as f64 / u32::MAX as f64;
        let r2     = (seed & 0xFFFF_FFFF) as f64 / u32::MAX as f64;
        let high   = open.max(close) * (1.0 + r1 * 0.001);
        let low    = open.min(close) * (1.0 - r2 * 0.001);
        let volume = 0.5 + r1 * 2.0;
        price = close;

        let candle = Candle { ts: now_nanos(), open, high, low, close, volume };
        if !push_or_shutdown(&writer, candle, &shutdown) {
            // Shutdown signal observed while waiting for ring space — exit
            // the hot loop and let the graceful-shutdown block below run.
            break;
        }
        count += 1;
        if count & 1023 == 0 {
            candles_pushed.store(count, Ordering::Relaxed);
        }

        if count.is_multiple_of(report_every) {
            let elapsed = last_log_at.elapsed().as_secs_f64();
            let rate_now = (count - last_log_count) as f64 / elapsed;
            info!(count, last_close = close, rate_per_sec = rate_now, "feed progress");
            last_log_count = count;
            last_log_at = Instant::now();
        }
    }

    // ── graceful shutdown ────────────────────────────────────────────────────
    info!(total_pushed = count, "shutdown signal received — draining and unlinking SHM");
    // Final metric flush so a Prometheus scrape post-shutdown gets the
    // up-to-date count.
    candles_pushed.store(count, Ordering::Relaxed);
    // `writer` drops here → munmap + shm_unlink cleans up the segment so the
    // next feed_handler start doesn't see a stale segment.
    drop(writer);
    info!("SHM segment unlinked, feed_handler stopped");
}
