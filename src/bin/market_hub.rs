//! Market hub — ingests candles from the feed handler, stores them, runs a
//! SMA-crossover strategy, and routes signals to the paper executor.
//!
//! Usage (start AFTER feed_handler):
//!   cargo run --release --bin market_hub
//!
//! Thread layout (each pinned to its own core):
//!
//!   core HUB_CORE:      ingester thread   (ShmRingReader → CandleStore)
//!   core HUB_CORE+1:    strategy thread   (CandleStore → `SpscRing<Signal>`)
//!   core HUB_CORE+2:    executor thread   (`SpscRing<Signal>` → paper trades)
//!
//! The strategy is **reactive**: it spins on `CandleStore::wait_for_change`
//! (an `AtomicU64` version counter the ingester bumps on every append) and
//! recomputes its SMA the instant a new candle lands. Pinned to its own core
//! → 100% CPU on that core is the design, not a bug.
//!
//! Environment (all optional):
//!   FEED_SHM_NAME    POSIX shm name         default: /tradekern_feed
//!   FEED_SHM_CAP     Ring capacity (pow2)   default: 65536
//!   HUB_CORE         First core to use      default: 1
//!   FEED_SYMBOL      Symbol from feed       default: BTCUSDT:1m
//!   SMA_SHORT        Short SMA period       default: 10
//!   SMA_LONG         Long  SMA period       default: 20
//!   SIGNAL_QTY       Order quantity         default: 0.1
//!   METRICS_PORT     /metrics HTTP port     default: 9091
//!   RUST_LOG         tracing filter         default: info
//!
//! ## Observability
//!
//! - `tracing` logs go to stderr (configurable via RUST_LOG).
//! - Prometheus `/metrics` endpoint serves at 0.0.0.0:$METRICS_PORT.
//! - A 1-Hz polling thread reads `store.snapshot()` and `ingester.stats()`
//!   and updates Prometheus gauges/counters. The hot threads (ingester,
//!   strategy, executor) never emit metrics inline — all observability
//!   happens via low-frequency snapshots of atomic counters.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use candlestore::{
    CandleStore, ShmRingReader, ShmIngester,
    Signal, Side, SpscRing, SpscWriter, SpscReader,
    available_cores, pin_to_core, Candle,
};
use signal_hook::consts::{SIGINT, SIGTERM};
use tracing::{error, info, warn};

// ── env helpers ─────────────────────────────────────────────────────────────

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

// ── observability bootstrap ─────────────────────────────────────────────────

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

fn install_metrics_endpoint() {
    let port: u16 = std::env::var("METRICS_PORT")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(9091);
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    match metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
    {
        Ok(()) => info!(%addr, "prometheus /metrics endpoint up"),
        Err(e) => error!(%addr, error = %e, "failed to install prometheus exporter"),
    }
}

fn describe_all_metrics() {
    metrics::describe_counter!(
        "candlestore_appends_total",
        "Total candles appended to the store across all symbols."
    );
    metrics::describe_gauge!(
        "candlestore_symbols_active",
        "Symbols currently held in RAM (hot)."
    );
    metrics::describe_counter!(
        "candlestore_evictions_total",
        "Lifetime LRU evictions from the store."
    );
    metrics::describe_counter!(
        "candlestore_parquet_spill_bytes_total",
        "Lifetime bytes successfully spilled to Parquet on eviction."
    );
    metrics::describe_counter!(
        "candlestore_parquet_spill_errors_total",
        "Lifetime Parquet spill failures (each is also logged at ERROR)."
    );
    metrics::describe_counter!(
        "candlestore_appends_rejected_total",
        "Lifetime appends rejected to preserve existing data when a spill failed."
    );
    metrics::describe_counter!(
        "candlestore_ingest_popped_total",
        "Lifetime messages popped from the SHM ring by the ingester."
    );
    metrics::describe_gauge!(
        "candlestore_ingest_ring_depth",
        "Instantaneous unread messages in the SHM ring (lag indicator)."
    );
    metrics::describe_gauge!(
        "candlestore_ingest_ring_fill_ratio",
        "SHM ring fill fraction in [0, 1]. Sustained > 0.5 indicates backpressure."
    );
    metrics::describe_counter!(
        "candlestore_signals_total",
        "Lifetime SMA-crossover signals produced by the strategy."
    );
    metrics::describe_counter!(
        "candlestore_executor_signals_total",
        "Lifetime signals consumed by the paper executor."
    );
    metrics::describe_gauge!(
        "candlestore_executor_position",
        "Current paper position (positive = long, negative = short)."
    );
    metrics::describe_counter!(
        "candlestore_invalid_candles_total",
        "Lifetime candles rejected at the boundary for NaN/Inf/negative-ts. \
         Sustained growth means the producer is sending garbage."
    );
    metrics::describe_counter!(
        "candlestore_out_of_order_total",
        "Lifetime candles accepted but whose ts went backwards. May be \
         invisible to binary-search range queries until a re-sort runs."
    );
}

// ── SMA crossover strategy ──────────────────────────────────────────────────

fn sma(candles: &[Candle], period: usize) -> Option<f64> {
    if candles.len() < period { return None; }
    let slice = &candles[candles.len() - period..];
    Some(slice.iter().map(|c| c.close).sum::<f64>() / period as f64)
}

// The strategy thread is a process-boundary entry. Each parameter is a
// distinct piece of runtime state with no natural sub-grouping — pushing
// half of them into a config struct just adds an unwrap site.
#[allow(clippy::too_many_arguments)]
fn strategy_thread(
    store:           Arc<CandleStore>,
    shutdown:        Arc<AtomicBool>,
    symbol:          String,
    short_p:         usize,
    long_p:          usize,
    qty:             f64,
    sig_tx:          SpscWriter<Signal>,
    signals_counter: Arc<AtomicU64>,
    core_id:         usize,
) {
    let cores = available_cores();
    let ok = pin_to_core(core_id % cores);
    info!(core = core_id, affinity_set = ok, %symbol, short_p, long_p, "strategy thread started");

    let min_candles = long_p + 1;
    let mut prev_short: Option<f64> = None;
    let mut prev_long:  Option<f64> = None;
    let mut signal_count = 0u64;
    let report_every = 50;
    let strategy_start = Instant::now();

    let mut last_seen: u64 = 0;

    loop {
        last_seen = store.wait_for_change(last_seen);

        // Shutdown is signalled by main calling `store.signal_waiters()`,
        // which bumps the version one extra time to unblock this spinner.
        if shutdown.load(Ordering::Relaxed) {
            info!(signals = signal_count, "strategy thread exiting on shutdown");
            return;
        }

        let candles = store.last_n(&symbol, min_candles);
        if candles.len() < min_candles { continue; }

        let curr_short = sma(&candles, short_p);
        let curr_long  = sma(&candles, long_p);

        if let (Some(ps), Some(pl), Some(cs), Some(cl)) =
            (prev_short, prev_long, curr_short, curr_long)
        {
            let side = if ps <= pl && cs > cl {
                Some(Side::Buy)   // golden cross
            } else if ps >= pl && cs < cl {
                Some(Side::Sell)  // death cross
            } else {
                None
            };

            if let Some(side) = side {
                use std::time::{SystemTime, UNIX_EPOCH};
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
                let sig = Signal::new(ts, &symbol, side, qty, 0.0, 1);
                sig_tx.push(sig);
                signal_count += 1;
                signals_counter.store(signal_count, Ordering::Relaxed);

                if signal_count.is_multiple_of(report_every) || signal_count <= 5 {
                    let uptime = strategy_start.elapsed().as_secs_f64();
                    info!(
                        n = signal_count,
                        side = ?side, qty, sma_short = cs, sma_long = cl, uptime,
                        "signal"
                    );
                }
            }
        }

        prev_short = curr_short;
        prev_long  = curr_long;
    }
}

// ── Paper executor ──────────────────────────────────────────────────────────

fn executor_thread(
    sig_rx:           SpscReader<Signal>,
    shutdown:         Arc<AtomicBool>,
    signals_consumed: Arc<AtomicU64>,
    position_atomic:  Arc<parking_lot::Mutex<f64>>,
    core_id:          usize,
) {
    let cores = available_cores();
    let ok = pin_to_core(core_id % cores);
    info!(core = core_id, affinity_set = ok, "executor thread started");

    let mut position: f64 = 0.0;
    let mut total_signals: u64 = 0;

    // Drain remaining signals on shutdown so the last few are accounted for
    // in metrics, then exit. The drain bound prevents an infinite loop if
    // the producer is still pushing (it shouldn't be — strategy exits first).
    loop {
        match sig_rx.try_pop() {
            Some(sig) => {
                total_signals += 1;
                let delta = match sig.side() {
                    Side::Buy  =>  sig.qty,
                    Side::Sell => -sig.qty,
                };
                position += delta;

                signals_consumed.store(total_signals, Ordering::Relaxed);
                *position_atomic.lock() = position;

                info!(
                    n = total_signals,
                    side = ?sig.side(),
                    qty = sig.qty,
                    symbol = sig.symbol_str(),
                    position,
                    "executed"
                );
            }
            None => {
                if shutdown.load(Ordering::Relaxed) {
                    info!(
                        executed = total_signals, final_position = position,
                        "executor thread exiting on shutdown"
                    );
                    return;
                }
                std::hint::spin_loop();
            }
        }
    }
}

// ── metrics polling thread ──────────────────────────────────────────────────

struct PollerHandles {
    store:            Arc<CandleStore>,
    ingester_stats:   Arc<dyn Fn() -> candlestore::IngesterStats + Send + Sync>,
    signals_produced: Arc<AtomicU64>,
    signals_consumed: Arc<AtomicU64>,
    position:         Arc<parking_lot::Mutex<f64>>,
    shutdown:         Arc<AtomicBool>,
}

fn metrics_poller(h: PollerHandles) {
    info!("metrics poller started — emitting at 1 Hz");
    while !h.shutdown.load(Ordering::Relaxed) {
        // park_timeout so shutdown is observed within 1 s instead of waiting
        // for sleep to elapse fully.
        std::thread::park_timeout(Duration::from_secs(1));
        if h.shutdown.load(Ordering::Relaxed) { break; }

        let store_snap = h.store.snapshot();
        let ig         = (h.ingester_stats)();
        let signals_p  = h.signals_produced.load(Ordering::Relaxed);
        let signals_c  = h.signals_consumed.load(Ordering::Relaxed);
        let pos        = *h.position.lock();

        metrics::counter!("candlestore_appends_total").absolute(store_snap.appends_total);
        metrics::gauge!("candlestore_symbols_active").set(store_snap.symbol_count as f64);
        metrics::counter!("candlestore_evictions_total").absolute(store_snap.evictions_total);
        metrics::counter!("candlestore_parquet_spill_bytes_total")
            .absolute(store_snap.parquet_spill_bytes_total);
        metrics::counter!("candlestore_parquet_spill_errors_total")
            .absolute(store_snap.parquet_spill_errors_total);
        metrics::counter!("candlestore_appends_rejected_total")
            .absolute(store_snap.appends_rejected_total);
        metrics::counter!("candlestore_invalid_candles_total")
            .absolute(store_snap.invalid_candles_total);
        metrics::counter!("candlestore_out_of_order_total")
            .absolute(store_snap.out_of_order_total);

        // Operator alerts.
        if store_snap.appends_rejected_total > 0
            && store_snap.appends_rejected_total.is_multiple_of(1000)
        {
            warn!(
                total = store_snap.appends_rejected_total,
                "appends being rejected — Parquet spill is failing, investigate disk"
            );
        }
        if store_snap.invalid_candles_total > 0
            && store_snap.invalid_candles_total.is_multiple_of(100)
        {
            warn!(
                total = store_snap.invalid_candles_total,
                "invalid candles rejected — producer is sending NaN/Inf/negative-ts"
            );
        }

        metrics::counter!("candlestore_ingest_popped_total").absolute(ig.popped_total);
        metrics::gauge!("candlestore_ingest_ring_depth").set(ig.ring_depth as f64);
        let fill = if ig.ring_capacity > 0 {
            ig.ring_depth as f64 / ig.ring_capacity as f64
        } else { 0.0 };
        metrics::gauge!("candlestore_ingest_ring_fill_ratio").set(fill);
        if fill > 0.5 {
            warn!(fill_ratio = fill, depth = ig.ring_depth, capacity = ig.ring_capacity,
                "SHM ring fill > 50% — ingester may be falling behind");
        }

        metrics::counter!("candlestore_signals_total").absolute(signals_p);
        metrics::counter!("candlestore_executor_signals_total").absolute(signals_c);
        metrics::gauge!("candlestore_executor_position").set(pos);
    }
    info!("metrics poller exiting on shutdown");
}

/// Install SIGINT + SIGTERM handlers that flip `shutdown` to `true`.
fn install_signal_handlers(shutdown: &Arc<AtomicBool>) -> std::io::Result<()> {
    signal_hook::flag::register(SIGINT, Arc::clone(shutdown))?;
    signal_hook::flag::register(SIGTERM, Arc::clone(shutdown))?;
    Ok(())
}

// ── main ────────────────────────────────────────────────────────────────────

fn main() {
    init_tracing();
    install_metrics_endpoint();
    describe_all_metrics();

    // ── graceful shutdown wiring ─────────────────────────────────────────────
    let shutdown = Arc::new(AtomicBool::new(false));
    if let Err(e) = install_signal_handlers(&shutdown) {
        error!(error = %e, "failed to install signal handlers — SIGTERM will be ungraceful");
    }

    let shm_name = env_str("FEED_SHM_NAME", "/tradekern_feed");
    let shm_cap  = env_usize("FEED_SHM_CAP", 65_536);
    let hub_core = env_usize("HUB_CORE",      1);
    let symbol   = env_str("FEED_SYMBOL",    "BTCUSDT:1m");
    let short_p  = env_usize("SMA_SHORT",     10);
    let long_p   = env_usize("SMA_LONG",      20);
    let qty      = env_f64("SIGNAL_QTY",      0.1);

    let cores = available_cores();
    let pin_note = if cfg!(target_os = "linux") { "hard" } else { "soft-hint" };
    info!(
        cores, hub_core, %symbol, short_p, long_p, qty, affinity = pin_note,
        "market_hub starting"
    );

    // ── create store ─────────────────────────────────────────────────────────
    let store = Arc::new(CandleStore::from_hardware(10));

    // ── wait for SHM ring (also respect shutdown during the wait) ────────────
    info!(%shm_name, "waiting for feed_handler SHM ring");
    let reader = loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("shutdown received before SHM ring became available — exiting");
            return;
        }
        match ShmRingReader::open(&shm_name, shm_cap) {
            Ok(r)  => break r,
            Err(_) => std::thread::sleep(Duration::from_millis(500)),
        }
    };
    info!(%shm_name, "SHM ring opened");

    // ── start ingester (pinned to hub_core) ──────────────────────────────────
    let ingester = ShmIngester::start_on_core(
        reader,
        Arc::clone(&store),
        symbol.clone(),
        Some(hub_core % cores),
    );
    let ingester = Arc::new(ingester);
    let ingester_for_poller = Arc::clone(&ingester);

    // ── signal bus: strategy → executor ──────────────────────────────────────
    let (sig_tx, sig_rx): (SpscWriter<Signal>, SpscReader<Signal>) = SpscRing::new(1024);

    // ── counters shared between hot threads and the metrics poller ──────────
    let signals_produced = Arc::new(AtomicU64::new(0));
    let signals_consumed = Arc::new(AtomicU64::new(0));
    let position = Arc::new(parking_lot::Mutex::new(0.0f64));

    // ── spawn strategy thread ────────────────────────────────────────────────
    let store_for_strategy = Arc::clone(&store);
    let sym_strategy = symbol.clone();
    let signals_produced_for_strat = Arc::clone(&signals_produced);
    let shutdown_for_strat = Arc::clone(&shutdown);
    let strategy_join = std::thread::Builder::new()
        .name("strategy".into())
        .spawn(move || strategy_thread(
            store_for_strategy, shutdown_for_strat, sym_strategy,
            short_p, long_p, qty,
            sig_tx,
            signals_produced_for_strat,
            (hub_core + 1) % cores,
        ))
        .expect("spawn strategy thread");

    // ── spawn executor thread ────────────────────────────────────────────────
    let signals_consumed_for_exec = Arc::clone(&signals_consumed);
    let position_for_exec = Arc::clone(&position);
    let shutdown_for_exec = Arc::clone(&shutdown);
    let executor_join = std::thread::Builder::new()
        .name("executor".into())
        .spawn(move || executor_thread(
            sig_rx,
            shutdown_for_exec,
            signals_consumed_for_exec,
            position_for_exec,
            (hub_core + 2) % cores,
        ))
        .expect("spawn executor thread");

    // ── spawn metrics poller (background, 1 Hz) ──────────────────────────────
    let poller_handles = PollerHandles {
        store:            Arc::clone(&store),
        ingester_stats:   Arc::new(move || ingester_for_poller.stats()),
        signals_produced: Arc::clone(&signals_produced),
        signals_consumed: Arc::clone(&signals_consumed),
        position:         Arc::clone(&position),
        shutdown:         Arc::clone(&shutdown),
    };
    let poller_join = std::thread::Builder::new()
        .name("metrics-poller".into())
        .spawn(move || metrics_poller(poller_handles))
        .expect("spawn metrics-poller");

    // ── main thread: park until shutdown signal arrives ──────────────────────
    info!("startup complete — threads running (Ctrl-C to stop)");
    while !shutdown.load(Ordering::Relaxed) {
        // 100 ms park: tight enough that Ctrl-C feels instant, loose enough
        // that idle CPU is negligible. Signal handlers interrupt park early,
        // so this is a worst-case bound, not a polling interval.
        std::thread::park_timeout(Duration::from_millis(100));
    }
    info!("shutdown signal received — initiating graceful shutdown");

    // ── shutdown sequence ────────────────────────────────────────────────────
    //
    // Ordering matters (O5 from the deep review):
    // 1. ingester.stop_signal() — halts the SHM pop loop FIRST. Otherwise
    //    the ingester keeps pumping candles into a store no one is reading
    //    while we wait for strategy/executor/poller to join. Visible as
    //    wasted CPU on hub_core during the join window.
    // 2. signal_waiters() — wakes the strategy thread out of wait_for_change.
    // 3. Join strategy   — it exits cleanly, dropping sig_tx (the SpscWriter).
    // 4. Join executor   — it sees the shutdown flag on the next empty pop.
    // 5. Join poller     — it observes shutdown within ≤1s of being unparked.
    // 6. Drop the last `ingester` Arc — Drop → stop() (now a no-op since
    //    we already signalled in step 1) → join thread. ShmRingReader's
    //    Drop calls munmap on the shared segment (writer-side does shm_unlink).
    // 7. Drop the last `store` Arc.

    ingester.stop_signal();
    store.signal_waiters();
    // Also unpark each thread explicitly in case they're between iterations.
    strategy_join.thread().unpark();
    executor_join.thread().unpark();
    poller_join.thread().unpark();

    let join_timeout = Duration::from_secs(5);
    let join_with_label = |h: std::thread::JoinHandle<()>, label: &str| {
        let t0 = Instant::now();
        // std::thread::JoinHandle has no timeout-join. We poll-join on
        // `is_finished()` via a brief wait loop, then fall back to a regular
        // join which should be instant if `is_finished` was true.
        while !h.is_finished() && t0.elapsed() < join_timeout {
            std::thread::sleep(Duration::from_millis(10));
        }
        if h.is_finished() {
            h.join().ok();
            info!(thread = label, elapsed_ms = t0.elapsed().as_millis() as u64, "joined");
        } else {
            warn!(thread = label, "did not exit within timeout — detaching");
            // Detach: let the OS reclaim when process exits. The strategy/
            // executor threads are spin-loops that should always honor the
            // shutdown flag promptly; reaching this branch means a bug.
        }
    };

    join_with_label(strategy_join, "strategy");
    join_with_label(executor_join, "executor");
    join_with_label(poller_join,   "metrics-poller");

    // Drop the local ingester Arc. The poller's clone is also already
    // released (its thread joined). When the last clone drops, Drop runs
    // → ShmIngester::stop() → ingester thread joins → ShmRingReader Drop
    // → munmap.
    info!("stopping ingester (will join its thread)");
    drop(ingester);

    info!("dropping store");
    drop(store);

    info!("graceful shutdown complete");
}
