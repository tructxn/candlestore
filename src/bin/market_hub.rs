//! Market hub — ingests candles from the feed handler, stores them, runs a
//! SMA-crossover strategy, and routes signals to the paper executor.
//!
//! Usage (start AFTER feed_handler):
//!   cargo run --release --bin market_hub
//!
//! Thread layout (each pinned to its own core):
//!
//!   core HUB_CORE:      ingester thread   (ShmRingReader → CandleStore)
//!   core HUB_CORE+1:    strategy thread   (CandleStore → SpscRing<Signal>)
//!   core HUB_CORE+2:    executor thread   (SpscRing<Signal> → paper trades)
//!
//! Environment (all optional):
//!   FEED_SHM_NAME    POSIX shm name         default: /tradekern_feed
//!   FEED_SHM_CAP     Ring capacity (pow2)   default: 65536
//!   HUB_CORE         First core to use      default: 1
//!   FEED_SYMBOL      Symbol from feed        default: BTCUSDT:1m
//!   SMA_SHORT        Short SMA period        default: 10
//!   SMA_LONG         Long  SMA period        default: 20
//!   SIGNAL_QTY       Order quantity          default: 0.1

use std::sync::Arc;
use std::time::{Duration, Instant};
use candlestore::{
    Candle, CandleStore, ShmRingReader, ShmIngester,
    Signal, Side, SpscRing, SpscWriter, SpscReader,
    available_cores, pin_to_core,
};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

// ── SMA crossover strategy ────────────────────────────────────────────────────

fn sma(candles: &[Candle], period: usize) -> Option<f64> {
    if candles.len() < period { return None; }
    let slice = &candles[candles.len() - period..];
    Some(slice.iter().map(|c| c.close).sum::<f64>() / period as f64)
}

fn strategy_thread(
    store:      Arc<CandleStore>,
    symbol:     String,
    short_p:    usize,
    long_p:     usize,
    qty:        f64,
    sig_tx:     SpscWriter<Signal>,
    core_id:    usize,
) {
    let cores  = available_cores();
    let ok     = pin_to_core(core_id % cores);
    println!("[strategy] started  core={core_id}  affinity_set={ok}  \
              symbol={symbol}  SMA({short_p}/{long_p})");

    let min_candles = long_p + 1;
    let mut prev_short: Option<f64> = None;
    let mut prev_long:  Option<f64> = None;
    let mut signal_count = 0u64;
    let report_every = 50;

    loop {
        std::thread::sleep(Duration::from_millis(50));

        let candles = store.range(&symbol, 0, i64::MAX);
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

                if signal_count % report_every == 0 || signal_count <= 5 {
                    println!(
                        "[strategy] signal #{signal_count}  {:?}  qty={qty}  \
                         SMA{short_p}={cs:.2}  SMA{long_p}={cl:.2}",
                        side
                    );
                }
            }
        }

        prev_short = curr_short;
        prev_long  = curr_long;
    }
}

// ── Paper executor ────────────────────────────────────────────────────────────

fn executor_thread(
    sig_rx:     SpscReader<Signal>,
    core_id:    usize,
) {
    let cores  = available_cores();
    let ok     = pin_to_core(core_id % cores);
    println!("[executor] started  core={core_id}  affinity_set={ok}");

    let mut position: f64 = 0.0;
    let mut total_signals  = 0u64;
    let start = Instant::now();
    let mut last_report = Instant::now();

    loop {
        match sig_rx.try_pop() {
            Some(sig) => {
                total_signals += 1;
                let delta = match sig.side() {
                    Side::Buy  =>  sig.qty,
                    Side::Sell => -sig.qty,
                };
                position += delta;

                println!(
                    "[executor] #{total_signals:>4}  {:?}  qty={:.4}  \
                     symbol={}  position={:.4}",
                    sig.side(), sig.qty, sig.symbol_str(), position
                );
            }
            None => std::hint::spin_loop(),
        }

        if last_report.elapsed() >= Duration::from_secs(30) {
            last_report = Instant::now();
            println!(
                "[executor] uptime={:.0}s  signals={}  position={:.4}",
                start.elapsed().as_secs_f64(), total_signals, position
            );
        }
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    let shm_name  = env_str("FEED_SHM_NAME", "/tradekern_feed");
    let shm_cap   = env_usize("FEED_SHM_CAP", 65_536);
    let hub_core  = env_usize("HUB_CORE",      1);
    let symbol    = env_str("FEED_SYMBOL",    "BTCUSDT:1m");
    let short_p   = env_usize("SMA_SHORT",     10);
    let long_p    = env_usize("SMA_LONG",      20);
    let qty       = env_f64("SIGNAL_QTY",      0.1);

    let cores = available_cores();
    println!("market_hub  cores={cores}");
    println!("shm={shm_name}  cap={shm_cap}  symbol={symbol}");
    println!("threads: ingester→core{hub_core}  strategy→core{}  executor→core{}",
        hub_core + 1, hub_core + 2);

    // ── create store ──────────────────────────────────────────────────────────
    let store = Arc::new(CandleStore::from_hardware(10));

    // ── open SHM ring and start ingester (pinned to hub_core) ─────────────────
    println!("waiting for feed handler on {shm_name}...");
    let reader = loop {
        match ShmRingReader::open(&shm_name, shm_cap) {
            Ok(r)  => break r,
            Err(_) => std::thread::sleep(Duration::from_millis(500)),
        }
    };
    println!("shm ring opened — ingester starting");

    let pin_note = if cfg!(target_os = "linux") { "hard" } else { "soft-hint" };
    println!("affinity mode: {pin_note}");

    let store_for_ingester = Arc::clone(&store);
    let symbol_for_ingester = symbol.clone();
    let ingester = std::thread::Builder::new()
        .name("ingester".into())
        .spawn(move || {
            pin_to_core(hub_core % cores);
            ShmIngester::start(reader, store_for_ingester, symbol_for_ingester)
            // ShmIngester lives as long as this closure — drops when thread exits
        })
        .expect("spawn ingester thread");
    let _ = ingester; // keep thread running

    // ── signal bus: strategy → executor ──────────────────────────────────────
    let (sig_tx, sig_rx): (SpscWriter<Signal>, SpscReader<Signal>) = SpscRing::new(1024);

    // ── spawn strategy thread ─────────────────────────────────────────────────
    let store_for_strategy = Arc::clone(&store);
    let sym_strategy = symbol.clone();
    std::thread::Builder::new()
        .name("strategy".into())
        .spawn(move || strategy_thread(
            store_for_strategy, sym_strategy,
            short_p, long_p, qty,
            sig_tx,
            (hub_core + 1) % cores,
        ))
        .expect("spawn strategy thread");

    // ── spawn executor thread ─────────────────────────────────────────────────
    std::thread::Builder::new()
        .name("executor".into())
        .spawn(move || executor_thread(sig_rx, (hub_core + 2) % cores))
        .expect("spawn executor thread");

    // ── main thread: periodic store stats ─────────────────────────────────────
    let start = Instant::now();
    loop {
        std::thread::sleep(Duration::from_secs(5));
        let count = store.candle_count(&symbol);
        let elapsed = start.elapsed().as_secs_f64();
        println!(
            "[hub] uptime={elapsed:.0}s  candles_in_store={count}"
        );
    }
}
