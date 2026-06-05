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

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use candlestore::{Candle, ShmRingWriter, available_cores, pin_to_core};

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
    // Extract a u32 in [0, u32::MAX], map to [-0.0015, +0.0015] (±0.15%)
    let u = (*seed >> 32) as u32 as f64;
    let change = u / u32::MAX as f64 * 0.003 - 0.0015;
    (prev * (1.0 + change)).max(1.0)
}

fn main() {
    let shm_name  = env_str("FEED_SHM_NAME", "/tradekern_feed");
    let shm_cap   = env_usize("FEED_SHM_CAP", 65_536);
    let core_id   = env_usize("FEED_CORE",    0);
    let symbol    = env_str("FEED_SYMBOL",   "BTCUSDT:1m");
    let rate      = env_usize("FEED_RATE",    10_000);

    // ── pin to core ───────────────────────────────────────────────────────────
    let cores  = available_cores();
    let pinned = pin_to_core(core_id % cores);
    let pin_note = if cfg!(target_os = "linux") { "hard" } else { "soft-hint" };
    println!("feed_handler  cores={cores}  core={core_id}  affinity={pin_note}({pinned})");
    println!("symbol={symbol}  shm={shm_name}  cap={shm_cap}  rate={rate}/s");

    // ── create SHM ring ───────────────────────────────────────────────────────
    let writer = ShmRingWriter::create(&shm_name, shm_cap)
        .expect("failed to create shm ring — is market_hub already holding it?");
    println!("shm ring created — start market_hub in another terminal");
    println!("Ctrl-C to stop.\n");

    // ── generate candles ──────────────────────────────────────────────────────
    let interval  = Duration::from_nanos(1_000_000_000 / rate as u64);
    let mut price = 50_000.0_f64;
    let mut seed  = 0xDEAD_BEEF_1234_5678u64;
    let mut count = 0u64;
    let mut next_tick = Instant::now();
    let report_every = rate * 5; // print stats every 5 seconds

    loop {
        // Rate-limit: sleep until next scheduled tick
        let now = Instant::now();
        if now < next_tick {
            std::thread::sleep(next_tick - now);
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
        writer.push(candle);
        count += 1;

        if count % report_every as u64 == 0 {
            println!("[feed] pushed {count} candles  last_close={close:.2}");
        }
    }
}
