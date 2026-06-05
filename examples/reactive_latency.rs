//! Measure the wake-up latency of CandleStore::wait_for_change vs the old
//! 50ms sleep-poll model. Pinned threads, single-symbol, ~1µs sleep between
//! appends (well below the old 50ms granularity).
//!
//! Usage:
//!   cargo run --release --example reactive_latency

use std::sync::Arc;
use std::time::{Duration, Instant};

use candlestore::{Candle, CandleStore, pin_to_core, available_cores};

const N: usize = 200;
const SYMBOL: &str = "BTC/USDT:1m";

fn candle(ts: i64) -> Candle {
    Candle { ts, open: 100.0, high: 101.0, low: 99.0, close: 100.0, volume: 1.0 }
}

fn main() {
    let cores = available_cores();
    let store = Arc::new(CandleStore::new(1));

    // ── consumer: reactive (wait_for_change) ──────────────────────────────────
    let store_c = Arc::clone(&store);
    let consumer = std::thread::spawn(move || {
        pin_to_core(2 % cores);
        let mut last = 0u64;
        let mut latencies_ns = Vec::with_capacity(N);

        for _ in 0..N {
            // Sample T0 — the moment we are about to look for a new candle.
            let _ = store_c.candle_count(SYMBOL); // warm
            let t0 = Instant::now();
            last = store_c.wait_for_change(last);
            latencies_ns.push(t0.elapsed().as_nanos() as u64);
        }
        latencies_ns
    });

    // ── producer: append every ~10µs ──────────────────────────────────────────
    let store_p = Arc::clone(&store);
    let producer = std::thread::spawn(move || {
        pin_to_core(0);
        // small warm-up
        std::thread::sleep(Duration::from_millis(2));
        for i in 0..N {
            // Producer pace: ~10µs/append — well below the old 50ms sleep
            std::thread::sleep(Duration::from_micros(10));
            store_p.append(SYMBOL, candle(i as i64));
        }
    });

    producer.join().unwrap();
    let mut latencies = consumer.join().unwrap();
    latencies.sort_unstable();

    let p50 = latencies[N / 2];
    let p99 = latencies[(N * 99) / 100];
    let p999 = latencies[(N * 999) / 1000];
    let max = *latencies.last().unwrap();
    let mean: u64 = latencies.iter().sum::<u64>() / N as u64;

    println!(
        "wake-up latency (consumer detects new append):\n  \
        samples={N}\n  \
        mean = {mean:>8} ns  ({:.2} µs)\n  \
        p50  = {p50:>8} ns  ({:.2} µs)\n  \
        p99  = {p99:>8} ns  ({:.2} µs)\n  \
        p999 = {p999:>8} ns  ({:.2} µs)\n  \
        max  = {max:>8} ns  ({:.2} µs)\n",
        mean as f64 / 1000.0,
        p50  as f64 / 1000.0,
        p99  as f64 / 1000.0,
        p999 as f64 / 1000.0,
        max  as f64 / 1000.0,
    );

    let old_sleep_ms = 50.0;
    let new_p50_ms   = p50 as f64 / 1_000_000.0;
    println!(
        "vs old `sleep(50ms)` strategy poll:\n  \
         old_p50 ≈ {old_sleep_ms:.0} ms  (the sleep itself)\n  \
         new_p50 = {new_p50_ms:.3} ms\n  \
         improvement: {:.0}× lower wake-up latency",
        old_sleep_ms / new_p50_ms,
    );
}
