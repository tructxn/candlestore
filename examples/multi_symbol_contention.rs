//! Measure single-symbol contention vs multi-symbol parallelism.
//!
//! Usage:
//!   cargo run --release --example multi_symbol_contention
//!
//! Two scenarios, same total work (N_THREADS × N_PER_THREAD appends):
//!
//!   1. **Contended**: every thread appends to the SAME symbol. Per-symbol
//!      `RwLock<RingBuffer>` serialises all writers — they queue on one lock.
//!
//!   2. **Independent**: each thread appends to its OWN symbol. The per-symbol
//!      locks are disjoint, so writers run in parallel.
//!
//! Wall-clock ratio measures the parallelism the redesign actually unlocks.

use std::sync::Arc;
use std::time::Instant;

use candlestore::{Candle, CandleStore};

const N_THREADS: usize = 4;
const N_PER_THREAD: i64 = 100_000;

fn make_candle(ts: i64) -> Candle {
    Candle { ts, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1.0 }
}

fn run_contended(store: Arc<CandleStore>) -> std::time::Duration {
    let start = Instant::now();
    let handles: Vec<_> = (0..N_THREADS).map(|t| {
        let s = Arc::clone(&store);
        std::thread::spawn(move || {
            // All threads hammer the same symbol — write lock is contended.
            for i in 0..N_PER_THREAD {
                s.append("SHARED", make_candle((t as i64) * N_PER_THREAD + i));
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }
    start.elapsed()
}

fn run_independent(store: Arc<CandleStore>) -> std::time::Duration {
    let start = Instant::now();
    let handles: Vec<_> = (0..N_THREADS).map(|t| {
        let s = Arc::clone(&store);
        std::thread::spawn(move || {
            // Each thread owns its own symbol — independent locks.
            let sym = format!("SYM_{t}");
            for i in 0..N_PER_THREAD {
                s.append(&sym, make_candle(i));
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }
    start.elapsed()
}

fn main() {
    let total_appends = (N_THREADS as i64 * N_PER_THREAD) as f64;
    println!(
        "{N_THREADS} threads × {N_PER_THREAD} appends each = {} total\n",
        N_THREADS * N_PER_THREAD as usize
    );

    // warm up
    let _ = run_contended(Arc::new(CandleStore::new(16)));
    let _ = run_independent(Arc::new(CandleStore::new(16)));

    let contended = run_contended(Arc::new(CandleStore::new(16)));
    let independent = run_independent(Arc::new(CandleStore::new(16)));

    let c_thrpt = total_appends / contended.as_secs_f64() / 1e6;
    let i_thrpt = total_appends / independent.as_secs_f64() / 1e6;

    println!("contended (all → same symbol):");
    println!("  wall = {:>8.2} ms   throughput = {c_thrpt:>5.2}M append/s", contended.as_secs_f64() * 1000.0);

    println!("independent (each → own symbol):");
    println!("  wall = {:>8.2} ms   throughput = {i_thrpt:>5.2}M append/s", independent.as_secs_f64() * 1000.0);

    println!("\nparallelism gain: {:.2}× faster wall time", contended.as_secs_f64() / independent.as_secs_f64());
    println!("(theoretical max for {N_THREADS} threads: {N_THREADS}.00×)");
}
