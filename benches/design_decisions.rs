/// Microbenchmarks that isolate the performance impact of each
/// key architectural decision in candlestore.
///
/// Run with: cargo bench --bench design_decisions
///
/// Each group is labelled "decision_N" to make it easy to scan results.
use std::collections::HashMap as StdHashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use candlestore::ring_buffer::RingBuffer;
use candlestore::{Candle, CandleStore, HardwareProfile};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hashbrown::HashMap as HbHashMap;
use tempfile::TempDir;

fn make_candle(ts: i64) -> Candle {
    Candle { ts, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1000.0 }
}

// ── Decision 1: Ring buffer vs Vec append ─────────────────────────────────────
//
// Ring: O(1) guaranteed, fixed memory, wraps around when full (oldest overwritten).
// Vec no-prealloc: amortized O(1) — doubles capacity on overflow (realloc spikes).
// Vec prealloc: O(1) like ring but allocates N * 48 bytes upfront regardless of use.
//
// Trading implication: realloc spikes cause tail latency. A ring's worst-case
// append equals its best-case append.

fn bench_ring_vs_vec_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("decision_1_ring_vs_vec_append");

    for n in [1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("ring", n), &n, |b, &n| {
            b.iter(|| {
                let mut ring = RingBuffer::new(10_240);
                for i in 0..n {
                    ring.push(make_candle(i as i64));
                }
                black_box(ring.len())
            });
        });

        group.bench_with_input(BenchmarkId::new("vec_no_prealloc", n), &n, |b, &n| {
            b.iter(|| {
                let mut v: Vec<Candle> = Vec::new();
                for i in 0..n {
                    v.push(make_candle(i as i64));
                }
                black_box(v.len())
            });
        });

        group.bench_with_input(BenchmarkId::new("vec_prealloc", n), &n, |b, &n| {
            b.iter(|| {
                let mut v: Vec<Candle> = Vec::with_capacity(n as usize);
                for i in 0..n {
                    v.push(make_candle(i as i64));
                }
                black_box(v.len())
            });
        });
    }
    group.finish();
}

// ── Decision 2: hashbrown vs std HashMap for symbol lookup ───────────────────
//
// candlestore uses hashbrown (same underlying implementation as std since Rust 1.36,
// but with faster raw table API and no SipHash-DoS protection overhead for trusted keys).
// Symbol keys are not attacker-controlled, so DoS resistance is wasted cost.

fn bench_symbol_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("decision_2_symbol_lookup");

    let symbols: Vec<String> = (0..100).map(|i| format!("SYM{}/USDT:1m", i)).collect();
    let mut hb: HbHashMap<&str, u64> = HbHashMap::new();
    let mut std: StdHashMap<&str, u64> = StdHashMap::new();
    for (i, s) in symbols.iter().enumerate() {
        hb.insert(s.as_str(), i as u64);
        std.insert(s.as_str(), i as u64);
    }

    group.bench_function("hashbrown_100_symbols", |b| {
        b.iter(|| {
            for s in &symbols {
                black_box(hb.get(s.as_str()));
            }
        });
    });

    group.bench_function("std_hashmap_100_symbols", |b| {
        b.iter(|| {
            for s in &symbols {
                black_box(std.get(s.as_str()));
            }
        });
    });

    group.finish();
}

// ── Decision 3: L3-fit capacity vs overflow ──────────────────────────────────
//
// ring_capacity_for(10) uses (usable_L3 / symbols) to keep hot candles in L3.
// An oversized ring forces the CPU to fetch from DRAM during range scans.
// Two costs compound: more elements to scan (O(n) scan) AND cache misses.
//
// Both rings are filled to their full capacity so the comparison is realistic:
// l3_fit has ~2,800 elements; l3_overflow has 30× more (~84,000 elements).

fn bench_l3_fit_vs_overflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("decision_3_l3_fit_vs_overflow");

    let hw = HardwareProfile::detect();
    let fit_cap      = hw.ring_capacity_for(10);
    let overflow_cap = (fit_cap * 30).max(100_000);

    // Window spans the oldest candles in the ring (ts 0..window).
    // After filling to capacity the ring may have wrapped, so these candles
    // might not exist — but the scan still traverses the full ring looking.
    for window in [100i64, 1_000] {
        group.bench_with_input(BenchmarkId::new("l3_fit", window), &window, |b, &w| {
            let mut ring = RingBuffer::new(fit_cap);
            for i in 0..fit_cap as i64 { ring.push(make_candle(i)); }
            b.iter(|| black_box(ring.range(0, w)));
        });

        group.bench_with_input(BenchmarkId::new("l3_overflow", window), &window, |b, &w| {
            let mut ring = RingBuffer::new(overflow_cap);
            for i in 0..overflow_cap as i64 { ring.push(make_candle(i)); }
            b.iter(|| black_box(ring.range(0, w)));
        });
    }
    group.finish();
}

// ── Decision 4: Lock strategy — parking_lot vs std, RwLock vs Mutex ──────────
//
// candlestore uses parking_lot::RwLock. Why parking_lot?
//   - No allocation (std::sync::RwLock heap-allocates on some OSes)
//   - Smaller and faster in the uncontended fast path
// Why RwLock not Mutex?
//   - Many readers can hold a read lock concurrently
//   - Mutex serialises all callers — even concurrent reads block each other
//
// This benchmark measures the lock acquisition + release cost single-threaded.
// Under concurrent read load the Mutex penalty multiplies by the reader count.

fn bench_lock_strategy(c: &mut Criterion) {
    let mut group = c.benchmark_group("decision_4_lock_strategy");

    // Standard library locks
    let std_rw = RwLock::new(42u64);
    let std_mx = Mutex::new(42u64);

    // parking_lot locks (what candlestore uses)
    let pl_rw = parking_lot::RwLock::new(42u64);
    let pl_mx = parking_lot::Mutex::new(42u64);

    group.bench_function("std_rwlock_read", |b| {
        b.iter(|| black_box(*std_rw.read().unwrap()))
    });
    group.bench_function("std_mutex_lock", |b| {
        b.iter(|| black_box(*std_mx.lock().unwrap()))
    });
    group.bench_function("parking_lot_rwlock_read", |b| {
        b.iter(|| black_box(*pl_rw.read()))
    });
    group.bench_function("parking_lot_mutex_lock", |b| {
        b.iter(|| black_box(*pl_mx.lock()))
    });

    // Show concurrent reader scaling: 4 threads, RwLock vs Mutex
    // Each bench spawns 4 threads, each doing `iters` reads.
    // RwLock: all 4 proceed in parallel → elapsed ≈ 1× single-reader time.
    // Mutex:  all 4 serialise        → elapsed ≈ 4× single-reader time.
    const THREADS: usize = 4;
    let shared_data: Vec<Candle> = (0..10_000).map(make_candle).collect();
    let rw_data = Arc::new(RwLock::new(shared_data.clone()));
    let mx_data = Arc::new(Mutex::new(shared_data));

    group.bench_function("rwlock_4_concurrent_readers", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let handles: Vec<_> = (0..THREADS).map(|_| {
                let d = rw_data.clone();
                std::thread::spawn(move || {
                    for _ in 0..iters {
                        let g = d.read().unwrap();
                        black_box(g[0].ts);
                    }
                })
            }).collect();
            for h in handles { h.join().unwrap(); }
            start.elapsed()
        });
    });

    group.bench_function("mutex_4_concurrent_readers", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let handles: Vec<_> = (0..THREADS).map(|_| {
                let d = mx_data.clone();
                std::thread::spawn(move || {
                    for _ in 0..iters {
                        let g = d.lock().unwrap();
                        black_box(g[0].ts);
                    }
                })
            }).collect();
            for h in handles { h.join().unwrap(); }
            start.elapsed()
        });
    });

    group.finish();
}

// ── Decision 5: Hot (ring buffer) vs cold (Parquet) read ─────────────────────
//
// When a symbol is evicted via LRU it spills to Parquet. The next range() call
// hits the cold path: file discovery + Parquet decode + merge with hot data.
// This quantifies the cost of a cold miss so callers can size ring_capacity
// to keep their working set in RAM.
//
// LRU eviction order: symbol written LEAST recently is evicted first.
// Write COLD first (becomes LRU), then HOT (stays MRU).
// Adding EVICT as a 3rd symbol evicts COLD to Parquet.
// After setup: HOT is in RAM, COLD is on disk.

fn bench_hot_vs_cold(c: &mut Criterion) {
    let mut group = c.benchmark_group("decision_5_hot_vs_cold");
    group.measurement_time(Duration::from_secs(20));

    let dir = TempDir::new().unwrap();

    let store = CandleStore::new(2).with_data_dir(dir.path());
    for i in 0..1_000i64 { store.append("COLD/USDT:1m", make_candle(i)); } // written first → LRU
    for i in 0..1_000i64 { store.append("HOT/USDT:1m",  make_candle(i)); } // written last → MRU
    store.append("EVICT/USDT:1m", make_candle(0)); // 3rd symbol → evicts COLD to Parquet

    group.bench_function("hot_ring_read_1000", |b| {
        b.iter(|| black_box(store.range("HOT/USDT:1m", 0, 1_000)))
    });

    group.bench_function("cold_parquet_read_1000", |b| {
        b.iter(|| black_box(store.range("COLD/USDT:1m", 0, 1_000)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ring_vs_vec_append,
    bench_symbol_lookup,
    bench_l3_fit_vs_overflow,
    bench_lock_strategy,
    bench_hot_vs_cold,
);
criterion_main!(benches);
