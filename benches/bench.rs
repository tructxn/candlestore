use std::collections::HashMap;
use candlestore::{Candle, CandleStore};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn make_candle(ts: i64) -> Candle {
    Candle { ts, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1000.0 }
}

// ── candlestore ───────────────────────────────────────────────────────────────

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append");
    for n in [1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let store = CandleStore::new(10);
                for i in 0..n {
                    store.append(black_box("BTC/USDT:1m"), make_candle(i as i64));
                }
            });
        });
    }
    group.finish();
}

fn bench_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");
    for window in [100i64, 1_000, 5_000] {
        group.throughput(Throughput::Elements(window as u64));
        group.bench_with_input(BenchmarkId::from_parameter(window), &window, |b, &window| {
            let store = CandleStore::new(10);
            for i in 0..10_000i64 { store.append("BTC/USDT:1m", make_candle(i)); }
            b.iter(|| black_box(store.range(black_box("BTC/USDT:1m"), 0, window)));
        });
    }
    group.finish();
}

fn bench_lru_eviction(c: &mut Criterion) {
    c.bench_function("lru_eviction_100_symbols", |b| {
        b.iter(|| {
            let store = CandleStore::new(10);
            for i in 0..100u64 {
                let sym = format!("SYM{}/USDT:1m", i);
                store.append(black_box(&sym), make_candle(i as i64));
            }
        });
    });
}

// ── naive baselines (what you'd write before reaching for candlestore) ────────

/// Naive: flat Vec<Candle> with a linear scan — no symbol map, no ring buffer.
fn bench_naive_vec_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("naive_vec_range");
    for window in [100i64, 1_000, 5_000] {
        group.throughput(Throughput::Elements(window as u64));
        group.bench_with_input(BenchmarkId::from_parameter(window), &window, |b, &window| {
            let data: Vec<Candle> = (0..10_000i64).map(make_candle).collect();
            b.iter(|| {
                // Allocate on every call — mirrors what naive code does
                let result: Vec<Candle> = data.iter()
                    .filter(|c| c.ts >= 0 && c.ts <= window)
                    .copied()
                    .collect();
                black_box(result);
            });
        });
    }
    group.finish();
}

/// Naive: HashMap<symbol, Vec<Candle>> — the obvious "add a map" upgrade.
fn bench_naive_hashmap_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("naive_hashmap_range");
    for window in [100i64, 1_000, 5_000] {
        group.throughput(Throughput::Elements(window as u64));
        group.bench_with_input(BenchmarkId::from_parameter(window), &window, |b, &window| {
            let mut map: HashMap<&str, Vec<Candle>> = HashMap::new();
            map.insert("BTC/USDT:1m", (0..10_000i64).map(make_candle).collect());
            b.iter(|| {
                let result: Vec<Candle> = map["BTC/USDT:1m"].iter()
                    .filter(|c| c.ts >= 0 && c.ts <= window)
                    .copied()
                    .collect();
                black_box(result);
            });
        });
    }
    group.finish();
}

/// Naive: HashMap + binary search (optimised naive — sorted vec + bisect).
fn bench_naive_hashmap_bisect(c: &mut Criterion) {
    let mut group = c.benchmark_group("naive_hashmap_bisect");
    for window in [100i64, 1_000, 5_000] {
        group.throughput(Throughput::Elements(window as u64));
        group.bench_with_input(BenchmarkId::from_parameter(window), &window, |b, &window| {
            let mut map: HashMap<&str, Vec<Candle>> = HashMap::new();
            map.insert("BTC/USDT:1m", (0..10_000i64).map(make_candle).collect());
            b.iter(|| {
                let data = &map["BTC/USDT:1m"];
                let lo   = data.partition_point(|c| c.ts < 0);
                let hi   = data.partition_point(|c| c.ts <= window);
                let result: Vec<Candle> = data[lo..hi].to_vec();
                black_box(result);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_append,
    bench_range_query,
    bench_lru_eviction,
    bench_naive_vec_range,
    bench_naive_hashmap_range,
    bench_naive_hashmap_bisect,
);
criterion_main!(benches);
