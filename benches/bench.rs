use candlestore::{Candle, CandleStore};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn make_candle(ts: i64) -> Candle {
    Candle { ts, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1000.0 }
}

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

fn bench_append_multi_symbol(c: &mut Criterion) {
    let symbols = ["BTC/USDT:1m", "ETH/USDT:1m", "SOL/USDT:1m", "BNB/USDT:1m"];
    c.bench_function("append_4_symbols_10k_each", |b| {
        b.iter(|| {
            let store = CandleStore::new(10);
            for i in 0..10_000u64 {
                for sym in &symbols {
                    store.append(black_box(sym), make_candle(i as i64));
                }
            }
        });
    });
}

fn bench_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");

    for window in [100i64, 1_000, 5_000] {
        group.throughput(Throughput::Elements(window as u64));
        group.bench_with_input(BenchmarkId::from_parameter(window), &window, |b, &window| {
            // pre-populate store outside the benchmark loop
            let store = CandleStore::new(10);
            for i in 0..10_000i64 {
                store.append("BTC/USDT:1m", make_candle(i));
            }
            b.iter(|| {
                black_box(store.range(black_box("BTC/USDT:1m"), 0, window));
            });
        });
    }
    group.finish();
}

fn bench_lru_eviction(c: &mut Criterion) {
    // stress LRU: 100 symbols into a store that fits 10
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

criterion_group!(
    benches,
    bench_append,
    bench_append_multi_symbol,
    bench_range_query,
    bench_lru_eviction,
);
criterion_main!(benches);
