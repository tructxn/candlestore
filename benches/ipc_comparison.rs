//! IPC throughput and latency: std::sync::mpsc vs lock-free SPSC ring.
//!
//! Run with:  cargo bench --bench ipc_comparison
//!
//! ## Design notes
//!
//! ### Throughput
//! Ring capacity (1024) is much smaller than N (1_000_000). This forces the
//! writer and reader to run concurrently — the writer blocks when the ring is
//! full, the reader unblocks it. This is the realistic workload SPSC is built
//! for. A ring larger than N lets the writer drain ahead of the reader,
//! turning a concurrent test into a sequential write-then-read test where
//! mpsc's dynamic allocation actually wins on cache behaviour.
//!
//! ### Latency
//! We measure wall-clock time for N round-trips (writer sends one, reader
//! confirms receipt via a back-channel) then divide by N. This avoids the
//! clock-resolution noise of stamping every message with SystemTime, which
//! has ~1 µs resolution on macOS despite the nanosecond API.
//!
//! For mpsc we use `sync_channel(0)` — a rendezvous channel that blocks the
//! sender until the receiver has taken the value. This forces true one-at-a-time
//! handoff, identical to the SPSC ring-capacity-1 case, making the comparison fair.
//!
//! ### Pipeline (SHM → CandleStore)
//! Measures the full production data path: ShmRingWriter.push() → SHM ring →
//! ShmIngester.try_pop() → CandleStore.append(). Compare against direct
//! CandleStore.append() to isolate IPC overhead.
//!
//! Both use the same CandleStore::with_capacity(1, N+1) so the ring holds
//! all N candles. Timing runs from the first write until store.candle_count()
//! reaches N (spin wait — no sleep, no syscall in the hot path).

use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use candlestore::{Candle, CandleStore, SpscRing, SpscWriter, SpscReader};
#[cfg(any(target_os = "macos", target_os = "linux"))]
use candlestore::{ShmRingWriter, ShmRingReader, ShmIngester};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

// ── constants ────────────────────────────────────────────────────────────────

/// Total messages per benchmark iteration. Large enough to amortise thread
/// spawn (~50 µs on macOS) to < 0.005% of measurement time.
const N_THROUGHPUT: usize = 1_000_000;

/// Messages per latency round-trip iteration. Smaller so criterion can run
/// many samples quickly.
const N_LATENCY: usize = 10_000;

/// Ring capacity: small so writer and reader run concurrently.
/// Must be a power of two. 1024 × 48 B = 48 KB — fits in L2.
const RING_CAP: usize = 1024;

/// Pipeline benchmark: messages per iteration.
/// Small enough that the store ring (N+1 × 48B ≈ 480 KB) fits in L3 so
/// store overhead is cache-resident and IPC cost is isolated cleanly.
const N_PIPELINE: usize = 10_000;

/// SHM ring capacity for pipeline benchmark. Must be a power of two.
/// Smaller than N_PIPELINE so writer and ingester run concurrently.
const PIPELINE_RING_CAP: usize = 4096;

/// POSIX shm segment name for the pipeline benchmark.
#[cfg(any(target_os = "macos", target_os = "linux"))]
const PIPELINE_SHM: &str = "/cs_bench_pipeline";

/// Symbol name used in pipeline benchmarks.
const BENCH_SYM: &str = "BENCH";

fn make_candle(i: usize) -> Candle {
    Candle { ts: i as i64, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1.0 }
}

// ── Throughput ───────────────────────────────────────────────────────────────
//
// Both writer and reader run in separate threads. Ring capacity (1024) forces
// them to interleave — writer blocks when full, reader unblocks it.
// We measure the wall-clock time for the reader to consume all N messages.

fn run_spsc_throughput(n: usize) -> Duration {
    let (w, r): (SpscWriter, SpscReader) = SpscRing::new(RING_CAP);
    let writer = std::thread::spawn(move || {
        for i in 0..n { w.push(make_candle(i)); }
    });
    let start = Instant::now();
    for _ in 0..n { let _ = r.pop(); }
    let elapsed = start.elapsed();
    writer.join().unwrap();
    elapsed
}

fn run_mpsc_throughput(n: usize) -> Duration {
    let (tx, rx) = mpsc::channel::<Candle>();
    let writer = std::thread::spawn(move || {
        for i in 0..n { tx.send(make_candle(i)).unwrap(); }
    });
    let start = Instant::now();
    for _ in 0..n { let _ = rx.recv().unwrap(); }
    let elapsed = start.elapsed();
    writer.join().unwrap();
    elapsed
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipc_throughput");
    group.throughput(Throughput::Elements(N_THROUGHPUT as u64));
    group.measurement_time(Duration::from_secs(15));

    // warm up the thread pool
    let _ = run_spsc_throughput(1024);
    let _ = run_mpsc_throughput(1024);

    group.bench_function("spsc_ring_cap1024", |b| {
        b.iter_custom(|iters| {
            (0..iters).map(|_| run_spsc_throughput(N_THROUGHPUT)).sum()
        });
    });

    group.bench_function("mpsc_channel", |b| {
        b.iter_custom(|iters| {
            (0..iters).map(|_| run_mpsc_throughput(N_THROUGHPUT)).sum()
        });
    });

    group.finish();
}

// ── Latency ──────────────────────────────────────────────────────────────────
//
// Measure wall-clock time for N sequential one-at-a-time handoffs and divide
// by N. Using clock timestamps per-message introduces ~1 µs noise from
// SystemTime resolution on macOS. Wall-clock / N gives the true average.
//
// SPSC: ring capacity = 1 (rendezvous — writer blocks after every push until
//       reader pops, identical to sync_channel(0)).
// mpsc: sync_channel(0) — sender blocks until receiver takes the value.

fn bench_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipc_latency");
    group.throughput(Throughput::Elements(N_LATENCY as u64));
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("spsc_ring_cap1_rendezvous", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // capacity=1: writer must wait for reader to pop before next push
                let (w, r): (SpscWriter, SpscReader) = SpscRing::new(1);
                let writer = std::thread::spawn(move || {
                    for i in 0..N_LATENCY { w.push(make_candle(i)); }
                });
                let start = Instant::now();
                for _ in 0..N_LATENCY { let _ = r.pop(); }
                total += start.elapsed() / N_LATENCY as u32;
                writer.join().unwrap();
            }
            total
        });
    });

    group.bench_function("mpsc_sync_channel_rendezvous", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                // sync_channel(0): sender blocks until receiver takes the value
                let (tx, rx) = mpsc::sync_channel::<Candle>(0);
                let writer = std::thread::spawn(move || {
                    for i in 0..N_LATENCY { tx.send(make_candle(i)).unwrap(); }
                });
                let start = Instant::now();
                for _ in 0..N_LATENCY { let _ = rx.recv().unwrap(); }
                total += start.elapsed() / N_LATENCY as u32;
                writer.join().unwrap();
            }
            total
        });
    });

    group.finish();
}

// ── Pipeline (IPC overhead on CandleStore) ───────────────────────────────────
//
// Direct: store.append() called in a tight loop — no IPC, no extra thread.
// Pipeline: writer pushes to SHM ring; ShmIngester pops and calls append().
//
// Both use the same store ring size (N_PIPELINE+1) so every candle is retained
// and we can spin on candle_count() to detect completion without sleeping.
// Timing covers only the "first push → last candle in store" window.

fn run_direct_append(n: usize) -> Duration {
    let store = CandleStore::with_capacity(1, n + 1);
    let start = Instant::now();
    for i in 0..n { store.append(BENCH_SYM, make_candle(i)); }
    start.elapsed()
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn run_pipeline(n: usize) -> Duration {
    let store = Arc::new(CandleStore::with_capacity(1, n + 1));
    let writer = ShmRingWriter::create(PIPELINE_SHM, PIPELINE_RING_CAP)
        .expect("create shm writer");
    let reader = ShmRingReader::open(PIPELINE_SHM, PIPELINE_RING_CAP)
        .expect("open shm reader");
    let _ingester = ShmIngester::start(reader, Arc::clone(&store), BENCH_SYM);

    let start = Instant::now();
    for i in 0..n { writer.push(make_candle(i)); }
    while store.candle_count(BENCH_SYM) < n { std::hint::spin_loop(); }
    start.elapsed()
    // _ingester: stop+join on drop; writer: munmap+shm_unlink on drop
}

fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipc_pipeline");
    group.throughput(Throughput::Elements(N_PIPELINE as u64));
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("direct_append", |b| {
        b.iter_custom(|iters| {
            (0..iters).map(|_| run_direct_append(N_PIPELINE)).sum()
        });
    });

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    group.bench_function("pipeline_shm_ingester", |b| {
        b.iter_custom(|iters| {
            (0..iters).map(|_| run_pipeline(N_PIPELINE)).sum()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput, bench_latency, bench_pipeline);
criterion_main!(benches);
