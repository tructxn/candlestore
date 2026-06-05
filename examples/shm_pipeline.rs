//! Full SHM ingestion pipeline demo.
//!
//! Usage:
//!   cargo run --release --example shm_pipeline
//!
//! Architecture:
//!
//!   [feed thread]  ──SHM ring──►  [ShmIngester thread]  ──►  CandleStore  ──►  range()
//!
//! In production the feed thread lives in a separate process (`shm_writer`).
//! This demo runs both sides in one process; the SHM ring is still real.
//!
//! The store is configured with ring_capacity = N so all candles are retained.
//! In production, size the ring to your actual lookback window (L3-fit default
//! auto-tunes to ~2,800 candles per symbol on M-series).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use candlestore::{Candle, CandleStore, ShmIngester, ShmRingReader, ShmRingWriter};

const SHM_NAME:   &str  = "/candlestore_pipeline";
const N:          usize = 50_000;
const SHM_CAP:    usize = 65_536;  // SHM ring capacity (power of two, > N)
const STORE_CAP:  usize = N + 1;   // ring_buffer capacity: hold all N candles
const SYMBOL:     &str  = "BTCUSDT:1m";

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn main() {
    // ── 1. Store with explicit ring capacity ──────────────────────────────────
    //    with_capacity(max_symbols, ring_capacity)
    let store = Arc::new(CandleStore::with_capacity(1, STORE_CAP));
    println!("store ready  (1 symbol, ring_cap = {STORE_CAP})");

    // ── 2. Create SHM ring ────────────────────────────────────────────────────
    let writer = ShmRingWriter::create(SHM_NAME, SHM_CAP)
        .expect("failed to create shm writer");
    let reader = ShmRingReader::open(SHM_NAME, SHM_CAP)
        .expect("failed to open shm reader");

    // ── 3. Start ingester  ────────────────────────────────────────────────────
    let mut ingester = ShmIngester::start(reader, Arc::clone(&store), SYMBOL);
    println!("ingester running  →  {SHM_NAME} → store[{SYMBOL}]");

    // ── 4. Feed thread writes N candles via SHM ring ──────────────────────────
    let ts_start   = now_nanos();
    let wall_start = Instant::now();

    for _ in 0..N {
        writer.push(Candle {
            ts:     now_nanos(),
            open:   50_000.0,
            high:   50_100.0,
            low:    49_900.0,
            close:  50_050.0,
            volume: 1.0,
        });
    }

    let write_time = wall_start.elapsed();
    let ts_end     = now_nanos();
    println!(
        "wrote   {N} candles in {:.3}s  →  {:.0} candles/sec",
        write_time.as_secs_f64(),
        N as f64 / write_time.as_secs_f64(),
    );

    // ── 5. Wait for ingester to drain the remaining ring buffer ───────────────
    //    At 77 ns/msg the ring drains in at most SHM_CAP × 77 ns ≈ 5 ms.
    //    100 ms is comfortable headroom.
    std::thread::sleep(Duration::from_millis(100));

    // ── 6. Stop ingester and drop writer (shm_unlink) ─────────────────────────
    ingester.stop();
    drop(writer);

    let e2e = wall_start.elapsed();

    // ── 7. Query the store ────────────────────────────────────────────────────
    let candles = store.range(SYMBOL, ts_start - 1, ts_end + 1_000_000_000);
    let count   = candles.len();

    println!(
        "ingested {count} / {N} candles  (e2e {:.3}s from first write)",
        e2e.as_secs_f64(),
    );

    if count > 0 {
        let avg_close = candles.iter().map(|c| c.close).sum::<f64>() / count as f64;
        let min_ts    = candles.first().unwrap().ts;
        let max_ts    = candles.last().unwrap().ts;

        println!("\n── store snapshot ──────────────────────────────────");
        println!("  symbol    : {SYMBOL}");
        println!("  candles   : {count}");
        println!("  ts span   : {:.3} ms", (max_ts - min_ts) as f64 / 1e6);
        println!("  avg close : {avg_close:.2}");
    }

    assert_eq!(count, N, "all {N} candles should be in the store");
    println!("\nOK — all {N} candles confirmed in store.");
}
