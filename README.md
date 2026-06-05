# candlestore

Embeddable Rust library for financial OHLCV (candle) time-series data.

Hot symbols live in RAM inside a lock-free ring buffer. Cold symbols spill to Parquet on disk. No server, no GC pauses, no SQL overhead. Usable from Rust, C, and Go.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      CandleStore                        │
│                                                         │
│  "BTCUSDT:1m" ──► RingBuffer[Candle]  ◄── L3-tuned      │
│  "ETHUSDT:1m" ──► RingBuffer[Candle]      capacity      │
│  "SOLUSDT:1m" ──► RingBuffer[Candle]                    │
│        ...                                              │
│  LRU eviction ──► {data_dir}/symbol/*.parquet           │
│  Cache miss   ──► read Parquet + merge with hot         │
└─────────────────────────────────────────────────────────┘
         ▲                        ▲
  Binance WebSocket          Go / C FFI
  (--features feed)        (cgo wrapper)
```

---

## Benchmarks

> Full report with methodology, all raw numbers, confidence intervals, and
> architectural decision analysis: **[BENCHMARKS.md](BENCHMARKS.md)**

Measured on Apple M-series (10 physical cores, 4 MB L3), release build (`cargo bench`).

### Ingestion

| Operation              | candlestore     |
|------------------------|-----------------|
| Append (single symbol) | **~29M ops/sec** |

### Range Query (10k pre-loaded candles, concurrent-safe RwLock)

| Window   | candlestore | Naive Vec filter | Naive bisect¹ |
|----------|-------------|------------------|---------------|
| 100      | ~24 µs      | ~5.6 µs          | ~170 ns       |
| 1,000    | ~25 µs      | ~7.9 µs          | ~950 ns       |
| 5,000    | ~31 µs      | ~23 µs           | ~4.6 µs       |

¹ Sorted `Vec` + `partition_point` — fastest possible single-symbol, no-concurrency, in-memory baseline.

> **Why is bisect faster?** `partition_point` + `memcpy` on a pre-sorted contiguous `Vec` is O(log n).
> candlestore pays for what naive code cannot give you:
> - Concurrent reads under `RwLock` (readers never block each other)
> - O(1) append without reallocation (fixed ring buffer)
> - LRU eviction: 100+ symbols → oldest auto-spilled to Parquet, reloaded on miss
> - Hardware-aware sizing: capacity derived from actual L3 cache / symbols
>
> If you have one symbol, no concurrency, and never exceed RAM, `Vec + bisect` wins.
> Once you need multi-symbol management, cold storage, or concurrent writes, you need a store.

### vs. Standalone Databases (documented benchmarks)

| Database         | Ingestion          | Query latency  | Overhead          |
|------------------|--------------------|----------------|-------------------|
| **candlestore**  | **~29M rows/sec**  | **~24–31 µs**  | Zero (embedded)   |
| QuestDB          | ~11M rows/sec²     | ~1 ms²         | TCP + SQL parser  |
| InfluxDB 3.0     | ~320K rows/sec³    | ~10 ms³        | TCP + SQL parser  |
| TimescaleDB      | ~500K rows/sec⁴    | ~10–50 ms⁴     | TCP + SQL parser  |

> **Why so much faster than standalone databases?** No network hop, no SQL parser, no query planner.
> Ingestion is a ring buffer `push()`. Queries are a lock + linear scan on contiguous heap memory.
> The tradeoff: no multi-node replication, no ad-hoc joins, single-process only.

² QuestDB documentation / TSBS benchmarks  
³ QuestDB blog: "We finally benchmarked InfluxDB 3 OSS Core"  
⁴ TimescaleDB community benchmarks  

---

## Usage

### Rust

```rust
use candlestore::{Candle, CandleStore};

// Shared machine: uses 1/3 of L3 cache by default
let store = CandleStore::from_hardware(10)
    .with_data_dir("/tmp/candles");

store.append("BTCUSDT:1m", Candle {
    ts: 1_700_000_000_000,
    open: 50_000.0, high: 50_200.0, low: 49_800.0,
    close: 50_100.0, volume: 1.5,
});

let candles = store.range("BTCUSDT:1m", from_ts, to_ts);
```

### Go (via cgo)

```go
import cs "github.com/tructxn/candlestore-go/candlestore"

store := cs.NewHardware(10)
defer store.Close()

store.Append("BTCUSDT:1m", cs.Candle{Ts: 1_700_000_000_000, Close: 50_100.0, ...})
candles := store.Range("BTCUSDT:1m", fromTs, toTs)
```

---

## Hardware-Aware Tuning

candlestore auto-tunes ring buffer capacity to your machine's L3 cache.

```rust
// Shared machine (default) — uses 1/3 of L3
let store = CandleStore::from_hardware(10);

// Dedicated server — uses full L3
let store = CandleStore::from_hardware_dedicated(10);

// Custom fraction
use candlestore::HardwareProfile;
let hw    = HardwareProfile::detect().with_fraction(0.5);
let store = CandleStore::with_capacity(10, hw.ring_capacity_for(10));
```

| Machine          | L3     | Usable (1/3) | Candles/symbol @ 10 symbols |
|------------------|--------|-------------|------------------------------|
| Apple M2 Pro     | 4 MB   | 1.3 MB      | ~2,900                       |
| AWS c6i.4xlarge  | 8 MB   | 2.7 MB      | ~5,800                       |
| AWS c6i.32xlarge | 56 MB  | 18.7 MB     | ~40,800                      |

---

## Features

| Feature | Default | Enable with |
|---------|---------|-------------|
| Core storage | ✅ | always |
| Parquet cold spill | ✅ | always |
| Hardware detection | ✅ | always |
| Binance WebSocket feed | ❌ | `--features feed` |

---

## Project Structure

```
src/
  candle.rs       Candle struct (#[repr(C)], 48 bytes)
  ring_buffer.rs  Fixed-capacity O(1) ring buffer
  store.rs        CandleStore — LRU + per-symbol ring buffers
  parquet.rs      Cold spill to Parquet, range-aware file naming
  hw.rs           Hardware detection (L3, cache line, cores)
  ffi.rs          C ABI for Go/cgo
  matching/       Order book + paper trading engine
    book.rs       Price-time priority (Limit/Market/IOC/FOK)
    paper.rs      Candle-based paper trading simulation
    portfolio.rs  P&L tracking with avg cost basis

go-client/        Go wrapper + SMA crossover example
examples/
  binance_feed.rs Live BTC/ETH/SOL feed (requires --features feed)
  paper_trade.rs  SMA(10/20) crossover strategy
  hw_probe.rs     Print detected hardware profile
include/
  candlestore.h   C header for FFI consumers
```
