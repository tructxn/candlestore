# candlestore

[![CI](https://github.com/tructxn/candlestore/actions/workflows/ci.yml/badge.svg)](https://github.com/tructxn/candlestore/actions/workflows/ci.yml)

A trading system kernel written in Rust. Lock-free SPSC rings carry market data
from exchange feed handlers to a hardware-tuned time-series store, then signals
from a strategy engine to an order executor. Each component runs on a dedicated
CPU core. No heap allocation, no kernel involvement, no GC on the hot path.

```
core 0  feed_handler   exchange WebSocket → ShmRingWriter  (77 ns/msg)
core 1  ingester       ShmRingReader → CandleStore          (19M candles/sec)
core 2  strategy       CandleStore → SpscRing<Signal>       (SMA crossover)
core 3  executor       SpscReader<Signal> → order tracker
```

---

## Quick Start

```bash
# terminal 1 — feed handler, pinned to core 0
cargo run --release --bin feed_handler

# terminal 2 — market hub (ingest + strategy + executor, cores 1–3)
cargo run --release --bin market_hub
```

You will see signals flow end-to-end as the SMA(10/20) crossover fires:

```
[strategy] signal #3  Buy   qty=0.1  SMA10=48311.20  SMA20=48278.20
[executor] #   3  Buy   qty=0.1000  symbol=BTCUSDT:1m  position=0.1000
[strategy] signal #4  Sell  qty=0.1  SMA10=51752.57  SMA20=51787.03
[executor] #   4  Sell  qty=0.1000  symbol=BTCUSDT:1m  position=0.0000
```

---

## Architecture

```
┌────────────────────── Process: feed_handler ────────────────────────┐
│  Exchange WebSocket ──► parse ──► ShmRingWriter("/tradekern_feed")  │
│  (GBM simulator in demo)             77 ns/push, no syscall         │
│  core 0 — Linux: sched_setaffinity / macOS: affinity tag            │
│  SIGINT/SIGTERM → shm_unlink → clean restart                        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  POSIX shm_open / mmap
                               │  same physical pages, two VA spaces
                               ▼
┌────────────────────── Process: market_hub ──────────────────────────┐
│                                                                      │
│  core 1 ── ingester ─────────────────────────────────────────────┐  │
│            ShmRingReader::try_pop() → CandleStore::append()      │  │
│            store.version.fetch_add(1, Release)                   │  │
│            19M candles/sec end-to-end (SHM + store overhead)     │  │
│                                    │                              │  │
│                              CandleStore                          │  │
│                              per-symbol RwLock — BTC ≠ ETH       │  │
│                              L3-tuned ring per symbol             │  │
│                              LRU evict → Parquet (versioned)      │  │
│                                    │                              │  │
│  core 2 ── strategy ───────────────┘                             │  │
│            store.wait_for_change() ── 14µs p50 wake-up           │  │
│            store.last_n(21) ── ~80 ns                            │  │
│            SMA crossover → Signal                                │  │
│            SpscWriter<Signal> ──────────────────────────────┐    │  │
│                                                             │    │  │
│  core 3 ── executor ────────────────────────────────────────┘    │  │
│            SpscReader<Signal>::try_pop() → position tracker       │  │
│                                                                   │  │
│  /metrics:9091 ◄── 1Hz poller ── store.snapshot() + ingester.stats() │
└──────────────────────────────────────────────────────────────────┘  │
                                                                       │
```

Every inter-component boundary is an SPSC atomic ring. No mutexes, no condition
variables, no OS scheduler involvement on the hot path.

Full design doc: **[ARCHITECTURE.md](ARCHITECTURE.md)**

---

## Performance

All numbers measured on Apple M-series (10 physical cores, 4 MB L3), release build.

### Hot path latency

| Stage                                  | Latency    |
|----------------------------------------|------------|
| ShmRingWriter::push (feed → ring)      | 31 ns      |
| SHM ring transit (cross-process)       | 77 ns      |
| CandleStore::append (direct)           | 19 ns      |
| SHM pipeline end-to-end (ring + store) | 52 ns/msg  |
| CandleStore::range (W=1,000, L3-hot)   | 1.0 µs     |
| CandleStore::last_n (N=100)            | 110 ns     |
| Strategy wake-up (wait_for_change p50) | 14 µs      |
| SpscRing\<Signal\> push                | 77 ns      |

### Ingestion throughput

| Path                                | Throughput         |
|-------------------------------------|--------------------|
| Direct `store.append()` (1 thread)  | **53M candles/sec** |
| Direct `store.append()` (4 sym × 4 t) | **6.2M ops/sec/thread** (1.59× parallelism) |
| SHM pipeline (ring → ingester)      | **19M candles/sec** |
| SPSC ring only (raw IPC)            | **28M msg/sec**    |

### IPC: SPSC ring vs mpsc channel

|                        | SPSC ring  | std mpsc    | SPSC advantage |
|------------------------|------------|-------------|----------------|
| Latency (rendezvous)   | **77 ns**  | 1,300 ns    | **17×**        |
| Bulk throughput        | 28M msg/s  | **58M msg/s** | mpsc 2×      |

SPSC wins on latency (no kernel, pure atomics). mpsc wins on bulk throughput
(unbounded queue, writer never stalls). For a tick-by-tick trading system,
latency is the right metric.

### Range queries (10k candles pre-loaded, RwLock, binary search)

| Window | candlestore | Vec filter | Vec bisect¹ |
|--------|-------------|------------|-------------|
| 100    | **210 ns**  | 5.6 µs     | 170 ns      |
| 1,000  | **1.01 µs** | 7.9 µs     | 950 ns      |
| 5,000  | **4.32 µs** | 23 µs      | 4.6 µs      |

candlestore matches `Vec + partition_point` within noise while also providing
concurrent `RwLock` reads, O(1) append without reallocation, LRU eviction across
100+ symbols, and hardware-aware ring sizing.

For the strategy's actual access pattern — "give me the last N candles" — a
dedicated `last_n` path skips the binary search entirely: **65 ns at N=10,
108 ns at N=100**.

¹ Pre-sorted `Vec` + `partition_point` — the fastest single-symbol,
single-threaded, always-in-memory baseline.

### vs. Standalone Databases

| Database            | Ingestion          | Query latency   | Overhead         |
|---------------------|--------------------|-----------------|------------------|
| **candlestore**     | **~32M rows/sec**  | **~24–31 µs**   | Zero (embedded)  |
| QuestDB             | ~11M rows/sec²     | ~1 ms²          | TCP + SQL parser |
| InfluxDB 3.0        | ~320K rows/sec³    | ~10 ms³         | TCP + SQL parser |
| TimescaleDB         | ~500K rows/sec⁴    | ~10–50 ms⁴      | TCP + SQL parser |

The embedded design eliminates the network hop, SQL parser, and query planner.
Trade-off: no multi-node replication, no ad-hoc joins, single-process only.

² QuestDB TSBS benchmarks / documentation  
³ QuestDB blog: "We finally benchmarked InfluxDB 3 OSS Core"  
⁴ TimescaleDB community benchmarks  

Full benchmark report + methodology: **[BENCHMARKS.md](BENCHMARKS.md)**

---

## Library Usage

### Rust

```rust
use candlestore::{Candle, CandleStore, AppendError};

let store = CandleStore::from_hardware(10)   // L3-tuned, 10 symbols
    .with_data_dir("/tmp/candles");           // cold spill to Parquet

// Fire-and-forget — logs + increments `appends_rejected_total` on spill failure
store.append("BTCUSDT:1m", Candle {
    ts: 1_700_000_000_000,
    open: 50_000.0, high: 50_200.0, low: 49_800.0,
    close: 50_100.0, volume: 1.5,
});

// Strict — returns Err if eviction's Parquet spill failed. On error the
// store state is unchanged (existing data preserved, new candle dropped).
match store.try_append("BTCUSDT:1m", candle) {
    Ok(())                                          => {}
    Err(AppendError::EvictionSpillFailed { .. })    => halt_ingestion(),
}

let candles = store.range("BTCUSDT:1m", from_ts, to_ts);
```

### SHM pipeline (cross-process)

```rust
// ── feed handler process ──────────────────────────────────────────────
use candlestore::{ShmRingWriter, pin_to_core};

pin_to_core(0);
let writer = ShmRingWriter::create("/my_feed", 65536)?;
loop { writer.push(fetch_from_exchange()); }  // 77 ns, no syscall

// ── market hub process ────────────────────────────────────────────────
use std::sync::Arc;
use candlestore::{CandleStore, ShmRingReader, ShmIngester, SpscRing, Signal};

let store   = Arc::new(CandleStore::from_hardware(10));
let reader  = ShmRingReader::open("/my_feed", 65536)?;
let _ingest = ShmIngester::start(reader, Arc::clone(&store), "BTCUSDT:1m");

// Signal bus: strategy thread → executor thread
let (sig_tx, sig_rx) = SpscRing::<Signal>::new(1024);
```

### Go (via cgo)

```go
import cs "github.com/tructxn/candlestore-go/candlestore"

store := cs.NewHardware(10)
defer store.Close()
store.Append("BTCUSDT:1m", cs.Candle{Ts: 1_700_000_000_000, Close: 50_100.0})
candles := store.Range("BTCUSDT:1m", fromTs, toTs)
```

---

## Hardware-Aware Tuning

Ring buffer capacity is derived from the machine's L3 cache at startup:

```rust
// Shared machine — uses 1/3 of L3 (default)
let store = CandleStore::from_hardware(10);

// Dedicated server — uses full L3
let store = CandleStore::from_hardware_dedicated(10);

// Manual fraction
use candlestore::HardwareProfile;
let hw    = HardwareProfile::detect().with_fraction(0.5);
let store = CandleStore::with_capacity(10, hw.ring_capacity_for(10));
```

| Machine            | L3     | Usable (1/3) | Candles/symbol @ 10 sym |
|--------------------|--------|--------------|--------------------------|
| Apple M2 Pro       | 4 MB   | 1.3 MB       | ~2,900                   |
| AWS c6i.4xlarge    | 8 MB   | 2.7 MB       | ~5,800                   |
| AWS c6i.32xlarge   | 56 MB  | 18.7 MB      | ~40,800                  |

When all active symbols fit in L3, range scans run at **~160M elem/sec**. When the
ring overflows L3, every scan element causes a DRAM miss — **23× slower**. Size
`max_symbols` to exceed your active symbol count and eviction never occurs.

---

## Configuration

Both binaries are configured via environment variables:

| Variable        | Default              | Description                         |
|-----------------|----------------------|-------------------------------------|
| `FEED_SHM_NAME` | `/tradekern_feed`    | POSIX shm segment name              |
| `FEED_SHM_CAP`  | `65536`              | Ring capacity (must be power of two)|
| `FEED_CORE`     | `0`                  | CPU core for feed_handler           |
| `FEED_SYMBOL`   | `BTCUSDT:1m`         | Symbol pushed to ring               |
| `FEED_RATE`     | `10000`              | Candles/sec (simulator)             |
| `HUB_CORE`      | `1`                  | First core for market_hub threads   |
| `SMA_SHORT`     | `10`                 | Short SMA period                    |
| `SMA_LONG`      | `20`                 | Long SMA period                     |
| `SIGNAL_QTY`    | `0.1`                | Quantity per signal                 |
| `METRICS_PORT`  | `9090` / `9091`      | Prometheus HTTP port (feed / hub)   |
| `RUST_LOG`      | `info`               | `tracing` filter (e.g. `debug,hyper=warn`) |

---

## Features

| Feature                       | Default | Enable with        |
|-------------------------------|---------|--------------------|
| Core storage + Parquet spill  | ✅      | always             |
| Hardware detection            | ✅      | always             |
| SHM SPSC rings + ShmIngester  | ✅      | always (Linux/mac) |
| CPU affinity                  | ✅      | always             |
| Signal bus + Side type        | ✅      | always             |
| `tracing` structured logging  | ✅      | always (lib emits) |
| Prometheus `/metrics` endpoint | ✅     | binaries only      |
| Go / C FFI                    | ✅      | always (cdylib)    |
| Binance WebSocket feed        | ❌      | `--features feed`  |

## Observability

Both binaries publish structured `tracing` logs (filter via `RUST_LOG`) and
serve Prometheus metrics over HTTP (configurable via `METRICS_PORT`):

```bash
$ curl -s localhost:9091/metrics | grep candlestore
# HELP candlestore_appends_total Total candles appended to the store...
candlestore_appends_total 206054
# HELP candlestore_ingest_popped_total Lifetime messages popped from the SHM ring...
candlestore_ingest_popped_total 206054
candlestore_signals_total 200
candlestore_executor_signals_total 193
candlestore_executor_position -0.1
candlestore_ingest_ring_depth 0
candlestore_ingest_ring_fill_ratio 0
candlestore_symbols_active 1
candlestore_evictions_total 0
candlestore_parquet_spill_bytes_total 0
candlestore_parquet_spill_errors_total 0
candlestore_appends_rejected_total 0
```

When `candlestore_appends_rejected_total` is nonzero, the store is dropping
new candles to preserve existing data because Parquet spills are failing.
This means disk is full, permissions changed, or the filesystem is in error
— investigate immediately.

The library NEVER emits metrics on the hot path — that would cost ~26% of
throughput at 53M append/s. Instead, the `CandleStore` and `ShmIngester` keep
internal `AtomicU64` counters and expose them via `snapshot()` / `stats()`.
The binary's metrics-poller thread reads these once per second and updates the
Prometheus exporter. Rare events (Parquet spill failures, fill > 50%) are
logged inline via `tracing::error!` / `tracing::warn!`.

## Parquet schema versioning

Every cold-storage file carries Arrow schema metadata identifying the writer:

```
candlestore.brand           = "candlestore"
candlestore.schema_version  = "1"
```

Read-side behavior (see `parquet::check_schema_compat`):

| File state                                           | Read result            |
|------------------------------------------------------|------------------------|
| Brand + version == current                           | Read normally          |
| Brand + version < current (future migration path)    | Read normally          |
| Brand + version > current                            | `SpillError::IncompatibleVersion` — never panic |
| No brand metadata (pre-versioning v0 files)          | Read as v0 (column-compatible with v1) |
| Required column missing                              | `SpillError::MissingColumn` |
| Required column has wrong type                       | `SpillError::WrongColumnType` |
| Extra unknown columns present (future v2+ writer)    | Ignore them, read what we recognise |

The previous implementation used positional column lookup (`column(0)`,
`column(1)`, …) with `unwrap()` — adding a field to `Candle` or producing a
corrupted file would crash the process. The new path looks columns up by
name and returns structured errors, so `query_cold` can skip an unreadable
file (logged via `tracing::warn!`) and still return the readable history.

When adding a field to `Candle`, bump `parquet::SCHEMA_VERSION` to `2`,
add the column at the END of the schema as nullable, and existing v1 files
remain readable.

## Graceful shutdown

Both binaries handle `SIGINT` (Ctrl-C) and `SIGTERM` cleanly:

- `feed_handler`: stops pushing, drops the `ShmRingWriter` → `munmap` +
  `shm_unlink` so the next start sees a fresh segment. If the consumer is
  behind and the ring is full when the signal arrives, the writer exits
  immediately rather than spinning until ring space is available.
- `market_hub`: signals all worker threads (strategy via
  `CandleStore::signal_waiters()`, executor via shared `AtomicBool`),
  joins them with a 5 s timeout, then drops the `ShmIngester` Arc which
  triggers `munmap` on the reader side.

Clean exit traces (one for each binary):

```
INFO shutdown signal received — initiating graceful shutdown
INFO strategy thread exiting on shutdown    signals=8664
INFO executor thread exiting on shutdown    executed=8664 final_position=0.0
INFO joined  thread="strategy"  elapsed_ms=0
INFO joined  thread="executor"  elapsed_ms=0
INFO joined  thread="metrics-poller"  elapsed_ms=0
INFO stopping ingester (will join its thread)
INFO graceful shutdown complete
```

```
INFO shutdown signal received — draining and unlinking SHM   total_pushed=65536
INFO SHM segment unlinked, feed_handler stopped
```

**Caveat**: in-flight data sitting in the SHM ring at the moment of shutdown
is lost. A drain-then-exit phase (writer stops pushing but consumer keeps
reading until empty) is a future improvement.

---

## Project Structure

```
src/
  candle.rs        Candle (#[repr(C)], 48 bytes, Default)
  ring_buffer.rs   Fixed-capacity O(1) ring, wrap-around, range query
  store.rs         CandleStore — RwLock, LRU eviction, L3-tuned rings
  parquet.rs       Evict to Parquet, range-aware filenames, cold reload
  hw.rs            HardwareProfile — L3 size, cache line, core count
  shm.rs           SpscRing<T>, ShmRingWriter/Reader, ShmIngester
  signal.rs        Signal (64-byte, Copy, repr(C)), Side enum
  affinity.rs      pin_to_core() — sched_setaffinity / Mach affinity tag
  ffi.rs           C ABI (#[no_mangle]) for cgo consumers
  matching/        Order book + paper trading engine
    book.rs          Price-time priority (Limit/Market/IOC/FOK orders)
    paper.rs         Candle-based fill simulation
    portfolio.rs     P&L, positions, avg cost basis

src/bin/
  feed_handler.rs  Exchange feed → ShmRingWriter, core 0
  market_hub.rs    Ingester + strategy + executor, cores 1–3

examples/
  shm_pipeline.rs  Single-process pipeline demo (50k candles, asserted)
  shm_writer.rs    Cross-process SHM writer (5M candles, throughput)
  shm_reader.rs    Cross-process SHM reader (p99 latency report)
  cache_ladder.rs  Drepper pointer-chase: L1/L2/L3/DRAM latency ladder
  binance_feed.rs  Live BTC/ETH/SOL feed (--features feed)
  paper_trade.rs   SMA(10/20) crossover paper trading strategy
  hw_probe.rs      Print detected HardwareProfile

go-client/         Go cgo wrapper + SMA strategy example
include/
  candlestore.h    C header for FFI consumers

benches/
  bench.rs             Baseline ingestion + range query throughput
  design_decisions.rs  Per-decision micro-benchmarks (5 decisions)
  ipc_comparison.rs    SPSC vs mpsc vs SHM pipeline (latency + throughput)
```

---

## Documentation

| Document                            | Contents                                              |
|-------------------------------------|-------------------------------------------------------|
| [ARCHITECTURE.md](ARCHITECTURE.md)  | Full design doc: IPC protocol, memory ordering, CPU affinity, latency budget, all design decisions with trade-offs |
| [BENCHMARKS.md](BENCHMARKS.md)      | All benchmark results, methodology, Criterion reports |
| [ROADMAP.md](ROADMAP.md)            | Phase-by-phase build history, current status          |
