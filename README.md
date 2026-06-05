# candlestore

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
└──────────────────────────────┬──────────────────────────────────────┘
                               │  POSIX shm_open / mmap
                               │  same physical pages, two VA spaces
                               ▼
┌────────────────────── Process: market_hub ──────────────────────────┐
│                                                                      │
│  core 1 ── ingester ─────────────────────────────────────────────┐  │
│            ShmRingReader::try_pop() → CandleStore::append()      │  │
│            19M candles/sec end-to-end (SHM + store overhead)     │  │
│                                    │                              │  │
│                              CandleStore                          │  │
│                              L3-tuned ring per symbol             │  │
│                              LRU eviction → Parquet               │  │
│                                    │                              │  │
│  core 2 ── strategy ───────────────┘                             │  │
│            store.range() → SMA crossover → Signal                │  │
│            SpscWriter<Signal> ──────────────────────────────┐    │  │
│                                                             │    │  │
│  core 3 ── executor ────────────────────────────────────────┘    │  │
│            SpscReader<Signal>::try_pop() → position tracker       │  │
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
| CandleStore::append (direct)           | 31 ns      |
| SHM pipeline end-to-end (ring + store) | 52 ns/msg  |
| CandleStore::range (W=1,000, L3-hot)   | 25 µs      |
| SpscRing\<Signal\> push                | 77 ns      |

### Ingestion throughput

| Path                             | Throughput         |
|----------------------------------|--------------------|
| Direct `store.append()`          | **32M candles/sec** |
| SHM pipeline (ring → ingester)   | **19M candles/sec** |
| SPSC ring only (raw IPC)         | **28M msg/sec**    |

### IPC: SPSC ring vs mpsc channel

|                        | SPSC ring  | std mpsc    | SPSC advantage |
|------------------------|------------|-------------|----------------|
| Latency (rendezvous)   | **77 ns**  | 1,300 ns    | **17×**        |
| Bulk throughput        | 28M msg/s  | **58M msg/s** | mpsc 2×      |

SPSC wins on latency (no kernel, pure atomics). mpsc wins on bulk throughput
(unbounded queue, writer never stalls). For a tick-by-tick trading system,
latency is the right metric.

### Range queries (10k candles pre-loaded, RwLock)

| Window | candlestore | Vec filter | Vec bisect¹ |
|--------|-------------|------------|-------------|
| 100    | 24 µs       | 5.6 µs     | 170 ns      |
| 1,000  | 25 µs       | 7.9 µs     | 950 ns      |
| 5,000  | 31 µs       | 23 µs      | 4.6 µs      |

¹ Pre-sorted `Vec` + `partition_point` — fastest single-symbol, single-threaded,
always-in-memory baseline. candlestore pays for concurrent RwLock reads, O(1)
append without reallocation, LRU eviction across 100+ symbols, and hardware-aware
ring sizing. For those requirements, you need a store.

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
use candlestore::{Candle, CandleStore};

let store = CandleStore::from_hardware(10)   // L3-tuned, 10 symbols
    .with_data_dir("/tmp/candles");           // cold spill to Parquet

store.append("BTCUSDT:1m", Candle {
    ts: 1_700_000_000_000,
    open: 50_000.0, high: 50_200.0, low: 49_800.0,
    close: 50_100.0, volume: 1.5,
});

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

---

## Features

| Feature                       | Default | Enable with        |
|-------------------------------|---------|--------------------|
| Core storage + Parquet spill  | ✅      | always             |
| Hardware detection            | ✅      | always             |
| SHM SPSC rings + ShmIngester  | ✅      | always (Linux/mac) |
| CPU affinity                  | ✅      | always             |
| Signal bus + Side type        | ✅      | always             |
| Go / C FFI                    | ✅      | always (cdylib)    |
| Binance WebSocket feed        | ❌      | `--features feed`  |

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
