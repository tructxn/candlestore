# candlestore — Architecture

This document explains the design of the trading system kernel built on top of
`candlestore`. It covers the full data path from exchange tick to executed order,
the IPC mechanisms between components, CPU affinity rationale, and the measured
performance at each stage.

---

## 1. System Overview

```
┌─────────────────────── Process: feed_handler ───────────────────────┐
│                                                                      │
│  Exchange WebSocket ──► parse ──► ShmRingWriter("/tradekern_feed")  │
│  (or GBM simulator)                  77 ns/push, no kernel          │
│                                                                      │
│  Pinned: core 0  (Linux: hard / macOS: affinity tag)                │
└──────────────────────────────┬───────────────────────────────────────┘
                               │  POSIX shared memory
                               │  mmap'd into both processes
                               │  SPSC atomic ring (head/tail AtomicU64)
                               ▼
┌─────────────────────── Process: market_hub ─────────────────────────┐
│                                                                      │
│  ┌── Thread: ingester (core 1) ─────────────────────────────────┐   │
│  │  ShmRingReader::try_pop()  ──►  CandleStore::append()        │   │
│  │  77 ns/msg · 19M candles/sec end-to-end                      │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                        │                                             │
│                 CandleStore (shared, Arc<RwLock>)                   │
│                 LRU ring buffers, L3-tuned capacity                  │
│                        │                                             │
│  ┌── Thread: strategy (core 2) ─────────────────────────────────┐   │
│  │  store.range()  ──►  SMA(10/20) crossover  ──►  Signal       │   │
│  │  SpscWriter<Signal> ──► in-process SPSC ring ──────────────┐ │   │
│  └────────────────────────────────────────────────────────────│─┘   │
│                                                               │      │
│  ┌── Thread: executor (core 3) ──────────────────────────────┐│     │
│  │  SpscReader<Signal>::try_pop()                            ││     │
│  │  ──► risk check ──► position update ──► order submit     ◄┘│     │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

Each arrow on the hot path is either a shared-memory SPSC ring or an in-process
SPSC ring. There are no mutexes, no condition variables, and no OS scheduler
involvement on any message-passing boundary.

---

## 2. Why Separate Processes

The feed handler and market hub run in separate OS processes by design.

**Fault isolation.** A strategy bug, a slow GC pause (if you embed a JVM), or an
OOM in the strategy process cannot crash the feed handler. The feed keeps running
and accumulating data. When the strategy restarts, it opens the SHM ring and
catches up immediately.

**Independent deployment.** You can restart, upgrade, or replace the strategy
without any feed downtime. This is the production model used in HFT shops: the
market data infrastructure is managed separately from the alpha generation layer.

**Security boundary.** The feed handler can run with minimal capabilities
(network access to exchange IPs only, no disk write). The strategy process gets
disk access for Parquet cold storage, but no network. Different processes, different
privilege sets.

The cost of process separation is the SHM ring hop (~77 ns). That is acceptable
because the alternative — a TCP socket or Unix socket — costs 1–50 µs per message
(kernel copy + scheduler wakeup + copy back). SHM avoids all of that: the kernel
just maps the same physical pages into both address spaces. The ring protocol is
pure user-space atomics.

---

## 3. IPC Mechanism: POSIX Shared Memory SPSC Ring

### Layout

```
SHM segment layout (offset from base pointer):

  offset   0: ShmHeader (384 bytes, 3 × 128-byte cache lines)
    line 0 (  0): ready:    AtomicU64  ← READY_MAGIC when writer is initialised
                  capacity: u64
                  _pad:     [u8; 112]
    line 1 (128): head:     AtomicU64  ← producer cursor (written by writer only)
                  _pad:     [u8; 120]
    line 2 (256): tail:     AtomicU64  ← consumer cursor (written by reader only)
                  _pad:     [u8; 120]

  offset 384: Candle[capacity]  ← ring slots
```

**Why three separate cache lines for head and tail?**

On Apple M-series (128-byte cache lines) and x86 (64-byte), the writer and reader
run on different cores. If `head` and `tail` share a cache line, every write by the
producer invalidates the reader's cache line (MESI protocol: the line transitions
from Shared to Invalid on the other core). This is called **false sharing** and
adds a full cache-coherence round trip (~50–150 ns on M-series) to every operation.

Padding `head` and `tail` to separate cache lines means each core owns its cursor
exclusively. The writer loads `tail` once to check for space, writes the slot, then
stores `head`. The reader loads `head` once to check for data, reads the slot, then
stores `tail`. Cache lines never bounce.

### Memory ordering

The SPSC protocol requires exactly two ordering guarantees:

```
Writer:                              Reader:
  head_local = head.load(Relaxed)      tail_local = tail.load(Relaxed)
  tail_seen  = tail.load(Acquire)  ←── head_seen  = head.load(Acquire)
  slots[head % cap] = item             item = slots[tail % cap]
  head.store(head+1, Release)  ──────► tail.store(tail+1, Release)
```

- `Release` store by the writer publishes the slot contents to any thread that
  subsequently does an `Acquire` load of `head`.
- `Acquire` load by the reader on `head` synchronises with the writer's `Release`
  store — the slot read is guaranteed to see the value the writer wrote.

No `SeqCst`, no fences, no locks. Two atomic operations per message transfer.

### Initialisation fence

```
Writer: writes capacity, zeroes all slots, stores READY_MAGIC with Release
Reader: spins on ready.load(Acquire) == READY_MAGIC before accessing any field
```

This ensures the reader never dereferences a partially-initialised header. The
`Release`/`Acquire` pair on `ready` is a full happens-before edge.

---

## 4. In-Process Signal Bus: SpscRing\<Signal\>

Strategy and executor communicate via a heap-backed `SpscRing<Signal>`. The
protocol is identical to the SHM ring but uses `Arc<SpscInner<T>>` instead of
`mmap`. Since they live in the same process, there is no kernel involvement at all.

### Signal wire format (64 bytes)

```
offset  0: ts:       i64      unix nanoseconds
offset  8: symbol:   [u8; 24] null-terminated ASCII, e.g. "BTCUSDT:1m\0"
offset 32: side:     u8       0 = Buy, 1 = Sell
offset 33: _pad:     [u8; 7]  alignment
offset 40: qty:      f64      base quantity
offset 48: price:    f64      limit price; 0.0 = market order
offset 56: strategy: u32      audit ID
offset 60: _pad2:    [u8; 4]
```

64 bytes = one x86 cache line = half an M-series cache line. A single SPSC push
writes the Signal in one aligned store. The ring capacity (1024 slots = 64 KB) fits
entirely in L2 cache, so signal bus latency is memory-hierarchy-bounded, not
network-bounded.

---

## 5. Market Data Store: CandleStore

### Per-symbol ring buffers

Each active symbol holds a fixed-capacity `RingBuffer<Candle>` in RAM. Capacity is
auto-tuned from the machine's L3 cache at startup:

```rust
ring_capacity = (usable_L3 / max_symbols) / size_of::<Candle>()
// M-series: (4 MB × 1/3) / 10 symbols / 48 B ≈ 2,800 candles/symbol
```

All symbol data combined stays within `usable_L3` — range queries run at L3
bandwidth (~160M candles/sec) rather than DRAM bandwidth (~7M candles/sec).

### LRU eviction to Parquet

When `symbol_count > max_symbols`, the least-recently-used symbol is evicted to a
Parquet file on disk (`{data_dir}/{symbol}/{ts_start}_{ts_end}.parquet`). On a
subsequent `range()` miss, the cold file is loaded and merged with any hot data.

Cold miss penalty: **23× slower** than a hot read (measured). Size `max_symbols`
to exceed your actual active symbol count and eviction never occurs.

### Concurrency model

`CandleStore` holds an outer `RwLock<HashMap<String, Arc<Entry>>>` plus a
per-`Entry` `RwLock<RingBuffer>`. The outer lock guards add/remove of symbols
only — every operation acquires its read-lock briefly (~10 ns) to clone an
`Arc<Entry>`, then drops it. The inner per-symbol RwLock is what guards each
symbol's ring. Result: appending to BTC does **not** block reads of ETH.

LRU is *approximate* — each `Entry` carries an `AtomicU64 last_access` that
gets stored on every append (a single `fetch_add` of a global tick). On
eviction, the store scans entries once to find the smallest timestamp. No
`VecDeque` to keep ordered on every access; no O(n) `lru_promote`.

Parquet I/O on eviction happens **under the outer write lock** for data
integrity — the LRU snapshot lives in the map while the spill runs. If
`parquet::spill` fails, the entry is left in place and the new candle is
rejected with `AppendError::EvictionSpillFailed`. The tradeoff: eviction
serialises all store operations for the duration of the spill (~100 ms
typical). Eviction is rare in well-sized configurations.

### Reactive consumer protocol

The store maintains `AtomicU64 version`, bumped on every successful append
(Release ordering). Consumers spin on `wait_for_change(last_seen)` (Acquire
load) on their own pinned core. Wake-up p50 = **14 µs** (vs the 50 ms wall-
clock-poll approach this replaced). For graceful shutdown, `signal_waiters()`
bumps version once without writing data so the spinner can observe its own
shutdown flag and exit.

### Append error contract

| API                                         | On spill failure          |
|---------------------------------------------|---------------------------|
| `try_append(symbol, candle) -> Result<…>`   | Returns `AppendError::EvictionSpillFailed`; state unchanged |
| `append(symbol, candle) -> ()`              | Logs ERROR + increments `appends_rejected_total`; new candle dropped, existing data preserved |

The old behaviour (silent data loss when the LRU's `Vec<Candle>` snapshot
was dropped on spill failure) is gone.

---

## 6. CPU Affinity

### Why pin threads to cores?

Modern OS schedulers migrate threads between cores freely. Each migration:
- invalidates the thread's L1/L2 cache (private to each core)
- may cross NUMA nodes on multi-socket systems (×4–8 latency multiplier)
- introduces jitter (the migration itself takes ~5–30 µs)

For a trading system, jitter on the signal path matters more than average latency.
A strategy thread that migrates between readings of the store will pay a cold-cache
penalty on every migration. Pinning eliminates this variability.

### Implementation

```
Linux:  sched_setaffinity(pid=0, cpu_set_t{core_id})
        Hard binding — kernel will not migrate the thread.

macOS:  thread_policy_set(mach_thread_self(), THREAD_AFFINITY_POLICY,
                          &{affinity_tag: core_id+1}, count=1)
        Soft co-location hint — threads with the same tag are grouped on the
        same physical core cluster. macOS does not expose hard pinning to
        user space (XNU scheduler reserves that right).
```

`src/affinity.rs` implements both with zero new dependencies — raw `libc` FFI for
Linux `sched_setaffinity`, raw Mach `extern "C"` declarations for macOS.

### Thread layout

```
Core 0: feed_handler (ingestion, network-facing, isolated)
Core 1: ingester thread (SHM pop → store append)
Core 2: strategy thread (store read → signal generation)
Core 3: executor thread (signal pop → order management)
```

Cores 1–3 all access the `CandleStore` `Arc`, which lives in shared heap. Adjacent
cores on the same L3 cluster share that cache — `store.range()` by the strategy
thread often hits cache lines already warm from the ingester's recent `append()`.

---

## 7. End-to-End Latency Budget

All numbers measured on Apple M-series, release build (`cargo bench --bench ipc_comparison`).

```
Stage                                Latency         Source
────────────────────────────────────────────────────────────────────
Exchange WebSocket → parse           ~50–500 µs      network + JSON
ShmRingWriter::push()                ~31 ns          atomic Release store
SHM ring transit (cross-process)     ~77 ns          cache line bounce
ShmRingReader::try_pop() + append()  ~21 ns          atomic Acquire + store append
CandleStore::append() (direct)       ~19 ns          per-symbol RwLock + ring push
CandleStore::wait_for_change wake-up ~14 µs (p50)    cross-core version load
CandleStore::last_n() (N=20)         ~80 ns          O(N) memcpy, no binary search
CandleStore::range() (W=1,000)       ~1.0 µs         O(log n) lower/upper bound + memcpy
Strategy compute (SMA crossover)     ~1–5 µs         floating-point, same data
SpscRing<Signal>::push()             ~77 ns          same SPSC protocol as above
SpscRing<Signal>::pop()              ~0 ns           (spin, usually empty)
────────────────────────────────────────────────────────────────────
Total (store-to-signal, no network)  ~16 µs
```

Network latency to the exchange (co-located): 50–500 µs.
Store-to-signal path: ~16 µs (was ~30 µs before reactive wake-up + binary search).
The bottleneck is exchange round-trip, not the internal kernel.

### Throughput

| Component                         | Throughput        |
|-----------------------------------|-------------------|
| ShmRingWriter::push               | ~32M candles/sec  |
| ShmIngester → store               | ~19M candles/sec  |
| Direct store.append() (1 thread)  | ~53M candles/sec  |
| Direct store.append() (4 sym × 4 t)| ~6.2M ops/s/thread (1.59× parallelism) |
| store.range() (W=5,000)           | ~1.2G elem/sec    |
| store.last_n (N=1,000)            | ~1.0G elem/sec    |

At 19M ingested candles/sec, the kernel can sustain 19 exchanges each sending
1M candles/sec simultaneously before the ingester becomes the bottleneck.

---

## 8. Design Decisions and Trade-offs

### Why SPSC, not MPSC?

MPSC (`std::sync::mpsc`) has no capacity limit — the writer never blocks. This
gives **2× higher bulk throughput** than a bounded SPSC ring (57M vs 28M msg/sec
measured). But MPSC requires OS-backed synchronisation on every handoff
(`sync_channel(0)` uses a futex — two syscalls per message, ~1.3 µs latency).

SPSC uses only atomic CAS with no kernel involvement: **77 ns latency**, 17× better.

For a trading system the choice is latency, not bulk throughput. The feed produces
at most ~10k candles/sec per symbol — well under any ring's capacity. SPSC is the
right choice.

### Why a fixed-capacity ring, not a growable queue?

A `Vec`-based store grows unboundedly. At `N=100k` it allocates 4.8 MB, exceeding
the 4 MB L3 on M-series. Range queries become DRAM-bound. Measured impact: the
L3-fit ring is **60–71× faster** than an unbounded ring of the same element count
(Decision 3 in `BENCHMARKS.md`).

A fixed ring also gives predictable memory usage. The store never OOMs under load.
Old candles are silently overwritten — which is correct for a **sliding-window**
market data store. If you need the full history, it is on disk in Parquet.

### Why Parquet for cold storage?

Parquet files are column-oriented, compressed, and readable by virtually every
analytics tool (DuckDB, Pandas, Spark). When a symbol is evicted from the hot ring,
its candles are serialised to `{data_dir}/{symbol}/{ts_start}_{ts_end}.parquet`.

Cold read penalty: 23× slower than hot (409 µs vs 17.5 µs for W=1,000). The right
response is to size `max_symbols` correctly — not to eliminate cold storage. Every
additional symbol in RAM costs `ring_capacity × 48 B` of L3 budget.

### Why hashbrown over std HashMap?

Symbol keys are internal strings (not attacker-controlled). `std::HashMap` uses
SipHash for DoS-resistance, which is correct for external keys but wasteful
internally. hashbrown's AHash is **2.5× faster per lookup** (6.7 ns vs 17.1 ns).
Every `append()` and `range()` call does one map lookup — this saves ~10 ns per
operation, or ~290 ms of CPU per second at 29M ops/sec peak load.

### Why parking_lot RwLock?

`parking_lot::RwLock::read()` acquires in **9.9 ns** vs `std::sync::RwLock::read()`
at 13.6 ns — 37% faster. `parking_lot` stores the waiter queue inline (no heap
allocation); its fast path is a single atomic CAS. `std::sync::RwLock` may
heap-allocate a waiter queue on some platforms.

More importantly: `RwLock` (over `Mutex`) lets multiple strategy threads read
concurrently. With `Mutex`, all readers serialise — throughput under N readers is
1× a single reader. With `RwLock` it scales linearly.

---

## 9. Running the System

```bash
# Build
cargo build --release

# Terminal 1: feed handler (core 0, 10k candles/sec)
cargo run --release --bin feed_handler

# Terminal 2: market hub (cores 1-3, ingest + strategy + executor)
cargo run --release --bin market_hub
```

### Configuration (env vars)

| Variable         | Default              | Description                      |
|------------------|----------------------|----------------------------------|
| `FEED_SHM_NAME`  | `/tradekern_feed`    | POSIX shm segment name           |
| `FEED_SHM_CAP`   | `65536`              | Ring capacity (must be pow2)     |
| `FEED_CORE`      | `0`                  | Core for feed_handler            |
| `FEED_SYMBOL`    | `BTCUSDT:1m`         | Symbol written to ring           |
| `FEED_RATE`      | `10000`              | Candles/sec from simulator       |
| `HUB_CORE`       | `1`                  | First core for market_hub threads|
| `SMA_SHORT`      | `10`                 | Short SMA period                 |
| `SMA_LONG`       | `20`                 | Long SMA period                  |
| `SIGNAL_QTY`     | `0.1`                | Order quantity per signal        |

### Benchmarks

```bash
cargo bench                            # all suites
cargo bench --bench ipc_comparison     # IPC throughput + latency + pipeline
cargo bench --bench design_decisions   # per-decision architectural analysis
cargo run --release --example cache_ladder   # L1/L2/L3/DRAM latency visualiser
```

Full results and methodology: **[BENCHMARKS.md](BENCHMARKS.md)**

---

## 10. Extension Points

The kernel is intentionally minimal. Natural next steps:

| Feature                    | Where to add                    | Notes                                 |
|----------------------------|---------------------------------|---------------------------------------|
| Real exchange feed         | Replace GBM in `feed_handler`   | Wire `BinanceFeed` (already built)    |
| Multiple symbols           | Add `FEED_SYMBOLS=BTC,ETH,SOL`  | One SHM ring per symbol, or multiplex |
| Multiple exchanges         | One `feed_handler` process each | Separate SHM rings, same `market_hub` |
| Order book (L2 data)       | New `OrderBookStore` module     | Same ring + store pattern             |
| Real order submission      | Extend executor thread          | Exchange REST/FIX, risk gate first    |
| NUMA-aware allocation      | `affinity.rs`                   | `numa_alloc_onnode` for ring memory   |
| Kernel bypass networking   | Replace WebSocket client        | DPDK / RDMA for sub-µs market data   |

---

## 11. Observability

The library never emits metrics on the hot path — at 53M append/sec a 5 ns
`metrics::counter!` call would cost ~26% of throughput. Instead:

- `CandleStore` and `ShmIngester` maintain internal `AtomicU64` counters
  (appends, evictions, parquet spill bytes/errors, popped, ring depth).
- `CandleStore::snapshot() -> StoreSnapshot` and `ShmIngester::stats() ->
  IngesterStats` expose them as cheap value snapshots (~6 atomic loads).
- Binaries install `tracing-subscriber` + `metrics-exporter-prometheus`.
- A 1-Hz metrics-poller thread reads snapshots and emits Prometheus counters
  on the `/metrics` HTTP endpoint.
- Rare events (Parquet spill failures, ring-fill > 50%) are logged inline
  via `tracing::error!` / `tracing::warn!`.

### Metric inventory

| Metric                                       | Type    | Source            |
|----------------------------------------------|---------|-------------------|
| `candlestore_appends_total`                  | counter | store.version     |
| `candlestore_symbols_active`                 | gauge   | store.map.len     |
| `candlestore_evictions_total`                | counter | store.evictions   |
| `candlestore_parquet_spill_bytes_total`      | counter | store             |
| `candlestore_parquet_spill_errors_total`     | counter | store             |
| `candlestore_appends_rejected_total`         | counter | store             |
| `candlestore_ingest_popped_total`            | counter | ingester          |
| `candlestore_ingest_ring_depth`              | gauge   | shm reader depth  |
| `candlestore_ingest_ring_fill_ratio`         | gauge   | shm reader        |
| `candlestore_signals_total`                  | counter | strategy thread   |
| `candlestore_executor_signals_total`         | counter | executor thread   |
| `candlestore_executor_position`              | gauge   | executor thread   |

No per-symbol labels — high cardinality would inflate Prometheus storage.
Per-symbol diagnosis comes from structured `tracing` events instead.

### Environment

| Variable        | Default | Description                                  |
|-----------------|---------|----------------------------------------------|
| `METRICS_PORT`  | 9090 (feed) / 9091 (hub) | Prometheus HTTP listen port |
| `RUST_LOG`      | `info`  | `tracing` filter (`debug,hyper=warn`, etc.)  |

---

## 12. Graceful Shutdown

Both binaries install `SIGINT` + `SIGTERM` handlers via `signal-hook` that
flip an `Arc<AtomicBool>` shutdown flag. Each worker thread observes the
flag at its idle/spin checkpoint and exits.

### market_hub shutdown sequence

```
SIGTERM arrives
  ↓
signal_hook handler flips shutdown=true
  ↓
main thread (park_timeout 100ms) wakes
  ↓
store.signal_waiters() — bumps version to unblock strategy's wait_for_change
  ↓
strategy.thread().unpark() / executor.thread().unpark() / poller.thread().unpark()
  ↓
each worker checks flag → break → return
  ↓
main joins each thread (5s timeout, with is_finished() poll)
  ↓
drop(Arc<ShmIngester>) — last ref → ShmIngester::Drop → stop() → ingester thread joins
  ↓
ShmRingReader::Drop → munmap
  ↓
drop(Arc<CandleStore>)
  ↓
main exits — "graceful shutdown complete"
```

### feed_handler shutdown sequence

```
SIGTERM arrives → shutdown=true
  ↓
hot loop top: `while !shutdown.load() { ... }` exits
  OR push_or_shutdown returns false (ring full + flag set, no consumer)
  ↓
drop(ShmRingWriter) → munmap + shm_unlink (segment removed for fresh restart)
  ↓
main exits — "SHM segment unlinked, feed_handler stopped"
```

**Caveat**: in-flight data sitting in the SHM ring at shutdown is lost. A
drain-then-exit phase (writer stops, reader drains, then both exit) is a
future improvement.

---

## 13. Parquet Schema Versioning

Every cold-storage file carries Arrow schema metadata identifying the writer:

```
candlestore.brand           = "candlestore"
candlestore.schema_version  = "1"
```

`check_schema_compat` runs before any read. The full read-behavior matrix:

| File state                                           | Result                          |
|------------------------------------------------------|----------------------------------|
| Brand + version == current                           | Read normally                    |
| Brand + version < current                            | Read normally                    |
| Brand + version > current                            | `SpillError::IncompatibleVersion`|
| No brand metadata (pre-versioning v0 files)          | Read as v0 (column-compat)       |
| Required column missing                              | `SpillError::MissingColumn`      |
| Required column has wrong type                       | `SpillError::WrongColumnType`    |
| Extra unknown columns present                        | Ignore (forward-compat)          |

The previous read path used positional column lookup with `unwrap()` — adding
a field to `Candle` would crash the process on the next cold read. The new
path looks up by name and returns structured errors, so `query_cold` can
skip an unreadable file (logged via `tracing::warn!`) and still return the
readable history.

When adding a `Candle` field: bump `parquet::SCHEMA_VERSION` to `2`, add the
column at the **end** of the schema as nullable, ship. Old binaries keep
reading old files; new binaries read both.
