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

`CandleStore` is wrapped in `Arc<RwLock<Inner>>`. Reads (`range()`) acquire a
shared read lock; writes (`append()`) acquire an exclusive write lock only long
enough to update the ring buffer and LRU order. Parquet I/O happens **outside the
lock** — the eviction payload is extracted under the write lock, then serialised
after it's released. Readers are never blocked by disk I/O.

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
Stage                               Latency         Source
────────────────────────────────────────────────────────────────────
Exchange WebSocket → parse          ~50–500 µs      network + JSON
ShmRingWriter::push()               ~31 ns          atomic Release store
SHM ring transit (cross-process)    ~77 ns          cache line bounce
ShmRingReader::try_pop() + append() ~21 ns          atomic Acquire + store append
CandleStore::append() (direct)      ~31 ns          RwLock + ring push
CandleStore::range() (hot, W=1000)  ~25 µs          lock + linear scan, L3-resident
Strategy compute (SMA crossover)    ~1–5 µs         floating-point, same data
SpscRing<Signal>::push()            ~77 ns          same SPSC protocol as above
SpscRing<Signal>::pop()             ~0 ns           (spin, usually empty)
────────────────────────────────────────────────────────────────────
Total (store-to-signal, no network) ~30 µs
```

Network latency to the exchange (co-located): 50–500 µs.
Store-to-signal path: ~30 µs.
The bottleneck is exchange round-trip, not the internal kernel.

### Throughput

| Component                  | Throughput        |
|----------------------------|-------------------|
| ShmRingWriter::push        | ~32M candles/sec  |
| ShmIngester → store        | ~19M candles/sec  |
| Direct store.append()      | ~32M candles/sec  |
| store.range() (W=5,000)    | ~160M candles/sec |

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
