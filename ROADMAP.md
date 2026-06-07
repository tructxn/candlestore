# candlestore Roadmap

Embeddable Rust library for financial candle (OHLCV) time-series data.
Hot data in RAM, cold data spilled to Parquet. No server, no GC, no SQL overhead.

---

## Phase 1 — Core Storage Engine ✅
> In-memory ring buffer per symbol, LRU eviction, benchmarks.

- [x] `Candle` struct — 48 bytes, `#[repr(C)]`, cache-line friendly
- [x] `RingBuffer` — fixed-capacity, O(1) append, wrap-around, range query
- [x] `CandleStore` — per-symbol ring buffers, LRU eviction when symbol cap hit
- [x] Unit tests — 10 tests covering push, wrap, range, LRU
- [x] Criterion benchmarks — ~30M appends/sec, ~175M elem/sec range scan

---

## Phase 2 — Hardware-Aware Configuration 🔲
> Auto-tune ring buffer capacity and struct layout based on the host machine.

- [ ] `HardwareProfile::detect()` — read L3 cache size, cache line size, physical core count
- [ ] `CandleStore::from_hardware(max_symbols)` — derive optimal `ring_capacity` from L3 / symbols / `size_of::<Candle>()`
- [ ] Adaptive `Candle` packing — 1 candle/cache-line on x86 (64B), 2 candles/cache-line on Apple M-series (128B)
- [ ] Expose `HardwareProfile` in public API so callers can inspect detected values
- [ ] Benchmark — compare auto-tuned vs static defaults on different machines
- [ ] Document: "why your ring buffer capacity should match your L3 cache"

---

## Phase 3 — Parquet Cold Storage ✅
> Evicted symbols spill to disk. Cold reads load back into RAM.

- [x] Write evicted `RingBuffer` to `{data_dir}/{symbol}/{ts_start}_{ts_end}.parquet`
- [x] Read cold Parquet file back on cache miss
- [x] Merge hot (RAM) + cold (Parquet) results in `range()` query
- [x] `CandleStore::with_data_dir(path)` builder method
- [x] Tests — evict → spill → reload → verify data integrity (6 parquet tests)
- [ ] Benchmark — cold read latency vs hot read latency

---

## Phase 4 — Binance WebSocket Feed ✅
> Populate the store with real BTC/USDT candle data.

- [x] Connect to Binance public combined stream WebSocket
- [x] Parse kline JSON → `Candle` (only closed candles stored)
- [x] Feed into `CandleStore` in real time via `BinanceFeed::run()`
- [x] Support multiple symbols + timeframes simultaneously
- [x] Graceful reconnect on any error (5s delay)
- [x] Example binary: `examples/binance_feed.rs`
- [x] Feature-gated behind `--features feed` — core lib stays zero async deps

---

## Phase 5 — Matching Engine (Paper Trading) ✅
> Order book on top of the store. Paper trade against real market data.

- [x] `OrderBook` — price-time priority, bid/ask sides (BTreeMap, integer price keys)
- [x] Order types: Limit, Market, IOC, FOK — all four implemented and tested
- [x] `PaperEngine` — candle-based fill simulation (market@open, limit@low/high touch)
- [x] Trade events: Fill, Cancel with CancelReason
- [x] `Portfolio` — positions, avg cost basis, realized + unrealized P&L
- [x] Example strategy: SMA(10/20) crossover — `examples/paper_trade.rs`
- [x] 33 tests passing across all modules

---

## Phase 6 — Go Client ✅
> Thin Go wrapper over the Rust library via FFI / C ABI.

- [x] `src/ffi.rs` — C ABI with `#[unsafe(no_mangle)]` (Rust 2024 edition)
- [x] `include/candlestore.h` — typed C header (`struct CandleStore` forward decl)
- [x] `go-client/candlestore/candlestore.go` — cgo wrapper: `New`, `Append`, `Range`, `Close`
- [x] `go-client/cmd/main.go` — SMA crossover strategy in Go, backed by Rust store
- [x] Hardware detection exposed: `L3CacheBytes()` callable from Go
- [x] Verified: same candle data + same strategy = same P&L in Go and Rust

---

## Phase 7 — Benchmarks vs QuestDB / InfluxDB ✅
> Prove the design is faster for this specific use case.

- [x] Naive baselines: flat Vec filter, HashMap+Vec filter, HashMap+bisect
- [x] Criterion benchmarks: append (~29M ops/sec), range (24–31 µs with RwLock)
- [x] Write up results in README with honest tradeoff analysis
- [x] vs QuestDB: ~2.6x faster ingestion, ~40x lower query latency (embedded vs TCP)
- [x] Hardware-aware sizing: `resource_fraction = 1/3` default for shared machines

---

## Phase 8 — SHM Ingestion Pipeline ✅
> Connect the SPSC shared-memory ring to the CandleStore, completing the
> feed handler → store → strategy data path.

- [x] `ShmIngester` — background thread that spins on `ShmRingReader::try_pop()`,
      calling `store.append(symbol, candle)` on every message
- [x] `ShmIngester::start(reader, Arc<CandleStore>, symbol)` — non-blocking factory;
      thread runs until `stop()` or `Drop`
- [x] `ShmIngester::stop()` — signals thread via `AtomicBool`, joins before returning
- [x] `examples/shm_pipeline.rs` — end-to-end demo: feed thread → SHM ring →
      ShmIngester → CandleStore → `range()` query; asserts all 50k candles land
- [x] Architecture diagram in README updated to show the full IPC path
- [x] Per-message IPC latency: **77 ns** (SPSC atomic CAS, no kernel, no copy)
- [x] 44 tests passing

---

## Phase 9 — Trading System Kernel ✅
> Full multi-process, multi-core trading pipeline with dedicated threads per
> component and SPSC rings as the inter-component bus.

- [x] `src/signal.rs` — `Signal` (64-byte, `Copy`, `repr(C)`) + `Side` enum
- [x] `src/affinity.rs` — `pin_to_core(id)` / `available_cores()`:
      Linux: `sched_setaffinity` (hard pin);
      macOS: Mach `thread_policy_set` affinity tag (soft hint)
- [x] `SpscRing<T>` — made generic over `T: Copy + Default + Send`; reused for
      both `Candle` (feed→store) and `Signal` (strategy→executor) buses
- [x] `src/bin/feed_handler.rs` — synthetic GBM price generator → ShmRingWriter,
      pinned to core 0, configurable rate (default 10k candles/sec)
- [x] `src/bin/market_hub.rs` — three threads:
      - ingester (core 1): ShmIngester → CandleStore
      - strategy (core 2): SMA(10/20) crossover → SpscRing<Signal>
      - executor (core 3): SpscReader<Signal> → paper position tracker
- [x] End-to-end demo verified: signals flow feed→ring→store→strategy→executor
- [x] 44 tests passing

```
Run in two terminals:
  cargo run --release --bin feed_handler
  cargo run --release --bin market_hub
```

---

## Phase 10 — Production Readiness ✅
> Close the gap from "impressive demo" to "I would deploy this." Driven by
> an honest review against enterprise standards; each sub-phase has its own
> diff and proof.

### 10.1 Reactive strategy — kill the 50 ms sleep ✅
- [x] `CandleStore::version()` + `wait_for_change(last_seen)` — pinned-core
      spin on an `AtomicU64` append counter (Release/Acquire ordering)
- [x] Strategy thread no longer polls on a wall-clock timer
- [x] `examples/reactive_latency.rs` measures wake-up: **p50 = 14 µs, p99 = 31 µs**
      (was 50 ms bounded below by the sleep) — **~3,500× faster reaction**

### 10.2 Binary-search range query ✅
- [x] `RingBuffer::range` rewritten using `O(log n)` lower/upper-bound search
      + `extend_from_slice` memcpy (one or two contiguous chunks for ring wrap)
- [x] New `RingBuffer::last_n(n)` — direct `O(n)` newest-K access, no binary
      search, no Vec-of-everything allocation
- [x] `CandleStore::last_n(symbol, n)` exposed; strategy uses it instead of
      `range(0, i64::MAX)`
- [x] Numbers: `range(W=100)` 24 µs → 210 ns (**114×**); `range(W=1k)` 25 µs →
      1.0 µs (**25×**); `last_n(N=10)` 65 ns; `last_n(N=100)` 108 ns
- [x] candlestore now matches `Vec + partition_point` baseline while keeping
      concurrent RwLock reads, LRU, and multi-symbol — "naive bisect wins"
      narrative gone

### 10.3 Per-symbol locks ✅
- [x] `RwLock<Inner>` → `RwLock<HashMap<String, Arc<Entry>>>` + per-`Entry`
      `RwLock<RingBuffer>`. Outer lock guards add/remove only
- [x] Approximate LRU via `Entry::last_access: AtomicU64` — removed O(n)
      `lru_promote` from every append
- [x] Reads/writes on different symbols don't block each other (test:
      `concurrent_reads_dont_block_writes_on_other_symbol`)
- [x] Single-threaded `append` throughput: 29M → **53M ops/s** (1.85×)
- [x] Multi-symbol parallelism: 4 threads × 4 symbols = 1.59× vs same-symbol
      contention (`examples/multi_symbol_contention.rs`)

### 10.4 Tracing + Prometheus metrics ✅
- [x] All `println!` replaced with structured `tracing::info!`/`warn!`/`error!`
- [x] `metrics-exporter-prometheus` HTTP `/metrics` endpoint on `METRICS_PORT`
      (feed_handler: 9090, market_hub: 9091)
- [x] `CandleStore::snapshot()` returns `StoreSnapshot` (lifetime counters
      via cheap atomic loads, never on the hot path)
- [x] `ShmRingReader::depth()` + `ShmIngester::stats()` for backpressure
      visibility
- [x] 11 named Prometheus metrics covering appends, evictions, parquet
      spills, ring depth/fill, signals, executor position
- [x] 1 Hz polling thread in market_hub reads snapshots and emits metrics —
      hot path pays zero metric overhead
- [x] `ShmRingReader` made `Sync` with documented SPSC safety contract

### 10.5 Plumb SpillError properly ✅
- [x] New `pub enum AppendError::EvictionSpillFailed { evicted_symbol,
      candles_lost, source: SpillError }`
- [x] `CandleStore::try_append() -> Result<(), AppendError>` for strict
      callers; `append()` retained as fire-and-forget convenience that logs
      and increments `appends_rejected_total`
- [x] Eviction's Parquet spill now happens **under the map write lock** —
      no window where the LRU snapshot exists only in a transient `Vec`
      that gets dropped on failure
- [x] On spill failure: existing data preserved in RAM, new candle rejected,
      structured error returned. Was: silent data loss
- [x] `appends_rejected_total` Prometheus counter + ERROR-level tracing log

### 10.6 Graceful SIGTERM ✅
- [x] `signal-hook` SIGINT/SIGTERM → `Arc<AtomicBool>` shutdown flag
- [x] `CandleStore::signal_waiters()` bumps version to unblock pinned
      consumers from `wait_for_change`
- [x] All worker threads (strategy, executor, metrics poller) honour the
      flag and exit cleanly
- [x] `ShmIngester::start_on_core` for clean ownership in `main` (fixes a
      pre-existing JoinHandle-drop lifetime bug)
- [x] `push_or_shutdown` helper in feed_handler — `try_push` with shutdown
      check in the spin-wait, fixes infinite hang when consumer disappears
- [x] Verified end-to-end: SIGTERM → "graceful shutdown complete" exit 0,
      SHM segment unlinked, fresh start works
- [x] 5 s timeout on thread joins with `is_finished()` poll — detach-with-
      warning if a worker misses the shutdown flag

### 10.8 CI pipeline ✅
- [x] `.github/workflows/ci.yml` running on push to `main` and on every PR
- [x] Matrix: ubuntu-latest + macos-latest (the deploy + dev OSes)
- [x] `cargo build --release --all-targets` + `cargo test --release`
      + `cargo build --release --features feed`
- [x] `cargo clippy --release --all-targets --no-deps -- -D warnings` — fails
      the build on any new lint
- [x] `cargo doc --no-deps --all-features` with `RUSTDOCFLAGS="-D warnings"`
      — broken intra-doc links fail the build
- [x] `rustsec/audit-check` — RustSec advisory database scan on every PR
- [x] `Swatinem/rust-cache@v2` — sub-minute warm cache hits
- [x] Cleared all 16 pre-existing clippy warnings + broken rustdoc links
      as part of bring-up

### 10.7 Parquet schema versioning ✅
- [x] `pub const SCHEMA_VERSION: u32 = 1` embedded in Arrow schema metadata
      (`candlestore.brand` + `candlestore.schema_version`)
- [x] `check_schema_compat` runs before any read — rejects newer-than-known
      files with `SpillError::IncompatibleVersion`
- [x] Column lookup by name (was: positional `.unwrap()`) — extra unknown
      columns are forward-compat ignored; missing required columns surface
      as `SpillError::MissingColumn` (no panics)
- [x] Pre-versioning (v0) files read transparently for back-compat
- [x] `query_cold` skips unreadable files with `tracing::warn!` instead of
      silently swallowing
- [x] 6 new tests cover the full matrix: round-trip metadata; v0 back-compat;
      future-version rejection; missing-column handling; extra-column
      forward-compat; query_cold skips bad files

**Phase 10 status: 69 tests passing, 0 ignored. Production hot path:**
- store append: 53M ops/s single-thread, 6.2M ops/s/thread with 4 symbols
- store range: 210 ns (W=100)
- store last_n: 65 ns (N=10)
- strategy wake-up: 14 µs p50
- IPC SPSC ring: ~77 ns rendezvous, ~19 Melem/s sustained
- SHM pipeline end-to-end: 19M candles/sec (1.7× IPC overhead vs direct)

---

## Phase 11 — Second-Pass Hardening ✅
> Findings from a second honest review of the codebase against enterprise
> production standards. The first review (Phase 10) closed the obvious gaps;
> this phase closes the second-order ones — concurrency edge cases,
> boundary validation, and real cross-process / FFI test coverage.

### 11.1 SHM safety ✅ (commit 0189cdb)
- [x] S1: `ShmRingWriter::create` now uses `O_CREAT | O_EXCL` — second
      producer for the same name fails with `AlreadyExists` instead of
      silently corrupting the first. `create_force()` provided for
      operator-controlled crash recovery.
- [x] S2: `ShmRingReader::open` verifies `header.capacity` matches the
      caller's argument — mismatch returns `InvalidData` instead of UB.
- [x] S3: `ShmRingWriter::push_until(candle, &AtomicBool)` — cancellable
      variant of `push` for safe shutdown. Replaces the per-binary
      `push_or_shutdown` workaround.
- [x] O4: `ShmRingWriter::stats()` exposes lifetime `push_full_events` —
      ops can see "consumer is behind" in Prometheus. Surfaced as
      `candlestore_feed_push_full_total`.

### 11.2 Boundary validation ✅ (commit 4a0f0cd)
- [x] S5: `Candle::is_valid()` rejects NaN/Inf/negative-ts. `try_append`
      returns `AppendError::InvalidCandle` at the boundary — no more
      NaN propagating into SMAs and executor positions.
- [x] S6: `RingBuffer::push` returns `PushOutcome { Ok | OutOfOrder }`.
      Store counts OOO events in `out_of_order_total` and emits a
      tracing::warn! per event. Operators learn before the binary-search
      range query starts silently missing data.
- [x] Q1: `Signal::try_new` returns Result, rejecting `SymbolTooLong`
      and `NonFiniteField`. The old `Signal::new` kept for back-compat
      but now logs warn! on truncation.

### 11.3 Order book + clock skew ✅ (commit f2ea192)
- [x] S4: `(p * 1e8).round() as u64` replaced with bounds-checked
      `try_price_key`. Rejects NaN/Inf/negative/overflow with structured
      `BookError`. `OrderBook::try_submit` for strict consumers; the
      convenience `submit` now emits `CancelReason::InvalidOrder`
      instead of panicking.
- [x] R8: `feed_handler` stamps candles with monotonic ts via
      `next_ts(last_ts)` — NTP clock-backwards corrections can no
      longer break downstream binary-search range queries.

### 11.4 Eviction race ✅
- [x] R5: `Entry::removed: AtomicBool` set under the buf write lock
      during eviction, checked by the fast-path appender after acquiring
      buf.write(). Closes the "Arc holder pushes to a dead Entry"
      data-loss window. Tested with a 3-thread stress test
      (`appends_during_concurrent_eviction_are_never_lost`).

### 11.5 Integration tests ✅
- [x] R3: `tests/cross_process_shm.rs` uses `std::env::current_exe()`
      self-spawn to test the SHM ring across two OS processes:
      writer-in-parent + reader-in-child roundtrip; capacity-mismatch
      detection; O_EXCL second-producer rejection.
- [x] R4: `tests/ffi.rs` exercises every `candlestore_*` extern function
      with valid input, null pointers, oversize buffer clamping, and
      NaN-candle passthrough. 10 tests.

### 11.6 Operability touch-ups (this commit)
- [x] O5: `ShmIngester::stop_signal(&self)` — market_hub signals the
      ingester FIRST in shutdown so it stops pumping data into a store
      no one is reading during the join window. CPU on hub_core drops
      to 0 immediately on SIGTERM.
- [x] O6: `hw.rs::default_l3_with_warning` — one-shot tracing::warn! fires
      when L3 detection falls back to the 8 MiB default. Operators in
      containers see "L3 detection failed: /sys unreadable" instead of
      silently wrong store sizing.
- [x] Q4: Phase 8 and Phase 9 reordered in this doc (Phase 8 came after
      Phase 9 chronologically inverted — a cosmetic distraction).

112 tests passing across 6 suites; cargo clippy --all-targets -D warnings: clean.

---

## Non-Goals
- Multi-node / replication — out of scope
- SQL interface — use DuckDB + Parquet for ad-hoc queries
- Tick data (sub-candle) — Phase 1+ focuses on OHLCV candles only
- Authentication / multi-tenancy — embedded library, caller owns security
