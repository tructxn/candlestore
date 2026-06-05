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

## Phase 7 — Benchmarks vs QuestDB / InfluxDB 🔲
> Prove the design is faster for this specific use case.

- [ ] Equivalent benchmark against QuestDB (same data, same queries)
- [ ] Equivalent benchmark against InfluxDB 3.0
- [ ] Write up results in README
- [ ] Target: 10x faster than QuestDB on hot symbol range queries

---

## Non-Goals
- Multi-node / replication — out of scope
- SQL interface — use DuckDB + Parquet for ad-hoc queries
- Tick data (sub-candle) — Phase 1+ focuses on OHLCV candles only
- Authentication / multi-tenancy — embedded library, caller owns security
