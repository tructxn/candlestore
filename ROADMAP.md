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

## Phase 3 — Parquet Cold Storage 🔲
> Evicted symbols spill to disk. Cold reads load back into RAM.

- [ ] Write evicted `RingBuffer` to `{data_dir}/{symbol}/{ts_start}-{ts_end}.parquet`
- [ ] Read cold Parquet file back on cache miss
- [ ] Merge hot (RAM) + cold (Parquet) results in `range()` query
- [ ] Configurable `data_dir` on `CandleStore::new()`
- [ ] Tests — evict → spill → reload → verify data integrity
- [ ] Benchmark — cold read latency vs hot read latency

---

## Phase 4 — Binance WebSocket Feed 🔲
> Populate the store with real BTC/USDT candle data.

- [ ] Connect to Binance public WebSocket kline stream
- [ ] Parse kline JSON → `Candle`
- [ ] Feed into `CandleStore` in real time
- [ ] Support multiple symbols + timeframes simultaneously
- [ ] Graceful reconnect on disconnect
- [ ] Example binary: `examples/binance_feed.rs`

---

## Phase 5 — Matching Engine (Paper Trading) 🔲
> Order book on top of the store. Paper trade against real market data.

- [ ] `OrderBook` — price-time priority, bid/ask sides
- [ ] Order types: Limit, Market, IOC, FOK
- [ ] Match engine loop — consume candle feed, match pending orders
- [ ] Trade event output — fills, cancels, partial fills
- [ ] Portfolio tracker — positions, PnL, cash balance
- [ ] Example strategy: simple moving average crossover

---

## Phase 6 — Go Client 🔲
> Thin Go wrapper over the Rust library via FFI / C ABI.

- [ ] Expose C ABI from Rust (`#[no_mangle]`, `extern "C"`)
- [ ] Go bindings via `cgo`
- [ ] Same API: `Append(symbol, candle)`, `Range(symbol, from, to)`
- [ ] Example Go matching engine using candlestore as storage layer
- [ ] This is the Zero Hash demo piece

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
