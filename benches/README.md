# candlestore Benchmarks

Two benchmark suites. Run with `cargo bench`.

---

## `bench.rs` — Baseline Throughput

Measures raw candlestore performance vs naive Rust alternatives.

| Benchmark             | What it measures                                              |
|-----------------------|---------------------------------------------------------------|
| `append/N`            | Ingestion throughput — ring buffer push under write lock      |
| `range_query/W`       | Range query — linear scan over 10k-candle ring buffer         |
| `lru_eviction`        | Cost of writing 100 symbols past the max_symbols cap          |
| `naive_vec_range`     | Flat `Vec<Candle>` with linear filter — allocates per call    |
| `naive_hashmap_range` | `HashMap<symbol, Vec>` + linear filter                        |
| `naive_hashmap_bisect`| `HashMap<symbol, Vec>` + `partition_point` — fastest naive   |

---

## `design_decisions.rs` — Architectural Validation

Each group benchmarks one design choice. Run `cargo bench --bench design_decisions`.

All numbers measured on Apple M-series (10 physical cores, 4 MB L3), release build.

---

### Decision 1: Ring buffer vs `Vec` append

**Question**: Is the ring buffer's guaranteed O(1) append meaningfully better than `Vec`'s
amortized O(1)?

| Impl             | N=1,000 | N=10,000 | N=100,000 | Per-push @ 100k |
|------------------|---------|----------|-----------|-----------------|
| ring             | 10.5 µs | 38.5 µs  | 308.7 µs  | 3.09 ns         |
| vec_no_prealloc  | 2.5 µs  | 41.2 µs  | 932.1 µs  | 9.32 ns         |
| vec_prealloc     | 1.9 µs  | 24.9 µs  | 484.4 µs  | 4.84 ns         |

**Reading**: The ring is slower at N=1k because it allocates its full fixed backing store
(10,240 × 48 B = 491 KB) up front. At N=100k the ring is **3× faster** than unoptimized
Vec and **36% faster** than pre-allocated Vec. Two reasons:

1. **No realloc spikes**: Vec doubles capacity on overflow, copying all existing data.
   Ring never reallocates — push cost is identical from element 1 to element 100,000.
2. **Cache-bound working set**: the ring stays at 491 KB (fits in L3).
   A 100k-element Vec grows to 4.8 MB — L3 on M-series is 4 MB, so it overflows.

In trading systems, tail latency from realloc spikes is as dangerous as average throughput.

---

### Decision 2: hashbrown vs std HashMap

**Question**: Does using `hashbrown` instead of `std::collections::HashMap` speed up
the per-append symbol lookup?

| Impl         | 100 lookups | Per-lookup |
|--------------|-------------|------------|
| hashbrown    | 668 ns      | 6.7 ns     |
| std HashMap  | 1,700 ns    | 17.1 ns    |

**2.5× faster.** `std::HashMap` defaults to SipHash-1-3 to defend against HashDoS attacks
(an adversary flooding the hash table with colliding keys). Symbol keys (`"BTCUSDT:1m"`)
are internal — never user-supplied — so the DoS protection burns CPU for no benefit.
`hashbrown` uses AHash in this configuration, which skips the SipHash mixing overhead.

Every `append()` call does one symbol lookup. At 29M appends/sec this is on the critical path.

---

### Decision 3: L3-fit capacity vs overflow

**Question**: Does sizing the ring buffer to fit in the usable portion of L3 cache
actually improve range query performance?

| Config       | Range window=100 | Range window=1,000 |
|--------------|-----------------|-------------------|
| l3_fit       | 6.9 µs          | 8.1 µs            |
| l3_overflow  | 488.6 µs        | 483.1 µs          |

**71× slower** when the ring overflows L3. Two effects compound:

1. **More data to scan**: l3_fit ring = ~2,800 candles; l3_overflow ring = ~84,000 candles.
   Range queries do a linear scan — O(n) in ring size.
2. **Cache pressure**: the 84k-element ring (~4 MB) blows out the 4 MB L3. Every scan
   fetches from DRAM rather than L3, adding ~100 ns per cache miss.

`HardwareProfile::ring_capacity_for(N)` derives the L3-fit size automatically from
the detected L3 cache size, symbol count, and `resource_fraction`. This is why the
hardware-awareness feature matters.

---

### Decision 4: Lock strategy — parking_lot vs std, RwLock vs Mutex

**Question**: Does using `parking_lot::RwLock` instead of `std::sync::RwLock` (or
`Mutex`) meaningfully reduce lock overhead?

**Single-threaded (uncontended) acquisition cost:**

| Lock                         | Acquisition |
|------------------------------|-------------|
| `parking_lot::RwLock::read`  | 9.9 ns      |
| `parking_lot::Mutex::lock`   | 8.3 ns      |
| `std::sync::RwLock::read`    | 13.6 ns     |
| `std::sync::Mutex::lock`     | 9.4 ns      |

`parking_lot` RwLock read is **27% faster** than std — it avoids a heap allocation that
`std::sync::RwLock` may incur on some platforms, and its fast path is shorter.

**Why RwLock over Mutex?** candlestore is read-heavy. Range queries (reads) vastly
outnumber appends (writes) in any real market data workload. With a Mutex, concurrent
range queries block each other — throughput under N concurrent readers is 1× single-reader.
With RwLock, all N readers proceed simultaneously — throughput scales linearly with readers.

---

### Decision 5: Hot (RAM) vs cold (Parquet) read

**Question**: How expensive is a Parquet cold miss relative to a hot ring buffer read?

| Path                  | Latency  | Relative |
|-----------------------|----------|----------|
| hot ring read (1k)    | 17.5 µs  | 1×       |
| cold Parquet read (1k)| 409.5 µs | **23×**  |

**23× slower** on a cold miss. The cold path must:
1. Discover matching Parquet files (directory scan + filename parse)
2. Open and decode the Parquet file via Arrow (columnar decode → row structs)
3. Merge the cold results with any hot data still in the ring

**Practical implication**: set `max_symbols` high enough that your active trading symbols
never get LRU-evicted. If you trade 50 symbols, use `CandleStore::new(50)` not `new(10)`.
The ring capacity auto-tunes to L3, so adding symbols doesn't mean losing cache efficiency —
it just shrinks the per-symbol candle window.
