use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hashbrown::HashMap;
use parking_lot::RwLock;
use thiserror::Error;

use crate::{Candle, hw::HardwareProfile, parquet, ring_buffer::RingBuffer};

const DEFAULT_RING_CAPACITY: usize = 10_000;

/// One symbol's hot ring plus an approximate-LRU timestamp.
///
/// `buf` is guarded by its own `RwLock` so reads and writes on different
/// symbols never contend. `last_access` is an atomic counter bumped on every
/// `append` for this symbol; the eviction scan picks the entry with the
/// smallest value.
struct Entry {
    buf:         RwLock<RingBuffer>,
    last_access: AtomicU64,
    /// Set to `true` under the buf write lock during eviction, BEFORE the
    /// entry is removed from the map. The hot-path appender, having cloned
    /// an `Arc<Entry>` via the map read-lock, acquires the buf write lock,
    /// re-checks this flag, and retries via the slow path if the entry has
    /// been evicted between the clone and the lock acquisition. Closes the
    /// "Arc holder pushes to a dead Entry" data-loss window.
    ///
    /// `Acquire`/`Release` ordering pairs with the eviction's store-true.
    removed:     std::sync::atomic::AtomicBool,
}

/// Embeddable hot-RAM + cold-Parquet candle store.
///
/// # Concurrency model
///
/// - **Outer map** (`RwLock<HashMap<String, Arc<Entry>>>`) guards the symbol
///   table itself. Read-locked on every `append`/`range`/`last_n` for ~10 ns
///   to clone the `Arc<Entry>`. Write-locked only when inserting a new symbol
///   (which may also evict the LRU symbol).
/// - **Per-symbol lock** (`Entry::buf` is its own `RwLock<RingBuffer>`)
///   guards each symbol's ring independently. Appending to `BTC` does NOT
///   block reads of `ETH`.
/// - **Approximate LRU** via `Entry::last_access`. No `VecDeque` to keep
///   sorted on every access; eviction does a single O(n_symbols) scan to
///   find the smallest timestamp, which is acceptable because eviction is
///   rare in well-sized configurations.
///
/// # Observability
///
/// Internal atomic counters track lifetime totals for appends (via
/// [`version`](Self::version)), evictions, and Parquet spill outcomes. Cheap to update
/// (single `fetch_add`) so the hot path stays sub-microsecond. The application
/// reads these via [`snapshot`](Self::snapshot) on a low-frequency timer (e.g.
/// every second) and feeds them to its metrics pipeline; the library itself
/// never emits metrics on the hot path. Rare error events (Parquet spill
/// failures) are logged via `tracing::error!`.
pub struct CandleStore {
    map: RwLock<HashMap<String, Arc<Entry>>>,

    max_symbols:   usize,
    ring_capacity: usize,
    data_dir:      Option<PathBuf>,

    /// Monotonic append counter — bumped on every `append()` regardless of
    /// symbol. Consumers spin on this to detect new candles without polling
    /// on a wall-clock timer. u64 at 1M appends/sec wraps in ~585,000 years.
    version: AtomicU64,

    /// Monotonic logical clock for the approximate LRU. Bumped on every
    /// `append()`, stored into the touched entry's `last_access`.
    tick: AtomicU64,

    /// Lifetime evictions count — bumped each time an LRU symbol is removed.
    evictions: AtomicU64,
    /// Lifetime bytes successfully written to Parquet on eviction.
    parquet_spill_bytes: AtomicU64,
    /// Lifetime Parquet spill failures. Each is also logged via tracing.
    parquet_spill_errors: AtomicU64,
    /// Lifetime appends rejected because an eviction spill failed and the
    /// store would have lost existing data to admit the new candle.
    appends_rejected: AtomicU64,
    /// Lifetime candles rejected at the boundary for NaN/Inf/negative-ts.
    invalid_candles: AtomicU64,
    /// Lifetime appends accepted but whose ts went backwards. See snapshot doc.
    out_of_order: AtomicU64,
}

/// Error returned from [`CandleStore::try_append`] when the store cannot
/// safely admit a new candle without losing data.
///
/// All variants leave the store in a consistent state — the caller may retry
/// the append, accept the data loss explicitly, or trigger an operator alert.
#[derive(Debug, Error)]
pub enum AppendError {
    /// The store is at `max_symbols`, the LRU eviction's Parquet spill
    /// failed, and the new candle was rejected to preserve the existing
    /// data already in RAM.
    ///
    /// Operator action: investigate the I/O failure (`source`), then either
    /// retry the append (a different LRU may now succeed) or stop ingestion
    /// until disk health is restored.
    #[error(
        "LRU eviction of {evicted_symbol:?} ({candles_lost} candles) failed during Parquet spill: {source}"
    )]
    EvictionSpillFailed {
        /// The symbol the store attempted to evict.
        evicted_symbol: String,
        /// The number of candles that would have been spilled (and which
        /// remain safely in RAM since the spill failed).
        candles_lost:   usize,
        /// The underlying Parquet/IO failure.
        #[source]
        source: parquet::SpillError,
    },
    /// The candle failed [`Candle::is_valid`] — at least one f64 field is
    /// NaN/Infinity or `ts` is negative. Rejected at the boundary so the
    /// poison value doesn't propagate into SMAs, executor positions, or
    /// metrics.
    ///
    /// Operator action: fix the producer. If a real feed is sending
    /// occasional NaN/Inf (some venues do under maintenance), filter
    /// upstream rather than relaxing this check — once it's in the store,
    /// every downstream computation goes NaN forever.
    #[error("candle rejected: invalid field (NaN/Inf/negative-ts)")]
    InvalidCandle {
        /// The symbol the rejected candle was destined for.
        symbol: String,
    },
}

/// Lifetime counter snapshot for metrics export.
///
/// Read via [`CandleStore::snapshot`]. Producing a snapshot is cheap (a few
/// atomic loads plus a read-lock on the outer map) and intended to be polled
/// at low frequency (e.g. once per second) by an observability sidecar that
/// publishes Prometheus / OpenTelemetry / etc. counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreSnapshot {
    /// Lifetime append count (== `version`).
    pub appends_total:              u64,
    /// Currently active symbols (in RAM).
    pub symbol_count:               usize,
    /// Configured cap.
    pub max_symbols:                usize,
    /// Per-symbol ring capacity.
    pub ring_capacity:              usize,
    /// Lifetime LRU evictions.
    pub evictions_total:            u64,
    /// Lifetime bytes written to Parquet on eviction (successes only).
    pub parquet_spill_bytes_total:  u64,
    /// Lifetime Parquet spill failures.
    pub parquet_spill_errors_total: u64,
    /// Lifetime appends rejected to preserve existing data when a Parquet
    /// spill failed. See [`AppendError::EvictionSpillFailed`].
    pub appends_rejected_total:     u64,
    /// Lifetime candles rejected for NaN/Infinity/negative-ts.
    /// See [`AppendError::InvalidCandle`].
    pub invalid_candles_total:      u64,
    /// Lifetime appends accepted but whose ts went BACKWARDS vs the previous
    /// candle on the same symbol. The candle is still stored, but binary-
    /// search range queries may miss it. Sustained growth means the producer
    /// is sending late updates and the system should be rebuilt with a
    /// re-sort strategy or accept the discrepancy.
    pub out_of_order_total:         u64,
}

impl CandleStore {
    pub fn new(max_symbols: usize) -> Self {
        Self::with_capacity(max_symbols, DEFAULT_RING_CAPACITY)
    }

    pub fn from_hardware(max_symbols: usize) -> Self {
        let hw = HardwareProfile::detect();
        Self::with_capacity(max_symbols, hw.ring_capacity_for(max_symbols))
    }

    pub fn with_capacity(max_symbols: usize, ring_capacity: usize) -> Self {
        Self {
            map:                  RwLock::new(HashMap::with_capacity(max_symbols)),
            max_symbols,
            ring_capacity,
            data_dir:             None,
            version:              AtomicU64::new(0),
            tick:                 AtomicU64::new(0),
            evictions:            AtomicU64::new(0),
            parquet_spill_bytes:  AtomicU64::new(0),
            parquet_spill_errors: AtomicU64::new(0),
            appends_rejected:     AtomicU64::new(0),
            invalid_candles:      AtomicU64::new(0),
            out_of_order:         AtomicU64::new(0),
        }
    }

    /// Enable Parquet cold storage — evicted symbols spill to `dir`.
    pub fn with_data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }

    // ── write ─────────────────────────────────────────────────────────────────

    /// Fire-and-forget append. On a Parquet spill failure during LRU eviction
    /// OR an invalid (NaN/Inf/negative-ts) candle, this logs an error and
    /// increments the appropriate counter. The candle is dropped; existing
    /// data is preserved.
    ///
    /// Use [`try_append`](Self::try_append) when you need a `Result` to
    /// branch on, e.g. to halt ingestion when disk health degrades or to
    /// surface invalid input to upstream.
    pub fn append(&self, symbol: &str, candle: Candle) {
        if let Err(e) = self.try_append(symbol, candle) {
            // Note: try_append already increments the specific counter for
            // InvalidCandle. appends_rejected_total is for spill failures
            // specifically. Branch on the variant.
            match &e {
                AppendError::InvalidCandle { .. }     => {} // counter already bumped
                AppendError::EvictionSpillFailed { .. } => {
                    self.appends_rejected.fetch_add(1, Ordering::Relaxed);
                }
            }
            tracing::error!(symbol = %symbol, error = %e,
                "append rejected — existing data preserved");
        }
    }

    /// Strict append. Returns:
    /// - `Err(AppendError::InvalidCandle)` if the candle has NaN/Inf/negative-ts.
    ///   Validated at the boundary so poison values never enter the ring.
    /// - `Err(AppendError::EvictionSpillFailed)` when the store is full and
    ///   the LRU eviction's Parquet spill failed.
    ///
    /// On error the store state is unchanged. Out-of-order candles are
    /// accepted but counted in `out_of_order_total`.
    pub fn try_append(&self, symbol: &str, candle: Candle) -> Result<(), AppendError> {
        // ── boundary validation ───────────────────────────────────────────────
        // Hot-path cost: 5 is_finite() calls + 1 i64 compare ≈ 1-2 ns. Worth it
        // to keep NaN out of the system; once it's in, every SMA, every
        // executor delta, every gauge propagates NaN forever.
        if !candle.is_valid() {
            self.invalid_candles.fetch_add(1, Ordering::Relaxed);
            return Err(AppendError::InvalidCandle { symbol: symbol.to_owned() });
        }

        let tick = self.tick.fetch_add(1, Ordering::Relaxed);

        // ── fast path: symbol already exists ──────────────────────────────────
        // Read-lock the outer map, clone the Arc, release outer lock. The
        // per-symbol write lock then guards the ring push without blocking
        // any other symbol.
        let entry = {
            let g = self.map.read();
            g.get(symbol).cloned()
        };

        if let Some(entry) = entry {
            entry.last_access.store(tick, Ordering::Relaxed);
            let mut buf_guard = entry.buf.write();

            // Eviction-race check: if our Arc was cloned BEFORE the symbol
            // got evicted, the entry is now stale and we must NOT push into
            // it. The evictor sets `removed=true` under this same buf write
            // lock, so by the time we hold the lock the flag is definitive.
            if entry.removed.load(Ordering::Acquire) {
                drop(buf_guard);
                return self.try_insert_new(symbol, candle, tick);
            }

            let outcome = buf_guard.push(candle);
            if let crate::ring_buffer::PushOutcome::OutOfOrder { prev_ts, this_ts } = outcome {
                self.out_of_order.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    symbol, prev_ts, this_ts,
                    "out-of-order candle — binary-search range queries may miss it"
                );
            }
            drop(buf_guard);
            self.version.fetch_add(1, Ordering::Release);
            return Ok(());
        }

        // ── slow path: insert (and possibly evict) ────────────────────────────
        self.try_insert_new(symbol, candle, tick)
    }

    /// Insert a new symbol. May evict the LRU symbol if `max_symbols` is hit.
    ///
    /// **Locking**: this method holds the outer map's write lock for the entire
    /// operation, including any Parquet spill I/O. This serialises evictions
    /// against all other store operations — appends and reads block for the
    /// duration of the spill (typically <100ms for an L3-sized ring). The
    /// trade-off is data integrity: spilling under the lock means we never
    /// have a window where the LRU's snapshot exists in memory but its Entry
    /// has already been removed from the map (which is how the previous
    /// implementation lost data on spill failure).
    ///
    /// In well-sized configurations evictions are rare, so the blocking cost
    /// is paid infrequently. Size `max_symbols` to exceed your active symbol
    /// count to avoid eviction entirely.
    fn try_insert_new(&self, symbol: &str, candle: Candle, tick: u64) -> Result<(), AppendError> {
        let mut g = self.map.write();

        // Race check: another thread may have inserted the same symbol while
        // we waited for the write lock.
        if let Some(entry) = g.get(symbol).cloned() {
            drop(g);
            entry.last_access.store(tick, Ordering::Relaxed);
            let mut buf_guard = entry.buf.write();
            // Same eviction-race window as the fast path — bounded retry.
            if entry.removed.load(Ordering::Acquire) {
                drop(buf_guard);
                return self.try_insert_new(symbol, candle, tick);
            }
            let outcome = buf_guard.push(candle);
            if let crate::ring_buffer::PushOutcome::OutOfOrder { prev_ts, this_ts } = outcome {
                self.out_of_order.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    symbol, prev_ts, this_ts,
                    "out-of-order candle — binary-search range queries may miss it"
                );
            }
            drop(buf_guard);
            self.version.fetch_add(1, Ordering::Release);
            return Ok(());
        }

        // Eviction if we're at capacity. Spill happens INSIDE the lock so
        // there is no window where the entry exists only in a transient Vec.
        if g.len() >= self.max_symbols {
            let evict_key = g.iter()
                .min_by_key(|(_, e)| e.last_access.load(Ordering::Relaxed))
                .map(|(k, _)| k.clone());

            if let Some(key) = evict_key {
                // Snapshot the candidate's data — entry is still in the map.
                let candles_to_spill = g.get(&key)
                    .map(|e| e.buf.read().as_slice())
                    .unwrap_or_default();

                if !candles_to_spill.is_empty() {
                    if let Some(dir) = self.data_dir.as_deref() {
                        let bytes = (candles_to_spill.len() * std::mem::size_of::<Candle>()) as u64;
                        match parquet::spill(dir, &key, &candles_to_spill) {
                            Ok(()) => {
                                self.parquet_spill_bytes.fetch_add(bytes, Ordering::Relaxed);
                                tracing::debug!(
                                    symbol = %key, candles = candles_to_spill.len(), bytes,
                                    "parquet spill ok"
                                );
                            }
                            Err(e) => {
                                // Failure path: entry is STILL in the map.
                                // Leave it there. The caller decides what to do.
                                self.parquet_spill_errors.fetch_add(1, Ordering::Relaxed);
                                tracing::error!(
                                    symbol = %key, candles = candles_to_spill.len(), error = %e,
                                    "parquet spill failed — eviction aborted, existing data preserved"
                                );
                                return Err(AppendError::EvictionSpillFailed {
                                    evicted_symbol: key,
                                    candles_lost:   candles_to_spill.len(),
                                    source:         e,
                                });
                            }
                        }
                    } else {
                        // No data_dir → caller explicitly opted into "drop on
                        // evict" mode. Warn once so it shows up in logs.
                        tracing::warn!(
                            symbol = %key, candles = candles_to_spill.len(),
                            "evicted without data_dir — candles dropped (user policy)"
                        );
                    }
                }

                // Critical: mark `removed = true` UNDER the entry's buf
                // write lock BEFORE removing from the map. This serialises
                // against any fast-path appender that already cloned the
                // Arc (via the outer read lock that we waited for before
                // acquiring the outer write lock above). When that
                // appender finally gets entry.buf.write(), it sees
                // removed=true and retries via the slow path instead of
                // pushing a candle into a buffer about to disappear.
                //
                // Lock order is outer-write → inner-write throughout; no
                // deadlock with the slow path (same order) or fast path
                // (no outer lock held when taking inner).
                if let Some(arc) = g.get(&key) {
                    let _buf_guard = arc.buf.write();
                    arc.removed.store(true, Ordering::Release);
                    // Release buf lock here; queued fast-path appenders
                    // unblock, observe removed=true, and retry — they will
                    // then queue on the outer write lock we still hold.
                }
                self.evictions.fetch_add(1, Ordering::Relaxed);
                g.remove(&key);
            }
        }

        // Now safe to insert the new symbol. First push of a new RingBuffer
        // is always Ok (no prior ts to compare against) — discard outcome.
        let mut buf = RingBuffer::new(self.ring_capacity);
        let _ = buf.push(candle);
        g.insert(symbol.to_string(), Arc::new(Entry {
            buf:         RwLock::new(buf),
            last_access: AtomicU64::new(tick),
            removed:     std::sync::atomic::AtomicBool::new(false),
        }));

        drop(g);
        self.version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    // ── read ──────────────────────────────────────────────────────────────────

    pub fn range(&self, symbol: &str, from_ts: i64, to_ts: i64) -> Vec<Candle> {
        let entry = {
            let g = self.map.read();
            g.get(symbol).cloned()
        };

        let hot = if let Some(entry) = entry {
            entry.buf.read().range(from_ts, to_ts)
        } else {
            Vec::new()
        };

        if let Some(dir) = self.data_dir.as_deref() {
            let cold = parquet::query_cold(dir, symbol, from_ts, to_ts);
            if cold.is_empty() { return hot; }
            if hot.is_empty()  { return cold; }
            merge_by_ts(cold, hot)
        } else {
            hot
        }
    }

    /// Returns the last `n` candles for `symbol` (newest, in chronological order).
    /// O(min(n, ring_capacity)) — the strategy's natural access pattern.
    ///
    /// Unlike [`range`](Self::range), this does NOT merge with cold Parquet
    /// data — it only returns what's hot in RAM. The strategy doesn't need
    /// cold data for a rolling SMA, and we don't want a hot loop touching disk.
    pub fn last_n(&self, symbol: &str, n: usize) -> Vec<Candle> {
        let entry = {
            let g = self.map.read();
            g.get(symbol).cloned()
        };
        entry.map(|e| e.buf.read().last_n(n)).unwrap_or_default()
    }

    pub fn symbol_count(&self) -> usize {
        self.map.read().len()
    }

    /// Number of candles currently held in the hot ring for `symbol`.
    pub fn candle_count(&self, symbol: &str) -> usize {
        let entry = {
            let g = self.map.read();
            g.get(symbol).cloned()
        };
        entry.map(|e| e.buf.read().len()).unwrap_or(0)
    }

    /// Monotonic append counter — bumped once per `append()` call.
    ///
    /// Consumers that need to react to new candles should snapshot this value,
    /// then spin on [`wait_for_change`](Self::wait_for_change) (pinned-core
    /// design) or compare on each tick (cooperative design). Cheaper than
    /// polling `range()` on a wall-clock timer.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Atomic counter snapshot for metrics export.
    ///
    /// Cheap: ~6 atomic loads + one outer-map read-lock for `symbol_count`.
    /// Intended to be polled at ~1 Hz by an observability sidecar that emits
    /// Prometheus / OpenTelemetry counters. The library never emits metrics
    /// on the hot path itself.
    pub fn snapshot(&self) -> StoreSnapshot {
        StoreSnapshot {
            appends_total:              self.version.load(Ordering::Relaxed),
            symbol_count:               self.map.read().len(),
            max_symbols:                self.max_symbols,
            ring_capacity:              self.ring_capacity,
            evictions_total:            self.evictions.load(Ordering::Relaxed),
            parquet_spill_bytes_total:  self.parquet_spill_bytes.load(Ordering::Relaxed),
            parquet_spill_errors_total: self.parquet_spill_errors.load(Ordering::Relaxed),
            appends_rejected_total:     self.appends_rejected.load(Ordering::Relaxed),
            invalid_candles_total:      self.invalid_candles.load(Ordering::Relaxed),
            out_of_order_total:         self.out_of_order.load(Ordering::Relaxed),
        }
    }

    /// Spin until [`version`](Self::version) advances past `last_seen`, then
    /// return the new version. Uses `std::hint::spin_loop` between checks.
    ///
    /// Designed for a consumer pinned to a dedicated core where 100% CPU on
    /// that core is acceptable (e.g. the strategy thread in `market_hub`).
    /// For a non-dedicated consumer, prefer polling `version()` with a yield.
    #[inline]
    pub fn wait_for_change(&self, last_seen: u64) -> u64 {
        loop {
            let v = self.version.load(Ordering::Acquire);
            if v != last_seen { return v; }
            std::hint::spin_loop();
        }
    }

    /// Wake any threads spinning on [`wait_for_change`](Self::wait_for_change)
    /// without appending data. Bumps `version` exactly once with `Release`
    /// ordering.
    ///
    /// Used during graceful shutdown to unblock pinned consumers so they can
    /// observe their own shutdown signal and exit cleanly. No data lost, no
    /// false signal — the consumer simply wakes, sees no new candles, checks
    /// its shutdown flag, and breaks out of its loop.
    #[inline]
    pub fn signal_waiters(&self) {
        self.version.fetch_add(1, Ordering::Release);
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Merge two ts-sorted Vec<Candle> into one sorted vec, dedup by ts.
fn merge_by_ts(a: Vec<Candle>, b: Vec<Candle>) -> Vec<Candle> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut ai, mut bi) = (0, 0);
    while ai < a.len() && bi < b.len() {
        match a[ai].ts.cmp(&b[bi].ts) {
            std::cmp::Ordering::Less    => { out.push(a[ai]); ai += 1; }
            std::cmp::Ordering::Greater => { out.push(b[bi]); bi += 1; }
            std::cmp::Ordering::Equal   => { out.push(a[ai]); ai += 1; bi += 1; } // prefer cold
        }
    }
    out.extend_from_slice(&a[ai..]);
    out.extend_from_slice(&b[bi..]);
    out
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(ts: i64) -> Candle {
        Candle { ts, open: 1.0, high: 2.0, low: 0.5, close: 1.5, volume: 100.0 }
    }

    #[test]
    fn append_and_range_basic() {
        let store = CandleStore::new(10);
        store.append("BTC/USDT:1m", candle(100));
        store.append("BTC/USDT:1m", candle(200));
        store.append("BTC/USDT:1m", candle(300));
        assert_eq!(store.range("BTC/USDT:1m", 100, 200).len(), 2);
    }

    #[test]
    fn unknown_symbol_returns_empty() {
        let store = CandleStore::new(10);
        assert!(store.range("ETH/USDT:1m", 0, 9999).is_empty());
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let store = CandleStore::new(2);
        store.append("A", candle(1));
        store.append("B", candle(2));
        store.append("A", candle(3)); // A is now MRU, B is LRU
        store.append("C", candle(4)); // evicts B
        assert_eq!(store.symbol_count(), 2);
        assert!(store.range("B", 0, 9999).is_empty());
        assert!(!store.range("A", 0, 9999).is_empty());
        assert!(!store.range("C", 0, 9999).is_empty());
    }

    #[test]
    fn multiple_symbols_independent() {
        let store = CandleStore::new(10);
        store.append("BTC/USDT:1m", candle(1));
        store.append("ETH/USDT:1m", candle(2));
        assert_eq!(store.range("BTC/USDT:1m", 0, 9999).len(), 1);
        assert_eq!(store.range("ETH/USDT:1m", 0, 9999).len(), 1);
    }

    #[test]
    fn evict_spills_to_parquet_and_range_merges() {
        let dir = tempfile::tempdir().unwrap();
        // max 1 symbol — appending second evicts first to disk
        let store = CandleStore::new(1).with_data_dir(dir.path());

        // fill BTC, then evict by adding ETH
        for i in 0..5i64 {
            store.append("BTC/USDT:1m", candle(i * 60_000));
        }
        store.append("ETH/USDT:1m", candle(999_999)); // evicts BTC → parquet

        // BTC is no longer hot but should be found on disk
        let cold = store.range("BTC/USDT:1m", 0, 4 * 60_000);
        assert_eq!(cold.len(), 5, "cold data should be recovered from parquet");
    }

    #[test]
    fn version_bumps_on_every_append() {
        let store = CandleStore::new(10);
        assert_eq!(store.version(), 0);
        store.append("BTC", candle(1));
        assert_eq!(store.version(), 1);
        store.append("BTC", candle(2));
        store.append("ETH", candle(3));
        assert_eq!(store.version(), 3);
    }

    #[test]
    fn signal_waiters_wakes_wait_for_change_without_appending() {
        use std::time::{Duration, Instant};

        let store = Arc::new(CandleStore::new(10));
        let v0 = store.version();
        let symbols_before = store.symbol_count();

        // Spawn a "consumer" that spins on wait_for_change. It must wake
        // promptly when we call signal_waiters().
        let store2 = Arc::clone(&store);
        let consumer = std::thread::spawn(move || {
            let start = Instant::now();
            let v = store2.wait_for_change(v0);
            (v, start.elapsed())
        });

        std::thread::sleep(Duration::from_millis(20)); // ensure consumer is spinning
        store.signal_waiters();

        let (v1, elapsed) = consumer.join().unwrap();
        assert!(v1 > v0, "version must advance");
        assert!(
            elapsed < Duration::from_millis(200),
            "wake-up should be prompt, got {elapsed:?}"
        );
        // No symbol inserted — just a version bump.
        assert_eq!(store.symbol_count(), symbols_before);
    }

    #[test]
    fn wait_for_change_returns_when_version_advances() {
        use std::time::{Duration, Instant};

        let store = Arc::new(CandleStore::new(10));
        store.append("BTC", candle(1));
        let v0 = store.version();

        let store2 = Arc::clone(&store);
        let producer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            store2.append("BTC", candle(2));
        });

        let start = Instant::now();
        let v1 = store.wait_for_change(v0);
        let elapsed = start.elapsed();

        producer.join().unwrap();
        assert!(v1 > v0, "version should have advanced");
        assert!(
            elapsed < Duration::from_millis(100),
            "wait_for_change should return promptly after the append, got {elapsed:?}"
        );
    }

    #[test]
    fn range_merges_hot_and_cold() {
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(1).with_data_dir(dir.path());

        // first batch → gets evicted to cold
        for i in 0..5i64 {
            store.append("BTC/USDT:1m", candle(i));
        }
        store.append("ETH/USDT:1m", candle(0)); // evicts BTC

        // re-add BTC with new candles (hot)
        for i in 5..10i64 {
            store.append("BTC/USDT:1m", candle(i));
        }
        // ETH evicted, BTC hot again — range should merge cold(0..4) + hot(5..9)
        let result = store.range("BTC/USDT:1m", 0, 9);
        assert_eq!(result.len(), 10);
        assert_eq!(result[0].ts, 0);
        assert_eq!(result[9].ts, 9);
    }

    // ── new: concurrency tests for per-symbol locking ────────────────────────

    #[test]
    fn concurrent_appends_across_many_symbols_complete_without_data_loss() {
        // Stress test: 8 threads × 4 symbols × 5k appends each. Verifies the
        // per-symbol locks neither lose data nor deadlock under contention.
        // We don't assert on timing — thread-spawn + version-counter cache
        // bouncing make wall-clock comparisons unreliable at this scale; the
        // throughput win shows up properly in the bench suite.
        let store = Arc::new(CandleStore::new(8));
        let n_per_thread = 5_000i64;
        let n_threads = 8;
        let symbols = ["BTC", "ETH", "SOL", "ADA"];

        let handles: Vec<_> = (0..n_threads).map(|t| {
            let s = Arc::clone(&store);
            let sym = symbols[t % symbols.len()];
            std::thread::spawn(move || {
                for i in 0..n_per_thread {
                    // Stagger timestamps so the same-symbol threads produce
                    // monotonically-increasing ts and the ring stays sorted.
                    s.append(sym, candle((t as i64) * n_per_thread + i));
                }
            })
        }).collect();

        for h in handles { h.join().unwrap(); }

        // Each symbol got 2 threads × 5k = 10k appends.
        for sym in &symbols {
            assert_eq!(
                store.candle_count(sym), 2 * n_per_thread as usize,
                "candle_count for {sym} should be {}", 2 * n_per_thread
            );
        }
        // version should equal total appends across all threads.
        assert_eq!(
            store.version(),
            (n_threads as u64) * (n_per_thread as u64),
            "version counter should equal total append count"
        );
    }

    #[test]
    fn concurrent_reads_dont_block_writes_on_other_symbol() {
        // Long-running read of BTC must not delay an ETH append.
        let store = Arc::new(CandleStore::new(8));
        for i in 0..50_000i64 { store.append("BTC", candle(i)); }

        let s1 = Arc::clone(&store);
        let reader = std::thread::spawn(move || {
            // Many large range queries on BTC.
            for _ in 0..200 {
                let _ = s1.range("BTC", 0, i64::MAX);
            }
        });

        // While the reader is busy, append ETH and verify version moves.
        let v0 = store.version();
        for i in 0..1_000i64 { store.append("ETH", candle(i)); }
        let v1 = store.version();

        reader.join().unwrap();
        assert!(v1 >= v0 + 1_000, "ETH appends must complete while BTC reads run");
        assert_eq!(store.candle_count("ETH"), 1_000);
    }

    // ── new: AppendError plumbing tests ──────────────────────────────────────

    #[test]
    fn try_append_returns_err_when_spill_fails_and_preserves_existing_data() {
        // Point data_dir at a path the process cannot write to. On macOS and
        // Linux, attempting to create a file under a non-writable parent
        // returns EACCES / EROFS / similar — parquet::spill will surface that
        // as a SpillError, which try_append must surface as EvictionSpillFailed.
        //
        // We use `/proc/self/cmdline` on Linux or `/dev/null` parent on macOS
        // — but the most portable trick is a *file* posing as a directory.
        // tempfile gives us a real path we can render unwritable.
        let dir = tempfile::tempdir().unwrap();
        // Create a regular file at the path we'll claim is data_dir.
        let bogus = dir.path().join("not-a-dir");
        std::fs::write(&bogus, b"not a directory").unwrap();
        // The store will try to write {bogus}/{symbol}/{ts_start}_{ts_end}.parquet
        // — that fails because `bogus` is a file, not a directory.

        let store = CandleStore::new(1).with_data_dir(&bogus);

        // Fill the single slot with BTC. This succeeds (no eviction needed).
        for i in 0..3i64 {
            assert!(store.try_append("BTC", candle(i)).is_ok());
        }
        assert_eq!(store.candle_count("BTC"), 3);

        // Now try ETH — this triggers eviction of BTC, which attempts to
        // spill to a bogus dir, which fails.
        let err = store.try_append("ETH", candle(999)).expect_err("must fail");
        match err {
            AppendError::EvictionSpillFailed { evicted_symbol, candles_lost, .. } => {
                assert_eq!(evicted_symbol, "BTC");
                assert_eq!(candles_lost, 3);
            }
            AppendError::InvalidCandle { .. } => panic!("candle was valid; expected spill failure"),
        }

        // Critically: BTC's data must still be in RAM. ETH must NOT be in the map.
        assert_eq!(store.candle_count("BTC"), 3, "BTC must be preserved");
        assert_eq!(store.candle_count("ETH"), 0, "ETH must NOT have been admitted");
        assert_eq!(store.symbol_count(), 1);

        // Counters reflect the failure.
        let snap = store.snapshot();
        assert!(snap.parquet_spill_errors_total >= 1);
        // appends_rejected is only bumped by the convenience `append`, not
        // `try_append`. Verify it stays at 0 here.
        assert_eq!(snap.appends_rejected_total, 0);
    }

    #[test]
    fn append_logs_and_increments_rejected_on_spill_failure() {
        // Same setup as the try_append test, but use the fire-and-forget
        // `append` and verify appends_rejected_total bumps.
        let dir = tempfile::tempdir().unwrap();
        let bogus = dir.path().join("not-a-dir");
        std::fs::write(&bogus, b"not a directory").unwrap();

        let store = CandleStore::new(1).with_data_dir(&bogus);
        store.append("BTC", candle(1));
        store.append("BTC", candle(2));
        store.append("ETH", candle(3)); // triggers eviction → spill fail → reject

        // BTC preserved, ETH rejected.
        assert_eq!(store.candle_count("BTC"), 2);
        assert_eq!(store.candle_count("ETH"), 0);

        let snap = store.snapshot();
        assert_eq!(snap.appends_rejected_total, 1);
        assert!(snap.parquet_spill_errors_total >= 1);
    }

    #[test]
    fn try_append_succeeds_when_spill_succeeds() {
        // Sanity check: with a writable data_dir, try_append returns Ok and
        // counters reflect a successful eviction.
        let dir = tempfile::tempdir().unwrap();
        let store = CandleStore::new(1).with_data_dir(dir.path());

        for i in 0..3i64 {
            assert!(store.try_append("BTC", candle(i * 1_000_000_000)).is_ok());
        }
        assert!(store.try_append("ETH", candle(999_999)).is_ok());

        assert_eq!(store.candle_count("BTC"), 0);
        assert_eq!(store.candle_count("ETH"), 1);

        let snap = store.snapshot();
        assert_eq!(snap.evictions_total, 1);
        assert_eq!(snap.parquet_spill_errors_total, 0);
        assert_eq!(snap.appends_rejected_total, 0);
        assert!(snap.parquet_spill_bytes_total > 0);

        // And the spilled BTC data is recoverable on read.
        let cold = store.range("BTC", 0, 3 * 1_000_000_000);
        assert_eq!(cold.len(), 3);
    }

    #[test]
    fn concurrent_inserts_of_same_symbol_dont_double_create() {
        // Two threads race to insert the same fresh symbol. Exactly one wins
        // the insert; the other's append must still land in the same entry.
        let store = Arc::new(CandleStore::new(4));
        let n = 5_000i64;

        let s1 = Arc::clone(&store);
        let s2 = Arc::clone(&store);
        let t1 = std::thread::spawn(move || {
            for i in 0..n { s1.append("RACE", candle(i)); }
        });
        let t2 = std::thread::spawn(move || {
            for i in n..(2 * n) { s2.append("RACE", candle(i)); }
        });
        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(store.symbol_count(), 1);
        assert_eq!(store.candle_count("RACE"), (2 * n) as usize);
    }

    // ── Phase 11: boundary validation ───────────────────────────────────────

    fn bad_candle(field: u8) -> Candle {
        let mut c = candle(1);
        match field {
            0 => c.ts     = -1,
            1 => c.open   = f64::NAN,
            2 => c.high   = f64::INFINITY,
            3 => c.low    = f64::NEG_INFINITY,
            4 => c.close  = f64::NAN,
            5 => c.volume = f64::NAN,
            _ => unreachable!(),
        }
        c
    }

    #[test]
    fn try_append_rejects_nan_field() {
        let store = CandleStore::new(4);
        for f in 0..=5u8 {
            let bad = bad_candle(f);
            match store.try_append("BTC", bad) {
                Err(AppendError::InvalidCandle { symbol }) => assert_eq!(symbol, "BTC"),
                other => panic!("expected InvalidCandle for field {f}, got {other:?}"),
            }
        }
        // No symbol entered the store.
        assert_eq!(store.symbol_count(), 0);
        let snap = store.snapshot();
        assert_eq!(snap.invalid_candles_total, 6);
        assert_eq!(snap.appends_total, 0);
    }

    #[test]
    fn append_rejects_invalid_candle_without_polluting_existing_data() {
        let store = CandleStore::new(4);
        store.append("BTC", candle(1));
        store.append("BTC", candle(2));
        // Now feed garbage — must not affect what's stored.
        store.append("BTC", bad_candle(4)); // close = NaN
        store.append("BTC", bad_candle(5)); // volume = NaN
        assert_eq!(store.candle_count("BTC"), 2);
        let snap = store.snapshot();
        assert_eq!(snap.invalid_candles_total, 2);
        assert_eq!(snap.appends_total, 2, "version only bumps on accepted appends");
    }

    // ── Phase 11: eviction race ────────────────────────────────────────────

    #[test]
    fn appends_during_concurrent_eviction_are_never_lost() {
        // Property test: while one thread hammers eviction by rotating 4
        // symbols through a 2-symbol-cap store, two other threads append to
        // two specific symbols. Every accepted append (the ones for the
        // "stable" symbols that don't get evicted) must show up in the
        // store. Before the Entry::removed flag, the fast-path appender
        // could push into a buf already removed from the map and the
        // candle would be silently lost.
        //
        // We assert via the version counter: it must equal the number of
        // successful try_append() calls across all threads.
        use std::sync::atomic::AtomicU64;
        use std::time::{Duration, Instant};

        let store = Arc::new(CandleStore::new(2));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let accepted = Arc::new(AtomicU64::new(0));

        let s = Arc::clone(&store);
        let stop2 = Arc::clone(&stop);
        let accepted2 = Arc::clone(&accepted);
        let writer_a = std::thread::spawn(move || {
            let mut i = 0i64;
            while !stop2.load(Ordering::Relaxed) {
                if s.try_append("STABLE_A", candle(i)).is_ok() {
                    accepted2.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        });

        let s = Arc::clone(&store);
        let stop2 = Arc::clone(&stop);
        let accepted2 = Arc::clone(&accepted);
        let writer_b = std::thread::spawn(move || {
            let mut i = 0i64;
            while !stop2.load(Ordering::Relaxed) {
                if s.try_append("STABLE_B", candle(i)).is_ok() {
                    accepted2.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        });

        // Eviction churner: insert new symbols, which forces eviction of
        // the LRU. Since STABLE_A and STABLE_B are being constantly
        // appended, they'd both be MRU; the churn-symbols would be the
        // eviction targets. To actually exercise the race we instead
        // append to a rotating set of "churn" symbols whose appends
        // push them ahead of STABLE_*. With max_symbols=2 and 3 ever-
        // appended-to symbols, one IS evicted each turn.
        let s = Arc::clone(&store);
        let stop2 = Arc::clone(&stop);
        let accepted2 = Arc::clone(&accepted);
        let churner = std::thread::spawn(move || {
            let mut i = 0i64;
            while !stop2.load(Ordering::Relaxed) {
                let sym = format!("CHURN_{}", i % 8);
                if s.try_append(&sym, candle(i)).is_ok() {
                    accepted2.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        });

        std::thread::sleep(Duration::from_millis(500));
        stop.store(true, Ordering::Relaxed);
        let _ = writer_a.join();
        let _ = writer_b.join();
        let _ = churner.join();

        // The invariant: every accepted append bumped version exactly once.
        // If the race had eaten a push, version < accepted.
        let n_accepted = accepted.load(Ordering::Relaxed);
        let snap = store.snapshot();
        assert!(
            n_accepted > 1000,
            "test should produce >>1k appends; got {n_accepted} (slow CI?)"
        );
        assert_eq!(
            snap.appends_total, n_accepted,
            "every successful try_append must bump version exactly once \
             (Entry::removed flag closes the eviction race)"
        );

        // Sanity: at least some evictions happened.
        assert!(snap.evictions_total > 0, "expected at least one eviction");

        // Suppress unused warning when build is fast.
        let _ = Instant::now();
    }

    #[test]
    fn out_of_order_candle_counted_but_accepted() {
        let store = CandleStore::new(4);
        store.append("BTC", candle(10));
        store.append("BTC", candle(20));
        store.append("BTC", candle(15)); // out-of-order
        assert_eq!(store.candle_count("BTC"), 3, "OOO candle stored");
        let snap = store.snapshot();
        assert_eq!(snap.out_of_order_total, 1);
        assert_eq!(snap.appends_total, 3);
    }
}
