use std::collections::VecDeque;
use std::path::PathBuf;

use hashbrown::HashMap;
use parking_lot::RwLock;

use crate::{Candle, hw::HardwareProfile, parquet, ring_buffer::RingBuffer};

const DEFAULT_RING_CAPACITY: usize = 10_000;

struct Entry {
    buf:         RingBuffer,
    last_access: u64,
}

pub struct CandleStore {
    inner: RwLock<Inner>,
}

struct Inner {
    symbols:       HashMap<String, Entry>,
    lru_order:     VecDeque<String>,
    max_symbols:   usize,
    ring_capacity: usize,
    tick:          u64,
    data_dir:      Option<PathBuf>,
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
            inner: RwLock::new(Inner {
                symbols:       HashMap::with_capacity(max_symbols),
                lru_order:     VecDeque::with_capacity(max_symbols),
                max_symbols,
                ring_capacity,
                tick:          0,
                data_dir:      None,
            }),
        }
    }

    /// Enable Parquet cold storage — evicted symbols spill to `dir`.
    pub fn with_data_dir(self, dir: impl Into<PathBuf>) -> Self {
        self.inner.write().data_dir = Some(dir.into());
        self
    }

    // ── write ─────────────────────────────────────────────────────────────────

    pub fn append(&self, symbol: &str, candle: Candle) {
        // Extract any eviction payload *before* releasing the write lock,
        // then do I/O outside the lock so readers aren't blocked.
        let eviction = {
            let mut g = self.inner.write();
            g.tick += 1;
            let tick = g.tick;

            if let Some(entry) = g.symbols.get_mut(symbol) {
                entry.buf.push(candle);
                entry.last_access = tick;
                lru_promote(&mut g.lru_order, symbol);
                None
            } else {
                let eviction = if g.symbols.len() >= g.max_symbols {
                    g.lru_order.pop_back().map(|sym| {
                        let candles = g.symbols.remove(&sym)
                            .map(|e| e.buf.as_slice())
                            .unwrap_or_default();
                        (sym, candles, g.data_dir.clone())
                    })
                } else {
                    None
                };

                let cap = g.ring_capacity;
                let mut buf = RingBuffer::new(cap);
                buf.push(candle);
                g.symbols.insert(symbol.to_string(), Entry { buf, last_access: tick });
                g.lru_order.push_front(symbol.to_string());
                eviction
            }
        };

        // I/O outside lock
        if let Some((sym, candles, Some(dir))) = eviction {
            let _ = parquet::spill(&dir, &sym, &candles);
        }
    }

    // ── read ──────────────────────────────────────────────────────────────────

    pub fn range(&self, symbol: &str, from_ts: i64, to_ts: i64) -> Vec<Candle> {
        let (hot, data_dir) = {
            let g = self.inner.read();
            let hot = g.symbols
                .get(symbol)
                .map(|e| e.buf.range(from_ts, to_ts))
                .unwrap_or_default();
            (hot, g.data_dir.clone())
        };

        if let Some(dir) = data_dir {
            let cold = parquet::query_cold(&dir, symbol, from_ts, to_ts);
            if cold.is_empty() { return hot; }
            if hot.is_empty()  { return cold; }
            merge_by_ts(cold, hot)
        } else {
            hot
        }
    }

    pub fn symbol_count(&self) -> usize {
        self.inner.read().symbols.len()
    }

    /// Number of candles currently held in the hot ring for `symbol`.
    pub fn candle_count(&self, symbol: &str) -> usize {
        self.inner.read().symbols.get(symbol).map(|e| e.buf.len()).unwrap_or(0)
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn lru_promote(order: &mut VecDeque<String>, symbol: &str) {
    if let Some(pos) = order.iter().position(|s| s == symbol) {
        order.remove(pos);
    }
    order.push_front(symbol.to_string());
}

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
}
