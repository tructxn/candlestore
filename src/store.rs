use std::collections::VecDeque;
use hashbrown::HashMap;
use parking_lot::RwLock;
use crate::{Candle, hw::HardwareProfile, ring_buffer::RingBuffer};

const DEFAULT_RING_CAPACITY: usize = 10_000; // ~10k candles hot per symbol

struct Entry {
    buf:        RingBuffer,
    last_access: u64,
}

pub struct CandleStore {
    inner: RwLock<Inner>,
}

struct Inner {
    symbols:      HashMap<String, Entry>,
    lru_order:    VecDeque<String>,  // front = most recent
    max_symbols:  usize,
    ring_capacity: usize,
    tick:         u64,
}

impl CandleStore {
    pub fn new(max_symbols: usize) -> Self {
        Self::with_capacity(max_symbols, DEFAULT_RING_CAPACITY)
    }

    /// Auto-tune ring buffer capacity from detected L3 cache size.
    pub fn from_hardware(max_symbols: usize) -> Self {
        let hw = HardwareProfile::detect();
        let ring_capacity = hw.ring_capacity_for(max_symbols);
        Self::with_capacity(max_symbols, ring_capacity)
    }

    pub fn with_capacity(max_symbols: usize, ring_capacity: usize) -> Self {
        Self {
            inner: RwLock::new(Inner {
                symbols:      HashMap::with_capacity(max_symbols),
                lru_order:    VecDeque::with_capacity(max_symbols),
                max_symbols,
                ring_capacity,
                tick:         0,
            }),
        }
    }

    pub fn append(&self, symbol: &str, candle: Candle) {
        let mut g = self.inner.write();
        g.tick += 1;
        let tick = g.tick;

        if let Some(entry) = g.symbols.get_mut(symbol) {
            entry.buf.push(candle);
            entry.last_access = tick;
            // move to front of LRU
            if let Some(pos) = g.lru_order.iter().position(|s| s == symbol) {
                g.lru_order.remove(pos);
            }
            g.lru_order.push_front(symbol.to_string());
        } else {
            // evict if at capacity
            if g.symbols.len() >= g.max_symbols {
                if let Some(evicted) = g.lru_order.pop_back() {
                    // TODO: spill evicted symbol to Parquet before dropping
                    g.symbols.remove(&evicted);
                }
            }
            let cap = g.ring_capacity;
            let mut buf = RingBuffer::new(cap);
            buf.push(candle);
            g.symbols.insert(symbol.to_string(), Entry { buf, last_access: tick });
            g.lru_order.push_front(symbol.to_string());
        }
    }

    pub fn range(&self, symbol: &str, from_ts: i64, to_ts: i64) -> Vec<Candle> {
        let g = self.inner.read();
        g.symbols
            .get(symbol)
            .map(|e| e.buf.range(from_ts, to_ts))
            .unwrap_or_default()
    }

    pub fn symbol_count(&self) -> usize {
        self.inner.read().symbols.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Candle;

    fn candle(ts: i64) -> Candle {
        Candle { ts, open: 1.0, high: 1.0, low: 1.0, close: 1.0, volume: 1.0 }
    }

    #[test]
    fn append_and_range_basic() {
        let store = CandleStore::new(10);
        store.append("BTC/USDT:1m", candle(100));
        store.append("BTC/USDT:1m", candle(200));
        store.append("BTC/USDT:1m", candle(300));
        let out = store.range("BTC/USDT:1m", 100, 200);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn unknown_symbol_returns_empty() {
        let store = CandleStore::new(10);
        assert!(store.range("ETH/USDT:1m", 0, 9999).is_empty());
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let store = CandleStore::new(2); // only 2 symbols fit
        store.append("A", candle(1));
        store.append("B", candle(2));
        // access A again to make B the LRU
        store.append("A", candle(3));
        // adding C should evict B
        store.append("C", candle(4));
        assert_eq!(store.symbol_count(), 2);
        assert!(store.range("B", 0, 9999).is_empty()); // B evicted
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
}
