use std::collections::VecDeque;
use hashbrown::HashMap;
use parking_lot::RwLock;
use crate::{Candle, ring_buffer::RingBuffer};

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
        Self {
            inner: RwLock::new(Inner {
                symbols:      HashMap::with_capacity(max_symbols),
                lru_order:    VecDeque::with_capacity(max_symbols),
                max_symbols,
                ring_capacity: DEFAULT_RING_CAPACITY,
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
