use crate::Candle;

/// Fixed-capacity ring buffer for candles.
/// Oldest entry is overwritten when full.
pub struct RingBuffer {
    buf:   Box<[Candle]>,
    head:  usize,   // next write position
    len:   usize,   // current count (≤ capacity)
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        let candle = Candle { ts: 0, open: 0.0, high: 0.0, low: 0.0, close: 0.0, volume: 0.0 };
        Self {
            buf:  vec![candle; capacity].into_boxed_slice(),
            head: 0,
            len:  0,
        }
    }

    pub fn push(&mut self, c: Candle) {
        self.buf[self.head] = c;
        self.head = (self.head + 1) % self.buf.len();
        if self.len < self.buf.len() { self.len += 1; }
    }

    /// Returns candles in chronological order (oldest first).
    pub fn as_slice(&self) -> Vec<Candle> {
        if self.len == 0 { return vec![]; }
        let cap = self.buf.len();
        let start = if self.len < cap { 0 } else { self.head };
        (0..self.len)
            .map(|i| self.buf[(start + i) % cap])
            .collect()
    }

    /// Range query — returns candles where from_ts ≤ ts ≤ to_ts.
    pub fn range(&self, from_ts: i64, to_ts: i64) -> Vec<Candle> {
        self.as_slice()
            .into_iter()
            .filter(|c| c.ts >= from_ts && c.ts <= to_ts)
            .collect()
    }

    pub fn len(&self) -> usize { self.len }
    pub fn capacity(&self) -> usize { self.buf.len() }
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// Oldest timestamp in buffer, if any.
    pub fn oldest_ts(&self) -> Option<i64> {
        self.as_slice().first().map(|c| c.ts)
    }
}
