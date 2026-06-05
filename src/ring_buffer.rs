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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Candle;

    fn candle(ts: i64, close: f64) -> Candle {
        Candle { ts, open: close, high: close, low: close, close, volume: 1.0 }
    }

    #[test]
    fn empty_on_creation() {
        let rb = RingBuffer::new(4);
        assert!(rb.is_empty());
        assert_eq!(rb.len(), 0);
        assert_eq!(rb.oldest_ts(), None);
    }

    #[test]
    fn push_and_retrieve_in_order() {
        let mut rb = RingBuffer::new(4);
        rb.push(candle(1, 10.0));
        rb.push(candle(2, 20.0));
        rb.push(candle(3, 30.0));
        let out = rb.as_slice();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].ts, 1);
        assert_eq!(out[2].ts, 3);
    }

    #[test]
    fn overwrites_oldest_when_full() {
        let mut rb = RingBuffer::new(3);
        rb.push(candle(1, 10.0));
        rb.push(candle(2, 20.0));
        rb.push(candle(3, 30.0));
        rb.push(candle(4, 40.0)); // evicts ts=1
        let out = rb.as_slice();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].ts, 2);
        assert_eq!(out[2].ts, 4);
    }

    #[test]
    fn range_query_filters_correctly() {
        let mut rb = RingBuffer::new(10);
        for i in 1..=10 {
            rb.push(candle(i, i as f64 * 100.0));
        }
        let out = rb.range(3, 6);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0].ts, 3);
        assert_eq!(out[3].ts, 6);
    }

    #[test]
    fn range_returns_empty_for_no_match() {
        let mut rb = RingBuffer::new(5);
        rb.push(candle(1, 1.0));
        rb.push(candle(2, 2.0));
        assert!(rb.range(10, 20).is_empty());
    }

    #[test]
    fn oldest_ts_tracks_correctly_after_wrap() {
        let mut rb = RingBuffer::new(3);
        rb.push(candle(1, 1.0));
        rb.push(candle(2, 2.0));
        rb.push(candle(3, 3.0));
        rb.push(candle(4, 4.0)); // wraps, oldest is now ts=2
        assert_eq!(rb.oldest_ts(), Some(2));
    }
}
