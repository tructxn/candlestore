use crate::Candle;

/// Outcome of [`RingBuffer::push`]. The candle is stored either way; the
/// variant reports whether the strict monotonic-ts invariant was honoured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "out-of-order pushes should be counted/logged at the store layer"]
pub enum PushOutcome {
    /// Candle stored; ts is >= the previously-stored ts (or this was the first push).
    Ok,
    /// Candle stored, but its ts is *strictly less than* the previously-stored
    /// ts. The binary-search `range` query may not find it. Real feeds
    /// occasionally do this (Binance late klines); the store records the
    /// event in `out_of_order_total` so operators can decide whether to
    /// run a re-sort or wear the discrepancy.
    OutOfOrder { prev_ts: i64, this_ts: i64 },
}

/// Fixed-capacity ring buffer for candles.
///
/// Oldest entry is overwritten when full. Range queries assume candles were
/// inserted in monotonically non-decreasing `ts` order — that's what makes
/// them `O(log n)` instead of `O(n)`. Pushes are still accepted out-of-order
/// (the underlying storage doesn't care), but the caller is informed via
/// the [`PushOutcome`] return so it can count the event.
pub struct RingBuffer {
    buf:     Box<[Candle]>,
    head:    usize,   // next write position
    len:     usize,   // current count (≤ capacity)
    last_ts: Option<i64>,
}

impl RingBuffer {
    /// Create a ring buffer with the given fixed `capacity`.
    pub fn new(capacity: usize) -> Self {
        let candle = Candle { ts: 0, open: 0.0, high: 0.0, low: 0.0, close: 0.0, volume: 0.0 };
        Self {
            buf:     vec![candle; capacity].into_boxed_slice(),
            head:    0,
            len:     0,
            last_ts: None,
        }
    }

    /// Push a candle. Returns [`PushOutcome::OutOfOrder`] if this candle's ts
    /// is strictly less than the previously-pushed candle's ts.
    pub fn push(&mut self, c: Candle) -> PushOutcome {
        let outcome = match self.last_ts {
            Some(prev) if c.ts < prev => PushOutcome::OutOfOrder { prev_ts: prev, this_ts: c.ts },
            _                          => PushOutcome::Ok,
        };
        self.buf[self.head] = c;
        self.head = (self.head + 1) % self.buf.len();
        if self.len < self.buf.len() { self.len += 1; }
        self.last_ts = Some(c.ts);
        outcome
    }

    // ── physical layout helpers ──────────────────────────────────────────────
    //
    // The ring stores candles in two layouts:
    //   - Not full (len < cap): logical [0..len] == physical buf[0..len]
    //   - Full   (len == cap): logical[0] == physical buf[head] (oldest);
    //                          subsequent logical positions wrap modulo cap.
    //
    // All public APIs operate on *logical* indices where 0 = oldest.

    /// Physical buffer index for logical position `i`.
    /// Caller must ensure `i < self.len`.
    #[inline]
    fn physical_idx(&self, i: usize) -> usize {
        let cap = self.buf.len();
        let start = if self.len < cap { 0 } else { self.head };
        let raw = start + i;
        if raw >= cap { raw - cap } else { raw }
    }

    /// Timestamp at logical position `i`. Caller must ensure `i < self.len`.
    #[inline]
    fn ts_at(&self, i: usize) -> i64 {
        self.buf[self.physical_idx(i)].ts
    }

    /// Smallest logical index `i` such that `ts_at(i) >= threshold`,
    /// or `self.len` if no such index exists. O(log n).
    fn lower_bound_idx(&self, threshold: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.ts_at(mid) < threshold {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Smallest logical index `i` such that `ts_at(i) > threshold`,
    /// or `self.len` if all candles have `ts <= threshold`. O(log n).
    fn upper_bound_idx(&self, threshold: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.ts_at(mid) <= threshold {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Copy logical range `[lo, hi)` into a fresh `Vec`. Handles ring wrap.
    fn copy_logical_range(&self, lo: usize, hi: usize) -> Vec<Candle> {
        debug_assert!(lo <= hi && hi <= self.len);
        if lo == hi { return Vec::new(); }

        let cap = self.buf.len();
        let start_phys = self.physical_idx(lo);
        let count = hi - lo;
        let mut out = Vec::with_capacity(count);

        // Either the whole logical range is contiguous in physical memory,
        // or it wraps once at `cap`. Use two `extend_from_slice` calls so we
        // get a memcpy per contiguous chunk instead of a per-element copy.
        if start_phys + count <= cap {
            out.extend_from_slice(&self.buf[start_phys..start_phys + count]);
        } else {
            let first = cap - start_phys;
            out.extend_from_slice(&self.buf[start_phys..cap]);
            out.extend_from_slice(&self.buf[0..count - first]);
        }
        out
    }

    // ── public API ───────────────────────────────────────────────────────────

    /// Copy the entire logical contents into a fresh `Vec`, oldest first.
    /// O(n) memcpy (one or two contiguous chunks). Named `to_vec` because it
    /// allocates — a borrowed slice is impossible once the ring has wrapped.
    pub fn to_vec(&self) -> Vec<Candle> {
        self.copy_logical_range(0, self.len)
    }

    /// Range query — returns candles where `from_ts <= ts <= to_ts`.
    /// O(log n + k) where k is the result size. Relies on monotonic ts.
    pub fn range(&self, from_ts: i64, to_ts: i64) -> Vec<Candle> {
        if self.len == 0 || from_ts > to_ts { return Vec::new(); }
        let lo = self.lower_bound_idx(from_ts);
        let hi = self.upper_bound_idx(to_ts);
        self.copy_logical_range(lo, hi)
    }

    /// Returns the last `n` candles (newest), or all if `n > len`.
    /// O(min(n, len)) memcpy — never scans the full buffer.
    pub fn last_n(&self, n: usize) -> Vec<Candle> {
        let take = n.min(self.len);
        if take == 0 { return Vec::new(); }
        self.copy_logical_range(self.len - take, self.len)
    }

    pub fn len(&self) -> usize { self.len }
    pub fn capacity(&self) -> usize { self.buf.len() }
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// Oldest timestamp in buffer, if any. O(1).
    pub fn oldest_ts(&self) -> Option<i64> {
        if self.len == 0 { None } else { Some(self.ts_at(0)) }
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
        let _ = rb.push(candle(1, 10.0));
        let _ = rb.push(candle(2, 20.0));
        let _ = rb.push(candle(3, 30.0));
        let out = rb.to_vec();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].ts, 1);
        assert_eq!(out[2].ts, 3);
    }

    #[test]
    fn overwrites_oldest_when_full() {
        let mut rb = RingBuffer::new(3);
        let _ = rb.push(candle(1, 10.0));
        let _ = rb.push(candle(2, 20.0));
        let _ = rb.push(candle(3, 30.0));
        let _ = rb.push(candle(4, 40.0)); // evicts ts=1
        let out = rb.to_vec();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].ts, 2);
        assert_eq!(out[2].ts, 4);
    }

    #[test]
    fn range_query_filters_correctly() {
        let mut rb = RingBuffer::new(10);
        for i in 1..=10 {
            let _ = rb.push(candle(i, i as f64 * 100.0));
        }
        let out = rb.range(3, 6);
        assert_eq!(out.len(), 4);
        assert_eq!(out[0].ts, 3);
        assert_eq!(out[3].ts, 6);
    }

    #[test]
    fn range_returns_empty_for_no_match() {
        let mut rb = RingBuffer::new(5);
        let _ = rb.push(candle(1, 1.0));
        let _ = rb.push(candle(2, 2.0));
        assert!(rb.range(10, 20).is_empty());
    }

    #[test]
    fn oldest_ts_tracks_correctly_after_wrap() {
        let mut rb = RingBuffer::new(3);
        let _ = rb.push(candle(1, 1.0));
        let _ = rb.push(candle(2, 2.0));
        let _ = rb.push(candle(3, 3.0));
        let _ = rb.push(candle(4, 4.0)); // wraps, oldest is now ts=2
        assert_eq!(rb.oldest_ts(), Some(2));
    }

    // ── binary-search range correctness ─────────────────────────────────────

    #[test]
    fn range_returns_empty_on_inverted_bounds() {
        let mut rb = RingBuffer::new(4);
        let _ = rb.push(candle(1, 1.0));
        let _ = rb.push(candle(2, 2.0));
        assert!(rb.range(10, 5).is_empty()); // from > to
    }

    #[test]
    fn range_inclusive_on_both_ends() {
        let mut rb = RingBuffer::new(8);
        for i in 1..=5i64 { let _ = rb.push(candle(i, i as f64)); }
        let r = rb.range(2, 4);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].ts, 2);
        assert_eq!(r[2].ts, 4);
    }

    #[test]
    fn range_handles_duplicate_timestamps() {
        let mut rb = RingBuffer::new(8);
        let _ = rb.push(candle(1, 1.0));
        let _ = rb.push(candle(2, 2.0));
        let _ = rb.push(candle(2, 2.1));
        let _ = rb.push(candle(2, 2.2));
        let _ = rb.push(candle(3, 3.0));
        let r = rb.range(2, 2);
        assert_eq!(r.len(), 3);
        assert!(r.iter().all(|c| c.ts == 2));
    }

    #[test]
    fn range_works_across_ring_wrap() {
        let mut rb = RingBuffer::new(4);
        // push 7 candles into a capacity-4 ring — wraps once
        for i in 1..=7i64 { let _ = rb.push(candle(i, i as f64)); }
        // logical content should be ts=4,5,6,7
        let r = rb.range(0, i64::MAX);
        assert_eq!(r.len(), 4);
        assert_eq!(r[0].ts, 4);
        assert_eq!(r[3].ts, 7);

        let mid = rb.range(5, 6);
        assert_eq!(mid.len(), 2);
        assert_eq!(mid[0].ts, 5);
        assert_eq!(mid[1].ts, 6);
    }

    #[test]
    fn range_window_outside_returns_empty() {
        let mut rb = RingBuffer::new(4);
        for i in 1..=3i64 { let _ = rb.push(candle(i, i as f64)); }
        assert!(rb.range(100, 200).is_empty()); // entirely above
        assert!(rb.range(-50, -1).is_empty());  // entirely below
    }

    #[test]
    fn last_n_returns_newest_candles_in_order() {
        let mut rb = RingBuffer::new(8);
        for i in 1..=5i64 { let _ = rb.push(candle(i, i as f64)); }
        let last = rb.last_n(3);
        assert_eq!(last.len(), 3);
        assert_eq!(last[0].ts, 3);
        assert_eq!(last[2].ts, 5);
    }

    #[test]
    fn last_n_caps_at_len() {
        let mut rb = RingBuffer::new(4);
        let _ = rb.push(candle(1, 1.0));
        let _ = rb.push(candle(2, 2.0));
        let last = rb.last_n(100);
        assert_eq!(last.len(), 2);
        assert_eq!(last[0].ts, 1);
    }

    #[test]
    fn last_n_works_across_ring_wrap() {
        let mut rb = RingBuffer::new(4);
        for i in 1..=7i64 { let _ = rb.push(candle(i, i as f64)); }
        // ring holds ts=4..=7 logically; last 2 should be ts=6,7
        let last = rb.last_n(2);
        assert_eq!(last.len(), 2);
        assert_eq!(last[0].ts, 6);
        assert_eq!(last[1].ts, 7);
    }

    #[test]
    fn last_n_zero_is_empty() {
        let mut rb = RingBuffer::new(4);
        let _ = rb.push(candle(1, 1.0));
        assert!(rb.last_n(0).is_empty());
    }

    #[test]
    fn oldest_ts_is_o1_after_wrap() {
        // smoke test that oldest_ts no longer allocates: build a giant ring
        // and call it repeatedly. If it were O(n) per call this would crawl.
        let mut rb = RingBuffer::new(100_000);
        for i in 1..=200_000i64 { let _ = rb.push(candle(i, i as f64)); }
        for _ in 0..10_000 { assert_eq!(rb.oldest_ts(), Some(100_001)); }
    }

    // ── out-of-order detection ─────────────────────────────────────────────

    #[test]
    fn push_returns_ok_for_monotonic_inserts() {
        let mut rb = RingBuffer::new(8);
        assert_eq!(rb.push(candle(1, 1.0)), PushOutcome::Ok);
        assert_eq!(rb.push(candle(2, 2.0)), PushOutcome::Ok);
        assert_eq!(rb.push(candle(2, 2.5)), PushOutcome::Ok, "equal ts is monotonic");
        assert_eq!(rb.push(candle(3, 3.0)), PushOutcome::Ok);
    }

    #[test]
    fn push_detects_out_of_order() {
        let mut rb = RingBuffer::new(8);
        let _ = rb.push(candle(10, 1.0));
        let _ = rb.push(candle(20, 2.0));
        // ts=15 is < prev (20) → must surface as OutOfOrder.
        assert_eq!(
            rb.push(candle(15, 1.5)),
            PushOutcome::OutOfOrder { prev_ts: 20, this_ts: 15 }
        );
    }

    #[test]
    fn out_of_order_candle_is_still_stored() {
        // RingBuffer never refuses a push; the OOO candle lands in the buffer
        // even though it may be invisible to the binary-search `range`.
        let mut rb = RingBuffer::new(8);
        let _ = rb.push(candle(10, 1.0));
        let _ = rb.push(candle(20, 2.0));
        let _ = rb.push(candle(15, 1.5));
        assert_eq!(rb.len(), 3);
        // last_n returns physical-insertion order, so OOO candle is at index 2.
        let last = rb.last_n(3);
        assert_eq!(last.iter().map(|c| c.ts).collect::<Vec<_>>(), vec![10, 20, 15]);
    }
}
