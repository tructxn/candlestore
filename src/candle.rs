/// OHLCV candle — 48 bytes, `#[repr(C)]`.
///
/// Layout trade-off: a 48-byte stride packs arrays ~25% denser in L3 than a
/// cache-line-padded 64-byte layout, at the cost that half of consecutive
/// candles straddle a 64-byte line boundary. For the scan-heavy access
/// patterns here (range copies, rolling SMA windows) density wins; random
/// single-candle access occasionally pays an extra line fill.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct Candle {
    /// Unix timestamp in nanoseconds (positive only — negative ts is rejected by [`Candle::is_valid`]).
    pub ts:     i64,
    /// Open price.
    pub open:   f64,
    /// High price.
    pub high:   f64,
    /// Low price.
    pub low:    f64,
    /// Close price.
    pub close:  f64,
    /// Volume traded.
    pub volume: f64,
}

impl Candle {
    /// Returns `true` if all numeric fields are finite (no NaN, no Infinity)
    /// and ts is non-negative.
    ///
    /// **NOT enforced inside `RingBuffer::push`** — that hot path is one
    /// atomic store and the validation belongs at the ingestion boundary
    /// (the SHM ingester, the exchange feed parser, the FFI surface).
    /// Once an invalid Candle lands in the ring, every downstream computation
    /// (SMAs, executor positions, metrics) silently propagates NaN forever.
    ///
    /// Use this from:
    /// - `ShmIngester` before forwarding into the store
    /// - `feed::BinanceFeed` after parsing each kline
    /// - `CandleStore::try_append` / `append` if you accept untrusted input
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.ts >= 0
            && self.open.is_finite()
            && self.high.is_finite()
            && self.low.is_finite()
            && self.close.is_finite()
            && self.volume.is_finite()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> Candle {
        Candle { ts: 1, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1.0 }
    }

    #[test] fn default_candle_is_valid() { assert!(Candle::default().is_valid()); }
    #[test] fn typical_candle_is_valid() { assert!(good().is_valid()); }

    #[test]
    fn nan_close_rejected() {
        let mut c = good(); c.close = f64::NAN; assert!(!c.is_valid());
    }

    #[test]
    fn infinite_volume_rejected() {
        let mut c = good(); c.volume = f64::INFINITY; assert!(!c.is_valid());
        c.volume = f64::NEG_INFINITY; assert!(!c.is_valid());
    }

    #[test]
    fn negative_ts_rejected() {
        let mut c = good(); c.ts = -1; assert!(!c.is_valid());
    }

    #[test]
    fn every_field_checked() {
        for set in [
            |c: &mut Candle| c.open   = f64::NAN,
            |c: &mut Candle| c.high   = f64::NAN,
            |c: &mut Candle| c.low    = f64::NAN,
            |c: &mut Candle| c.close  = f64::NAN,
            |c: &mut Candle| c.volume = f64::NAN,
        ] {
            let mut c = good();
            set(&mut c);
            assert!(!c.is_valid(), "expected rejection after mutation");
        }
    }
}
