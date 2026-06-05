/// OHLCV candle — 48 bytes, cache-line friendly
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[repr(C)]
pub struct Candle {
    pub ts:     i64,   // unix timestamp ms
    pub open:   f64,
    pub high:   f64,
    pub low:    f64,
    pub close:  f64,
    pub volume: f64,
}
