/// Trading signal — 64 bytes, `Copy`, cache-line friendly on x86.
///
/// Transmitted from the strategy engine to the order executor via an in-process
/// `SpscRing<Signal>` at SPSC ring latency (~77 ns).
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Signal {
    pub ts:       i64,       // unix nanoseconds
    pub symbol:   [u8; 24],  // null-terminated ASCII, e.g. "BTCUSDT:1m\0"
    pub side:     u8,        // 0 = Buy, 1 = Sell
    _pad:         [u8; 7],
    pub qty:      f64,       // base quantity
    pub price:    f64,       // limit price; 0.0 = market order
    pub strategy: u32,       // strategy ID for audit log
    _pad2:        [u8; 4],
}

const _: () = assert!(std::mem::size_of::<Signal>() == 64);

impl Signal {
    /// Construct a new signal. `symbol` is silently truncated to 23 bytes.
    pub fn new(
        ts:       i64,
        symbol:   &str,
        side:     Side,
        qty:      f64,
        price:    f64,
        strategy: u32,
    ) -> Self {
        let mut s = Self { ts, side: side as u8, qty, price, strategy, ..Default::default() };
        let bytes = symbol.as_bytes();
        let len   = bytes.len().min(23);
        s.symbol[..len].copy_from_slice(&bytes[..len]);
        s
    }

    /// Decode the symbol field as a `&str`.
    pub fn symbol_str(&self) -> &str {
        let end = self.symbol.iter().position(|&b| b == 0).unwrap_or(24);
        std::str::from_utf8(&self.symbol[..end]).unwrap_or("")
    }

    /// Decode the side field.
    pub fn side(&self) -> Side {
        if self.side == 0 { Side::Buy } else { Side::Sell }
    }
}

/// Order side.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy  = 0,
    Sell = 1,
}
