/// Maximum symbol length encodable in a [`Signal`]. The fixed [u8; 24] buffer
/// reserves the last byte as a null terminator (for C-side decoders that
/// expect zero-terminated strings).
pub const MAX_SIGNAL_SYMBOL_LEN: usize = 23;

/// Error from [`Signal::try_new`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SignalError {
    /// Symbol exceeded [`MAX_SIGNAL_SYMBOL_LEN`].
    ///
    /// Silent truncation to 23 bytes would produce a *valid-looking but wrong*
    /// symbol like `ETH-USD-29MAR2027-CALL-` (truncated from
    /// `ETH-USD-29MAR2027-CALL-3000`) — exactly the failure mode that's hard
    /// to spot in trade logs. `try_new` refuses to encode it.
    #[error("signal symbol too long: {len} bytes (max {max})")]
    SymbolTooLong { len: usize, max: usize },
    /// `qty` or `price` is NaN or Infinity. Would propagate into executor
    /// position accounting and risk gates as NaN. Refused at construction.
    #[error("signal field not finite: qty={qty} price={price}")]
    NonFiniteField { qty: f64, price: f64 },
}

/// Trading signal — 64 bytes, `Copy`, cache-line friendly on x86.
///
/// Transmitted from the strategy engine to the order executor via an in-process
/// `SpscRing<Signal>` at SPSC ring latency (~77 ns).
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Signal {
    /// Unix nanoseconds at which the strategy emitted the signal.
    pub ts: i64,
    /// Symbol identifier as null-terminated ASCII, e.g. `"BTCUSDT:1m\0"`.
    pub symbol: [u8; 24],
    /// 0 = [`Side::Buy`], 1 = [`Side::Sell`]. Decoded via [`Signal::side`].
    pub side: u8,
    _pad: [u8; 7],
    /// Base quantity to trade.
    pub qty: f64,
    /// Limit price; `0.0` means market order.
    pub price: f64,
    /// Strategy ID for the audit log.
    pub strategy: u32,
    _pad2: [u8; 4],
}

const _: () = assert!(std::mem::size_of::<Signal>() == 64);

impl Signal {
    /// Strict constructor. Returns:
    /// - [`SignalError::SymbolTooLong`] if `symbol` exceeds
    ///   [`MAX_SIGNAL_SYMBOL_LEN`] bytes.
    /// - [`SignalError::NonFiniteField`] if `qty` or `price` is NaN/Infinity.
    ///
    /// Prefer this over [`new`](Self::new) anywhere the symbol or numerics
    /// come from outside trusted code — exchange feeds, user input, strategy
    /// configuration.
    pub fn try_new(
        ts:       i64,
        symbol:   &str,
        side:     Side,
        qty:      f64,
        price:    f64,
        strategy: u32,
    ) -> Result<Self, SignalError> {
        if symbol.len() > MAX_SIGNAL_SYMBOL_LEN {
            return Err(SignalError::SymbolTooLong {
                len: symbol.len(),
                max: MAX_SIGNAL_SYMBOL_LEN,
            });
        }
        if !qty.is_finite() || !price.is_finite() {
            return Err(SignalError::NonFiniteField { qty, price });
        }
        let mut s = Self { ts, side: side as u8, qty, price, strategy, ..Default::default() };
        let bytes = symbol.as_bytes();
        s.symbol[..bytes.len()].copy_from_slice(bytes);
        Ok(s)
    }

    /// Lossy constructor — truncates a symbol longer than
    /// [`MAX_SIGNAL_SYMBOL_LEN`] and emits `tracing::warn!`. Kept for
    /// strategies whose symbol list is known-bounded at compile time
    /// (where the error case is unreachable and a Result is just noise).
    /// All other callers should use [`try_new`](Self::try_new).
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
        let len   = bytes.len().min(MAX_SIGNAL_SYMBOL_LEN);
        if bytes.len() > MAX_SIGNAL_SYMBOL_LEN {
            tracing::warn!(
                actual_len = bytes.len(),
                max_len = MAX_SIGNAL_SYMBOL_LEN,
                truncated_to = %std::str::from_utf8(&bytes[..len]).unwrap_or("?"),
                "Signal::new truncated symbol — prefer try_new for stricter semantics"
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_new_accepts_max_length_symbol() {
        let s = "X".repeat(MAX_SIGNAL_SYMBOL_LEN);
        let sig = Signal::try_new(0, &s, Side::Buy, 1.0, 0.0, 0).unwrap();
        assert_eq!(sig.symbol_str(), s);
    }

    #[test]
    fn try_new_rejects_oversize_symbol() {
        let s = "X".repeat(MAX_SIGNAL_SYMBOL_LEN + 1);
        match Signal::try_new(0, &s, Side::Buy, 1.0, 0.0, 0) {
            Err(SignalError::SymbolTooLong { len, max }) => {
                assert_eq!(len, MAX_SIGNAL_SYMBOL_LEN + 1);
                assert_eq!(max, MAX_SIGNAL_SYMBOL_LEN);
            }
            other => panic!("expected SymbolTooLong, got {other:?}"),
        }
    }

    #[test]
    fn try_new_rejects_nan_qty() {
        match Signal::try_new(0, "BTC", Side::Buy, f64::NAN, 0.0, 0) {
            Err(SignalError::NonFiniteField { .. }) => {}
            other => panic!("expected NonFiniteField, got {other:?}"),
        }
    }

    #[test]
    fn try_new_rejects_inf_price() {
        match Signal::try_new(0, "BTC", Side::Sell, 1.0, f64::INFINITY, 0) {
            Err(SignalError::NonFiniteField { .. }) => {}
            other => panic!("expected NonFiniteField, got {other:?}"),
        }
    }

    #[test]
    fn new_truncates_lossy_but_does_not_panic() {
        let s = "X".repeat(40);
        let sig = Signal::new(0, &s, Side::Buy, 1.0, 0.0, 0);
        assert_eq!(sig.symbol_str().len(), MAX_SIGNAL_SYMBOL_LEN);
    }
}

/// Order side.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy  = 0,
    Sell = 1,
}
