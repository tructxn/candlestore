pub type OrderId = u64;

/// Relative tolerance for treating a floating-point quantity as zero.
///
/// An *absolute* `f64::EPSILON` (≈2.2e-16) threshold is scale-wrong: f64 has
/// ~16 significant digits, so after sequential `remaining -= qty` at qty ~1e3
/// the residual dust is ~1e-13 — far above `EPSILON` — and orders were never
/// considered filled (ghost dust resting on the book, FOK falsely rejected,
/// spurious `MarketNoLiquidity` cancels). Anchoring the check to the order's
/// *original* size makes it scale-invariant: anything below one part in 1e9
/// of the original quantity is dust.
pub(crate) const QTY_REL_TOL: f64 = 1e-9;

/// Scale-aware "is this remaining quantity effectively zero?" check.
/// `original` anchors the tolerance to the order's own magnitude — the one
/// crate-wide replacement for the old absolute-epsilon comparisons.
pub(crate) fn qty_is_zero(remaining: f64, original: f64) -> bool {
    remaining <= original * QTY_REL_TOL
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Buy, Sell }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType { Limit, Market, Ioc, Fok }

#[derive(Debug, Clone)]
pub struct Order {
    pub id:        OrderId,
    pub symbol:    String,
    pub side:      Side,
    pub kind:      OrderType,
    pub price:     Option<f64>,  // None for Market
    pub qty:       f64,
    pub remaining: f64,
    pub ts:        i64,
}

impl Order {
    pub fn limit(id: OrderId, symbol: &str, side: Side, price: f64, qty: f64, ts: i64) -> Self {
        Self { id, symbol: symbol.into(), side, kind: OrderType::Limit,
               price: Some(price), qty, remaining: qty, ts }
    }
    pub fn market(id: OrderId, symbol: &str, side: Side, qty: f64, ts: i64) -> Self {
        Self { id, symbol: symbol.into(), side, kind: OrderType::Market,
               price: None, qty, remaining: qty, ts }
    }
    pub fn ioc(id: OrderId, symbol: &str, side: Side, price: f64, qty: f64, ts: i64) -> Self {
        Self { id, symbol: symbol.into(), side, kind: OrderType::Ioc,
               price: Some(price), qty, remaining: qty, ts }
    }
    pub fn fok(id: OrderId, symbol: &str, side: Side, price: f64, qty: f64, ts: i64) -> Self {
        Self { id, symbol: symbol.into(), side, kind: OrderType::Fok,
               price: Some(price), qty, remaining: qty, ts }
    }
    /// Filled when the remaining quantity is dust relative to the original
    /// size (see [`QTY_REL_TOL`] for why absolute epsilon was wrong here).
    pub fn is_filled(&self) -> bool { qty_is_zero(self.remaining, self.qty) }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_filled_tolerates_scale_proportional_dust() {
        // Residual from sequential subtraction at 1e6 scale is ~1e-10..1e-7:
        // far above f64::EPSILON, but pure dust relative to the order size.
        let mut o = Order::market(1, "BTC", Side::Buy, 1_000_000.0, 0);
        o.remaining = 1e-7;
        assert!(o.is_filled());
    }

    #[test]
    fn is_filled_rejects_real_remainder() {
        // One whole unit out of 1e6 is a real remainder, not dust.
        let mut o = Order::market(1, "BTC", Side::Buy, 1_000_000.0, 0);
        o.remaining = 1.0;
        assert!(!o.is_filled());
    }
}
