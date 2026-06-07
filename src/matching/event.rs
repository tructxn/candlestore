use super::order::{OrderId, Side};

#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: OrderId,
    pub symbol:   String,
    pub side:     Side,
    pub price:    f64,
    pub qty:      f64,
    pub ts:       i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// IOC order had remaining unfilled qty after matching.
    IocRemainder,
    /// FOK order could not be fully filled at submission time.
    FokNoFill,
    /// Market order ran out of opposing liquidity.
    MarketNoLiquidity,
    /// Order was rejected before matching because its price/qty failed
    /// validation (NaN/Inf, negative, out of representable range, etc.).
    /// See [`BookError`](super::book::BookError) for the variant detail
    /// that the caller of `try_submit` receives directly.
    InvalidOrder,
}

#[derive(Debug, Clone)]
pub enum TradeEvent {
    Fill(Fill),
    Cancel { order_id: OrderId, reason: CancelReason },
}

impl TradeEvent {
    pub fn order_id(&self) -> OrderId {
        match self { Self::Fill(f) => f.order_id, Self::Cancel { order_id, .. } => *order_id }
    }
    pub fn is_fill(&self) -> bool { matches!(self, Self::Fill(_)) }
}
