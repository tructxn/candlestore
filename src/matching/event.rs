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
pub enum CancelReason { IocRemainder, FokNoFill, MarketNoLiquidity }

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
