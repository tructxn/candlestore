pub type OrderId = u64;

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
    pub fn is_filled(&self) -> bool { self.remaining <= f64::EPSILON }
}
