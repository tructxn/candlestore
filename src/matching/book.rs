use std::collections::{BTreeMap, VecDeque};
use super::{
    event::{CancelReason, Fill, TradeEvent},
    order::{Order, OrderType, Side},
};

fn price_key(p: f64) -> u64  { (p * 1e8).round() as u64 }
fn key_price(k: u64)  -> f64 { k as f64 / 1e8 }

/// Price-time priority order book for one symbol.
pub struct OrderBook {
    pub symbol: String,
    bids: BTreeMap<u64, VecDeque<Order>>, // buy  side — iterate rev() for highest first
    asks: BTreeMap<u64, VecDeque<Order>>, // sell side — iterate fwd  for lowest  first
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self { symbol: symbol.into(), bids: BTreeMap::new(), asks: BTreeMap::new() }
    }

    /// Submit an order. Returns all trade events generated (fills + cancels).
    pub fn submit(&mut self, mut order: Order, ts: i64) -> Vec<TradeEvent> {
        let mut events = Vec::new();

        match order.kind {
            OrderType::Fok => {
                if !self.can_fill(&order) {
                    events.push(TradeEvent::Cancel { order_id: order.id, reason: CancelReason::FokNoFill });
                    return events;
                }
                self.match_order(&mut order, &mut events, ts);
            }
            _ => {
                self.match_order(&mut order, &mut events, ts);
                if !order.is_filled() {
                    match order.kind {
                        OrderType::Limit  => self.rest(order),
                        OrderType::Ioc    => events.push(TradeEvent::Cancel { order_id: order.id, reason: CancelReason::IocRemainder }),
                        OrderType::Market => events.push(TradeEvent::Cancel { order_id: order.id, reason: CancelReason::MarketNoLiquidity }),
                        OrderType::Fok    => unreachable!(),
                    }
                }
            }
        }
        events
    }

    pub fn best_bid(&self) -> Option<f64> { self.bids.keys().next_back().map(|&k| key_price(k)) }
    pub fn best_ask(&self) -> Option<f64> { self.asks.keys().next().map(|&k| key_price(k)) }
    pub fn spread(&self)   -> Option<f64> { Some(self.best_ask()? - self.best_bid()?) }

    // ── internals ─────────────────────────────────────────────────────────────

    fn can_fill(&self, order: &Order) -> bool {
        let needed = order.remaining;
        let avail: f64 = match order.side {
            Side::Buy => {
                let cap = order.price.map(price_key).unwrap_or(u64::MAX);
                self.asks.range(..=cap).flat_map(|(_, q)| q).map(|o| o.remaining).sum()
            }
            Side::Sell => {
                let floor = order.price.map(price_key).unwrap_or(0);
                self.bids.range(floor..).flat_map(|(_, q)| q).map(|o| o.remaining).sum()
            }
        };
        avail >= needed - f64::EPSILON
    }

    fn match_order(&mut self, order: &mut Order, events: &mut Vec<TradeEvent>, ts: i64) {
        match order.side {
            Side::Buy  => self.match_buy(order, events, ts),
            Side::Sell => self.match_sell(order, events, ts),
        }
    }

    fn match_buy(&mut self, order: &mut Order, events: &mut Vec<TradeEvent>, ts: i64) {
        let limit_key = order.price.map(price_key);
        while let Some(best) = self.asks.keys().next().copied() {
            if limit_key.map(|lk| best > lk).unwrap_or(false) { break; }
            self.fill_at(best, order, events, ts);
            if self.asks.get(&best).map(|l| l.is_empty()).unwrap_or(true) { self.asks.remove(&best); }
            if order.is_filled() { break; }
        }
    }

    fn match_sell(&mut self, order: &mut Order, events: &mut Vec<TradeEvent>, ts: i64) {
        let limit_key = order.price.map(price_key);
        while let Some(best) = self.bids.keys().next_back().copied() {
            if limit_key.map(|lk| best < lk).unwrap_or(false) { break; }
            self.fill_at(best, order, events, ts);
            if self.bids.get(&best).map(|l| l.is_empty()).unwrap_or(true) { self.bids.remove(&best); }
            if order.is_filled() { break; }
        }
    }

    fn fill_at(&mut self, key: u64, taker: &mut Order, events: &mut Vec<TradeEvent>, ts: i64) {
        let fill_price = key_price(key);
        let book_side  = match taker.side { Side::Buy => &mut self.asks, Side::Sell => &mut self.bids };
        let level      = match book_side.get_mut(&key) { Some(l) => l, None => return };

        while taker.remaining > f64::EPSILON {
            let maker = match level.front_mut() { Some(m) => m, None => break };
            let qty   = taker.remaining.min(maker.remaining);

            taker.remaining -= qty;
            maker.remaining -= qty;

            events.push(TradeEvent::Fill(Fill { order_id: taker.id, symbol: taker.symbol.clone(), side: taker.side, price: fill_price, qty, ts }));
            events.push(TradeEvent::Fill(Fill { order_id: maker.id, symbol: maker.symbol.clone(), side: maker.side, price: fill_price, qty, ts }));

            if maker.is_filled() { level.pop_front(); }
        }
    }

    fn rest(&mut self, order: Order) {
        let key = price_key(order.price.expect("limit order needs price"));
        match order.side {
            Side::Buy  => self.bids.entry(key).or_default().push_back(order),
            Side::Sell => self.asks.entry(key).or_default().push_back(order),
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::order::Order;

    fn buy_limit(id: u64, price: f64, qty: f64)  -> Order { Order::limit(id, "BTC", Side::Buy,  price, qty, 0) }
    fn sell_limit(id: u64, price: f64, qty: f64) -> Order { Order::limit(id, "BTC", Side::Sell, price, qty, 0) }
    fn buy_market(id: u64, qty: f64)             -> Order { Order::market(id, "BTC", Side::Buy,  qty, 0) }
    #[allow(dead_code)]
    fn sell_market(id: u64, qty: f64)            -> Order { Order::market(id, "BTC", Side::Sell, qty, 0) }

    #[test]
    fn limit_buy_rests_on_empty_book() {
        let mut book = OrderBook::new("BTC");
        let events = book.submit(buy_limit(1, 50_000.0, 1.0), 0);
        assert!(events.is_empty());
        assert_eq!(book.best_bid(), Some(50_000.0));
    }

    #[test]
    fn limit_sell_matches_existing_bid() {
        let mut book = OrderBook::new("BTC");
        book.submit(buy_limit(1, 50_000.0, 1.0), 0);
        let events = book.submit(sell_limit(2, 50_000.0, 1.0), 0);
        let fills: Vec<_> = events.iter().filter(|e| e.is_fill()).collect();
        assert_eq!(fills.len(), 2); // taker fill + maker fill
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn partial_fill_remainder_rests_on_book() {
        let mut book = OrderBook::new("BTC");
        book.submit(sell_limit(1, 50_000.0, 0.5), 0); // 0.5 BTC on ask
        let events = book.submit(buy_limit(2, 50_000.0, 1.0), 0); // wants 1.0
        let fills: Vec<_> = events.iter().filter(|e| e.is_fill()).collect();
        assert_eq!(fills.len(), 2);
        assert_eq!(book.best_bid(), Some(50_000.0)); // 0.5 BTC rests on bid
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn market_buy_sweeps_asks() {
        let mut book = OrderBook::new("BTC");
        book.submit(sell_limit(1, 49_900.0, 0.5), 0);
        book.submit(sell_limit(2, 50_000.0, 0.5), 0);
        let events = book.submit(buy_market(3, 1.0), 0);
        let fills: Vec<_> = events.iter().filter(|e| e.is_fill()).collect();
        assert_eq!(fills.len(), 4); // 2 fills per price level
        assert!(book.best_ask().is_none());
    }

    #[test]
    fn ioc_cancels_remainder() {
        let mut book = OrderBook::new("BTC");
        book.submit(sell_limit(1, 50_000.0, 0.3), 0);
        let events = book.submit(Order::ioc(2, "BTC", Side::Buy, 50_000.0, 1.0, 0), 0);
        let cancels: Vec<_> = events.iter().filter(|e| !e.is_fill()).collect();
        assert_eq!(cancels.len(), 1);
    }

    #[test]
    fn fok_cancels_if_insufficient_liquidity() {
        let mut book = OrderBook::new("BTC");
        book.submit(sell_limit(1, 50_000.0, 0.3), 0);
        let events = book.submit(Order::fok(2, "BTC", Side::Buy, 50_000.0, 1.0, 0), 0);
        assert_eq!(events.len(), 1);
        assert!(!events[0].is_fill()); // only a cancel, no fills
        assert_eq!(book.best_ask(), Some(50_000.0)); // original order untouched
    }

    #[test]
    fn fok_fully_fills_when_sufficient_liquidity() {
        let mut book = OrderBook::new("BTC");
        book.submit(sell_limit(1, 50_000.0, 2.0), 0);
        let events = book.submit(Order::fok(2, "BTC", Side::Buy, 50_000.0, 1.0, 0), 0);
        let fills: Vec<_> = events.iter().filter(|e| e.is_fill()).collect();
        assert_eq!(fills.len(), 2);
    }

    #[test]
    fn price_time_priority_fifo_at_same_price() {
        let mut book = OrderBook::new("BTC");
        book.submit(sell_limit(1, 50_000.0, 0.5), 0); // first
        book.submit(sell_limit(2, 50_000.0, 0.5), 0); // second
        let events = book.submit(buy_market(3, 0.5), 0);
        let fills: Vec<_> = events.iter().filter_map(|e| {
            if let TradeEvent::Fill(f) = e { Some(f.order_id) } else { None }
        }).collect();
        // maker fill should be order 1 (FIFO)
        assert!(fills.contains(&1));
    }
}
