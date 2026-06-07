use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;
use super::{
    event::{CancelReason, Fill, TradeEvent},
    order::{Order, OrderType, Side},
};

/// Errors from order-book operations that cannot be carried as a Cancel event.
///
/// These are *programming/data* errors (bad input) rather than market events
/// (no fill). The fire-and-forget [`OrderBook::submit`] surfaces them as
/// `CancelReason::InvalidOrder`; strict callers should use [`OrderBook::try_submit`]
/// to receive the precise variant.
#[derive(Debug, Clone, Copy, PartialEq, Error)]
pub enum BookError {
    /// Price was NaN or Infinity.
    #[error("price is not finite")]
    PriceNotFinite,
    /// Price was negative or zero (note: zero is technically valid for some
    /// instruments but the book uses it as the "market order" sentinel).
    #[error("price must be > 0, got {0}")]
    PriceNonPositive(f64),
    /// Price exceeded the representable range for the fixed-point u64 key.
    /// We multiply by 1e8 (8-decimal precision) so the maximum representable
    /// price is `u64::MAX / 1e8 ≈ 1.84e11`. Crypto prices are nowhere near
    /// this; if you see this error, either the input is bad or the
    /// instrument needs different precision.
    #[error("price {0} exceeds maximum representable ({MAX_PRICE})")]
    PriceOutOfRange(f64),
    /// Quantity was NaN, Infinity, negative, or zero.
    #[error("quantity must be finite and > 0, got {0}")]
    QuantityInvalid(f64),
}

/// Decimal precision of the integer price key. `(price * PRICE_SCALE).round() as u64`.
const PRICE_SCALE: f64 = 1e8;
/// Largest price representable as a u64 key without saturating.
/// `u64::MAX / 1e8 ≈ 1.844e11`.
pub const MAX_PRICE: f64 = (u64::MAX as f64) / PRICE_SCALE;

/// Convert a floating-point price to the BTreeMap key with full bounds
/// checking. Replaces the old `(p * 1e8).round() as u64` which silently
/// saturated to `u64::MAX` for prices over ~92 billion and was undefined
/// for NaN/Infinity.
fn try_price_key(p: f64) -> Result<u64, BookError> {
    if !p.is_finite() {
        return Err(BookError::PriceNotFinite);
    }
    if p <= 0.0 {
        return Err(BookError::PriceNonPositive(p));
    }
    if p > MAX_PRICE {
        return Err(BookError::PriceOutOfRange(p));
    }
    Ok((p * PRICE_SCALE).round() as u64)
}

/// Infallible variant for code paths that have already validated. Panics if
/// the input would fail [`try_price_key`] — only use in internal positions
/// where we KNOW the price came from a validated source.
fn price_key(p: f64) -> u64 {
    try_price_key(p).expect("internal: price_key called with unvalidated input")
}

fn key_price(k: u64) -> f64 { k as f64 / PRICE_SCALE }

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

    /// Validate that an order's price (if priced) and quantity are
    /// representable. Quoted price = `0.0` is allowed for market orders
    /// (the convention) but otherwise the price must pass [`try_price_key`].
    fn validate_order(&self, order: &Order) -> Result<(), BookError> {
        if !order.qty.is_finite() || order.qty <= 0.0 {
            return Err(BookError::QuantityInvalid(order.qty));
        }
        if let Some(p) = order.price {
            // For market orders, price is None — skipped here.
            try_price_key(p)?;
        }
        Ok(())
    }

    /// Strict submit. Returns `Err(BookError)` on invalid input (NaN/Inf,
    /// negative, out of range). On success returns the trade events as
    /// usual. Prefer this anywhere the order comes from untrusted input —
    /// strategy outputs, FFI, user submissions.
    pub fn try_submit(&mut self, order: Order, ts: i64) -> Result<Vec<TradeEvent>, BookError> {
        self.validate_order(&order)?;
        Ok(self.submit_validated(order, ts))
    }

    /// Fire-and-forget submit. Validates first; on invalid input emits
    /// `CancelReason::InvalidOrder` + `tracing::error!` (with the precise
    /// `BookError` variant in the log) rather than panicking. Existing
    /// callers (tests, internal demos) keep working but get the safety net.
    pub fn submit(&mut self, order: Order, ts: i64) -> Vec<TradeEvent> {
        if let Err(e) = self.validate_order(&order) {
            tracing::error!(order_id = order.id, symbol = %self.symbol, error = %e,
                "order rejected at submit — invalid price or quantity");
            return vec![TradeEvent::Cancel {
                order_id: order.id,
                reason:   CancelReason::InvalidOrder,
            }];
        }
        self.submit_validated(order, ts)
    }

    /// Internal matching path, called after validation has succeeded.
    fn submit_validated(&mut self, mut order: Order, ts: i64) -> Vec<TradeEvent> {
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

    // ── price-quantization safety ──────────────────────────────────────────

    #[test]
    fn try_price_key_accepts_typical_crypto_prices() {
        assert!(try_price_key(50_000.0).is_ok());
        assert!(try_price_key(0.000_000_01).is_ok()); // 1 satoshi
        assert!(try_price_key(MAX_PRICE).is_ok());
    }

    #[test]
    fn try_price_key_rejects_nan() {
        assert_eq!(try_price_key(f64::NAN), Err(BookError::PriceNotFinite));
    }

    #[test]
    fn try_price_key_rejects_infinity() {
        assert_eq!(try_price_key(f64::INFINITY), Err(BookError::PriceNotFinite));
        assert_eq!(try_price_key(f64::NEG_INFINITY), Err(BookError::PriceNotFinite));
    }

    #[test]
    fn try_price_key_rejects_zero_or_negative() {
        assert!(matches!(try_price_key(0.0), Err(BookError::PriceNonPositive(_))));
        assert!(matches!(try_price_key(-1.0), Err(BookError::PriceNonPositive(_))));
    }

    #[test]
    fn try_price_key_rejects_overflow() {
        // 1e15 * 1e8 = 1e23 > u64::MAX → would saturate silently in the old impl.
        assert!(matches!(try_price_key(1e15), Err(BookError::PriceOutOfRange(_))));
    }

    #[test]
    fn try_submit_returns_err_for_nan_price() {
        let mut book = OrderBook::new("BTC");
        let bad = Order::limit(1, "BTC", Side::Buy, f64::NAN, 1.0, 0);
        match book.try_submit(bad, 0) {
            Err(BookError::PriceNotFinite) => {}
            other => panic!("expected PriceNotFinite, got {other:?}"),
        }
    }

    #[test]
    fn try_submit_returns_err_for_zero_qty() {
        let mut book = OrderBook::new("BTC");
        let bad = Order::limit(1, "BTC", Side::Buy, 100.0, 0.0, 0);
        assert!(matches!(book.try_submit(bad, 0), Err(BookError::QuantityInvalid(_))));
    }

    #[test]
    fn submit_emits_invalid_order_cancel_instead_of_panic() {
        // The fire-and-forget submit() must not panic on bad input.
        // The old `(p * 1e8).round() as u64` for NaN was undefined behaviour
        // bordering on saturation; for negative it wrapped silently. This
        // test pins down the new safety net.
        let mut book = OrderBook::new("BTC");
        let bad = Order::limit(1, "BTC", Side::Buy, f64::NAN, 1.0, 0);
        let events = book.submit(bad, 0);
        assert_eq!(events.len(), 1);
        match &events[0] {
            TradeEvent::Cancel { reason: CancelReason::InvalidOrder, order_id } => {
                assert_eq!(*order_id, 1);
            }
            other => panic!("expected InvalidOrder cancel, got {other:?}"),
        }
    }
}
