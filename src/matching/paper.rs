use std::sync::atomic::{AtomicU64, Ordering};
use crate::Candle;
use super::{
    book::{validate_order, BookError},
    event::{CancelReason, Fill, TradeEvent},
    order::{Order, OrderId, OrderType, Side},
    portfolio::Portfolio,
};

/// Paper trading engine — simulates order fills against candle OHLCV data.
///
/// Fill rules:
///   Market      → fills at candle.open
///   Limit buy   → fills if candle.low  ≤ price  (fill price = min(price, open))
///   Limit sell  → fills if candle.high ≥ price  (fill price = max(price, open))
///   IOC/FOK     → same as Limit but cancel remainder at end of candle
pub struct PaperEngine {
    pub portfolio: Portfolio,
    pending:       Vec<Order>,
    next_id:       AtomicU64,
}

impl PaperEngine {
    pub fn new(initial_cash: f64) -> Self {
        Self {
            portfolio: Portfolio::new(initial_cash),
            pending:   Vec::new(),
            next_id:   AtomicU64::new(1),
        }
    }

    /// Strict submit. Validates price/qty with the same rules as
    /// [`OrderBook::try_submit`](super::book::OrderBook::try_submit) and
    /// returns `Err(BookError)` on NaN/Inf or non-positive input, so a
    /// poison order can never rest as pending or reach the portfolio.
    pub fn try_submit(&mut self, order: Order) -> Result<OrderId, BookError> {
        validate_order(&order)?;
        Ok(self.enqueue(order))
    }

    /// Fire-and-forget submit, mirroring [`OrderBook::submit`]'s safety net:
    /// invalid orders (NaN/Inf price or qty, qty ≤ 0) are logged and dropped
    /// — the returned id will never fill. Callers that need the rejection
    /// should use [`Self::try_submit`].
    pub fn submit(&mut self, order: Order) -> OrderId {
        match self.try_submit(order) {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e,
                    "paper order rejected at submit — invalid price or quantity");
                // Burn an id so the caller still gets a unique handle.
                self.next_id.fetch_add(1, Ordering::Relaxed)
            }
        }
    }

    fn enqueue(&mut self, mut order: Order) -> OrderId {
        order.id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let id = order.id;
        self.pending.push(order);
        id
    }

    /// Process a closed candle — simulate fills for pending orders on that symbol.
    pub fn on_candle(&mut self, symbol: &str, candle: &Candle) -> Vec<TradeEvent> {
        let mut events = Vec::new();

        for order in &mut self.pending {
            if order.symbol != symbol || order.is_filled() { continue; }

            match order.kind {
                OrderType::Market => {
                    let fill_price = candle.open;
                    emit_fill(order, fill_price, order.remaining, candle.ts, &mut events);
                }
                OrderType::Limit => {
                    if let Some(fill_price) = limit_fill_price(order, candle) {
                        emit_fill(order, fill_price, order.remaining, candle.ts, &mut events);
                    }
                }
                OrderType::Ioc | OrderType::Fok => {
                    if let Some(fill_price) = limit_fill_price(order, candle) {
                        emit_fill(order, fill_price, order.remaining, candle.ts, &mut events);
                    } else {
                        events.push(TradeEvent::Cancel {
                            order_id: order.id,
                            reason: if order.kind == OrderType::Fok {
                                CancelReason::FokNoFill
                            } else {
                                CancelReason::IocRemainder
                            },
                        });
                        order.remaining = 0.0; // mark as done
                    }
                }
            }
        }

        // apply fills to portfolio
        for event in &events {
            if let TradeEvent::Fill(fill) = event {
                self.portfolio.apply(fill);
            }
        }

        // remove fully processed orders (`is_filled` covers cancelled orders
        // too — their remaining is zeroed above)
        self.pending.retain(|o| !o.is_filled());
        events
    }

    pub fn pending_count(&self) -> usize { self.pending.len() }
}

fn limit_fill_price(order: &Order, candle: &Candle) -> Option<f64> {
    let price = order.price?;
    match order.side {
        Side::Buy  if candle.low  <= price => Some(price.min(candle.open)),
        Side::Sell if candle.high >= price => Some(price.max(candle.open)),
        _ => None,
    }
}

fn emit_fill(order: &mut Order, price: f64, qty: f64, ts: i64, events: &mut Vec<TradeEvent>) {
    order.remaining -= qty;
    events.push(TradeEvent::Fill(Fill {
        order_id: order.id,
        symbol:   order.symbol.clone(),
        side:     order.side,
        price,
        qty,
        ts,
    }));
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matching::order::Order;

    fn candle(open: f64, high: f64, low: f64, close: f64) -> Candle {
        Candle { ts: 1000, open, high, low, close, volume: 100.0 }
    }

    #[test]
    fn market_buy_fills_at_open() {
        let mut engine = PaperEngine::new(100_000.0);
        engine.submit(Order::market(0, "BTC", Side::Buy, 1.0, 0));
        let events = engine.on_candle("BTC", &candle(50_000.0, 51_000.0, 49_000.0, 50_500.0));
        assert_eq!(events.len(), 1);
        if let TradeEvent::Fill(f) = &events[0] {
            assert_eq!(f.price, 50_000.0);
        }
    }

    #[test]
    fn limit_buy_fills_when_low_touches_price() {
        let mut engine = PaperEngine::new(100_000.0);
        engine.submit(Order::limit(0, "BTC", Side::Buy, 49_500.0, 1.0, 0));
        let events = engine.on_candle("BTC", &candle(50_000.0, 51_000.0, 49_000.0, 50_500.0));
        assert_eq!(events.len(), 1);
        if let TradeEvent::Fill(f) = &events[0] {
            // open > price, so fill at price (49_500)
            assert_eq!(f.price, 49_500.0);
        }
    }

    #[test]
    fn limit_buy_no_fill_when_low_above_price() {
        let mut engine = PaperEngine::new(100_000.0);
        engine.submit(Order::limit(0, "BTC", Side::Buy, 48_000.0, 1.0, 0));
        let events = engine.on_candle("BTC", &candle(50_000.0, 51_000.0, 49_000.0, 50_500.0));
        assert!(events.is_empty());
        assert_eq!(engine.pending_count(), 1);
    }

    #[test]
    fn ioc_cancels_if_not_filled() {
        let mut engine = PaperEngine::new(100_000.0);
        engine.submit(Order::ioc(0, "BTC", Side::Buy, 48_000.0, 1.0, 0));
        let events = engine.on_candle("BTC", &candle(50_000.0, 51_000.0, 49_000.0, 50_500.0));
        assert_eq!(events.len(), 1);
        assert!(!events[0].is_fill());
        assert_eq!(engine.pending_count(), 0); // cancelled, not pending
    }

    #[test]
    fn portfolio_tracks_pnl() {
        let mut engine = PaperEngine::new(100_000.0);
        // buy on first candle (open=50_000), sell on second candle (open=51_000)
        engine.submit(Order::market(0, "BTC", Side::Buy, 1.0, 0));
        engine.on_candle("BTC", &candle(50_000.0, 51_000.0, 49_000.0, 50_500.0));
        assert_eq!(engine.portfolio.position("BTC"), 1.0);

        engine.submit(Order::market(0, "BTC", Side::Sell, 1.0, 0));
        engine.on_candle("BTC", &candle(51_000.0, 52_000.0, 50_500.0, 51_500.0));
        // bought at 50_000, sold at 51_000 → realized P&L = 1_000
        assert_eq!(engine.portfolio.realized_pnl, 1_000.0);
    }

    // ── submit validation (poison orders must never reach the portfolio) ───

    /// Runs a candle past the engine and asserts the poison order left no
    /// trace: nothing pending, no events, portfolio untouched.
    fn assert_no_trace(mut engine: PaperEngine) {
        assert_eq!(engine.pending_count(), 0, "invalid order must not rest as pending");
        let events = engine.on_candle("BTC", &candle(50_000.0, 51_000.0, 49_000.0, 50_500.0));
        assert!(events.is_empty(), "invalid order must produce no events");
        assert_eq!(engine.portfolio.cash, 100_000.0, "cash must be unchanged");
        assert!(engine.portfolio.trades.is_empty(), "no trades may be recorded");
    }

    #[test]
    fn submit_rejects_nan_qty() {
        let mut engine = PaperEngine::new(100_000.0);
        engine.submit(Order::market(0, "BTC", Side::Buy, f64::NAN, 0));
        assert_no_trace(engine);
    }

    #[test]
    fn submit_rejects_infinite_limit_price() {
        let mut engine = PaperEngine::new(100_000.0);
        engine.submit(Order::limit(0, "BTC", Side::Buy, f64::INFINITY, 1.0, 0));
        assert_no_trace(engine);
    }

    #[test]
    fn submit_rejects_non_positive_qty() {
        let mut engine = PaperEngine::new(100_000.0);
        engine.submit(Order::market(0, "BTC", Side::Buy, 0.0, 0));
        engine.submit(Order::market(0, "BTC", Side::Sell, -1.0, 0));
        assert_no_trace(engine);
    }

    #[test]
    fn try_submit_returns_precise_book_error() {
        let mut engine = PaperEngine::new(100_000.0);
        assert!(matches!(
            engine.try_submit(Order::market(0, "BTC", Side::Buy, f64::NAN, 0)),
            Err(BookError::QuantityInvalid(_))
        ));
        assert!(matches!(
            engine.try_submit(Order::limit(0, "BTC", Side::Buy, f64::INFINITY, 1.0, 0)),
            Err(BookError::PriceNotFinite)
        ));
        assert_eq!(engine.pending_count(), 0);
    }
}
