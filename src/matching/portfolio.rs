use std::collections::HashMap;
use super::{event::Fill, order::{qty_is_zero, Side, QTY_REL_TOL}};

#[derive(Debug, Clone)]
pub struct Trade {
    pub order_id:   u64,
    pub symbol:     String,
    pub side:       Side,
    pub price:      f64,
    pub qty:        f64,
    pub ts:         i64,
}

/// Signed-position average-cost portfolio.
///
/// Invariants:
///   * `pos > 0` → long,  `cost` = average entry price of the long.
///   * `pos < 0` → short, `cost` = average entry price of the short.
///   * `pos == 0` → flat, `cost == 0`.
///
/// Realized PnL is booked only when an existing position is *reduced*
/// (long reduced by a sell, short covered by a buy) — never when opening
/// or adding to one.
pub struct Portfolio {
    pub cash:         f64,
    positions:        HashMap<String, f64>,  // symbol → net qty (+ = long, − = short)
    avg_cost:         HashMap<String, f64>,  // symbol → avg entry price (0 when flat)
    pub trades:       Vec<Trade>,
    pub realized_pnl: f64,
}

impl Portfolio {
    pub fn new(initial_cash: f64) -> Self {
        Self {
            cash:         initial_cash,
            positions:    HashMap::new(),
            avg_cost:     HashMap::new(),
            trades:       Vec::new(),
            realized_pnl: 0.0,
        }
    }

    pub fn apply(&mut self, fill: &Fill) {
        self.trades.push(Trade {
            order_id: fill.order_id,
            symbol:   fill.symbol.clone(),
            side:     fill.side,
            price:    fill.price,
            qty:      fill.qty,
            ts:       fill.ts,
        });

        let pos  = self.positions.entry(fill.symbol.clone()).or_insert(0.0);
        let cost = self.avg_cost.entry(fill.symbol.clone()).or_insert(0.0);
        let (q, p) = (fill.qty, fill.price);

        // The old code booked `(price - cost) * qty` on every sell — while
        // flat (cost = 0) that fabricated the whole notional as profit — and
        // clamped negative positions to zero, destroying the short leg while
        // keeping the cash. Buys and sells are now exact mirrors over a
        // signed position.
        match fill.side {
            Side::Buy => {
                if *pos >= 0.0 {
                    // Opening / adding to a long — blend the average cost.
                    *cost = (*pos * *cost + q * p) / (*pos + q);
                    *pos += q;
                } else {
                    // Covering a short: realize (entry − exit) on the covered part.
                    let covered = q.min(-*pos);
                    self.realized_pnl += (*cost - p) * covered;
                    *pos += covered;
                    let leftover = q - covered;
                    if !qty_is_zero(leftover, q) {
                        // Oversized buy flips short → long at the fill price.
                        *pos  = leftover;
                        *cost = p;
                    }
                }
                self.cash -= q * p;
            }
            Side::Sell => {
                if *pos <= 0.0 {
                    // Opening / adding to a short — blend the average cost.
                    *cost = (-*pos * *cost + q * p) / (-*pos + q);
                    *pos -= q;
                } else {
                    // Reducing a long: realize (exit − entry) on the closed part.
                    let closed = q.min(*pos);
                    self.realized_pnl += (p - *cost) * closed;
                    *pos -= closed;
                    let leftover = q - closed;
                    if !qty_is_zero(leftover, q) {
                        // Oversell flips long → short at the fill price.
                        *pos  = -leftover;
                        *cost = p;
                    }
                }
                self.cash += q * p;
            }
        }

        // Flat within tolerance (relative to the fill size, not absolute
        // epsilon — see `order::QTY_REL_TOL`) → snap to exactly flat so a
        // stale cost never leaks into the next round trip.
        if pos.abs() <= q * QTY_REL_TOL {
            *pos  = 0.0;
            *cost = 0.0;
        }
    }

    pub fn position(&self, symbol: &str) -> f64 {
        *self.positions.get(symbol).unwrap_or(&0.0)
    }

    pub fn avg_cost(&self, symbol: &str) -> f64 {
        *self.avg_cost.get(symbol).unwrap_or(&0.0)
    }

    /// Mark-to-market PnL of the open position. Correct for both directions
    /// because `qty` is signed: long → `(mark − cost) * qty`; short
    /// (`qty < 0`) → the same expression equals `(cost − mark) * |qty|`.
    pub fn unrealized_pnl(&self, symbol: &str, current_price: f64) -> f64 {
        let qty  = self.position(symbol);
        let cost = self.avg_cost(symbol);
        (current_price - cost) * qty
    }

    pub fn total_pnl(&self, prices: &HashMap<String, f64>) -> f64 {
        let unrealized: f64 = prices.iter()
            .map(|(sym, &price)| self.unrealized_pnl(sym, price))
            .sum();
        self.realized_pnl + unrealized
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const CASH: f64 = 10_000.0;

    fn fill(side: Side, price: f64, qty: f64) -> Fill {
        Fill { order_id: 1, symbol: "BTC".into(), side, price, qty, ts: 0 }
    }

    fn portfolio_after(fills: &[Fill]) -> Portfolio {
        let mut pf = Portfolio::new(CASH);
        for f in fills { pf.apply(f); }
        pf
    }

    #[test]
    fn sell_while_flat_opens_short_without_fabricated_pnl() {
        // The old code booked the whole notional (2 × 100) as profit here.
        let pf = portfolio_after(&[fill(Side::Sell, 100.0, 2.0)]);
        assert_eq!(pf.position("BTC"), -2.0);
        assert_eq!(pf.avg_cost("BTC"), 100.0);
        assert_eq!(pf.realized_pnl, 0.0);
        assert_eq!(pf.cash, CASH + 200.0);
    }

    #[test]
    fn adding_to_short_blends_average_cost() {
        let pf = portfolio_after(&[
            fill(Side::Sell, 100.0, 1.0),
            fill(Side::Sell, 110.0, 1.0),
        ]);
        assert_eq!(pf.position("BTC"), -2.0);
        assert_eq!(pf.avg_cost("BTC"), 105.0);
        assert_eq!(pf.realized_pnl, 0.0);
    }

    #[test]
    fn buy_to_cover_realizes_entry_minus_exit() {
        let pf = portfolio_after(&[
            fill(Side::Sell, 100.0, 2.0), // short 2 @ 100
            fill(Side::Buy,   90.0, 2.0), // cover  2 @ 90
        ]);
        assert_eq!(pf.realized_pnl, 20.0);   // (100 − 90) × 2
        assert_eq!(pf.position("BTC"), 0.0);
        assert_eq!(pf.avg_cost("BTC"), 0.0); // flat resets cost
        assert_eq!(pf.cash, CASH + 20.0);
    }

    #[test]
    fn oversell_flips_long_to_short_realizing_only_covered_part() {
        // The old code clamped the position to 0 — the short leg vanished
        // while the cash from all 3 units was kept.
        let pf = portfolio_after(&[
            fill(Side::Buy,  100.0, 1.0), // long 1 @ 100
            fill(Side::Sell, 110.0, 3.0), // close 1, open short 2 @ 110
        ]);
        assert_eq!(pf.realized_pnl, 10.0);   // (110 − 100) × 1 only
        assert_eq!(pf.position("BTC"), -2.0);
        assert_eq!(pf.avg_cost("BTC"), 110.0);
        assert_eq!(pf.cash, CASH - 100.0 + 330.0);
    }

    #[test]
    fn overbuy_flips_short_to_long_realizing_only_covered_part() {
        let pf = portfolio_after(&[
            fill(Side::Sell, 100.0, 1.0), // short 1 @ 100
            fill(Side::Buy,   90.0, 3.0), // cover 1, open long 2 @ 90
        ]);
        assert_eq!(pf.realized_pnl, 10.0);   // (100 − 90) × 1 only
        assert_eq!(pf.position("BTC"), 2.0);
        assert_eq!(pf.avg_cost("BTC"), 90.0);
        assert_eq!(pf.cash, CASH + 100.0 - 270.0);
    }

    #[test]
    fn round_trip_long_realizes_pnl_and_cash() {
        let pf = portfolio_after(&[
            fill(Side::Buy,  100.0, 1.0),
            fill(Side::Sell, 110.0, 1.0),
        ]);
        assert_eq!(pf.realized_pnl, 10.0);
        assert_eq!(pf.position("BTC"), 0.0);
        assert_eq!(pf.avg_cost("BTC"), 0.0);
        assert_eq!(pf.cash, CASH + 10.0);
    }

    #[test]
    fn round_trip_short_realizes_pnl_and_cash() {
        let pf = portfolio_after(&[
            fill(Side::Sell, 110.0, 1.0),
            fill(Side::Buy,  100.0, 1.0),
        ]);
        assert_eq!(pf.realized_pnl, 10.0);
        assert_eq!(pf.position("BTC"), 0.0);
        assert_eq!(pf.avg_cost("BTC"), 0.0);
        assert_eq!(pf.cash, CASH + 10.0);
    }

    #[test]
    fn unrealized_and_equity_correct_while_short() {
        let pf = portfolio_after(&[fill(Side::Sell, 100.0, 1.0)]);
        // Short 1 @ 100, mark 90 → unrealized = (cost − mark) × |pos| = +10.
        assert_eq!(pf.unrealized_pnl("BTC", 90.0), 10.0);
        assert_eq!(pf.unrealized_pnl("BTC", 110.0), -10.0);

        let prices = HashMap::from([("BTC".to_string(), 90.0)]);
        assert_eq!(pf.total_pnl(&prices), 10.0);
        // Equity (cash + signed position × mark) = initial + total PnL.
        let equity = pf.cash + pf.position("BTC") * 90.0;
        assert_eq!(equity, CASH + pf.total_pnl(&prices));
    }
}
