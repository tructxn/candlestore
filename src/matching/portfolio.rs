use std::collections::HashMap;
use super::{event::Fill, order::Side};

#[derive(Debug, Clone)]
pub struct Trade {
    pub order_id:   u64,
    pub symbol:     String,
    pub side:       Side,
    pub price:      f64,
    pub qty:        f64,
    pub ts:         i64,
}

pub struct Portfolio {
    pub cash:         f64,
    positions:        HashMap<String, f64>,  // symbol → net qty (+ = long)
    avg_cost:         HashMap<String, f64>,  // symbol → avg entry price
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

        match fill.side {
            Side::Buy => {
                let total_cost = *pos * *cost + fill.qty * fill.price;
                *pos += fill.qty;
                *cost = if *pos > f64::EPSILON { total_cost / *pos } else { 0.0 };
                self.cash -= fill.qty * fill.price;
            }
            Side::Sell => {
                let realized = (fill.price - *cost) * fill.qty;
                self.realized_pnl += realized;
                *pos -= fill.qty;
                if *pos <= f64::EPSILON { *pos = 0.0; *cost = 0.0; }
                self.cash += fill.qty * fill.price;
            }
        }
    }

    pub fn position(&self, symbol: &str) -> f64 {
        *self.positions.get(symbol).unwrap_or(&0.0)
    }

    pub fn avg_cost(&self, symbol: &str) -> f64 {
        *self.avg_cost.get(symbol).unwrap_or(&0.0)
    }

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
