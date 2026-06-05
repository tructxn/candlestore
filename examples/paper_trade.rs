/// SMA crossover paper trading strategy.
///
/// Signal:  fast_sma(10) crosses above slow_sma(20) → Market Buy
///          fast_sma(10) crosses below slow_sma(20) → Market Sell
///
/// Data:    synthetic BTC candles simulating a trend + mean-reversion cycle.
/// Engine:  PaperEngine fills orders against candle OHLCV.
use std::collections::HashMap;
use std::sync::Arc;

use candlestore::matching::{Order, PaperEngine, Side, TradeEvent};
use candlestore::{Candle, CandleStore};

const SYMBOL: &str = "BTCUSDT:1m";
const FAST:   usize = 10;
const SLOW:   usize = 20;

fn main() {
    // ── 1. Build synthetic BTC candle history ────────────────────────────────
    let store  = Arc::new(CandleStore::new(10));
    let candles = synthetic_btc_candles(200);
    for c in &candles { store.append(SYMBOL, *c); }

    println!("Loaded {} candles into store", candles.len());
    println!("Price range: {:.0} – {:.0}\n",
        candles.iter().map(|c| c.low).fold(f64::MAX, f64::min),
        candles.iter().map(|c| c.high).fold(f64::MIN, f64::max),
    );

    // ── 2. Run strategy ──────────────────────────────────────────────────────
    let mut engine      = PaperEngine::new(100_000.0); // $100k cash
    let mut last_signal = Signal::None;
    let mut trade_count = 0;

    for i in SLOW..candles.len() {
        let candle = &candles[i];

        // compute SMAs from history in the store
        let history = store.range(SYMBOL, candles[i - SLOW].ts, candle.ts);
        if history.len() < SLOW { continue; }

        let fast_sma = sma(&history[history.len() - FAST..]);
        let slow_sma = sma(&history);

        let signal = if fast_sma > slow_sma { Signal::Long } else { Signal::Short };

        if signal != last_signal {
            match signal {
                Signal::Long if engine.portfolio.position(SYMBOL) == 0.0 => {
                    let id = engine.submit(Order::market(0, SYMBOL, Side::Buy, 0.01, candle.ts));
                    let events = engine.on_candle(SYMBOL, candle);
                    print_events(&events, "BUY ");
                    trade_count += 1;
                    let _ = id;
                }
                Signal::Short if engine.portfolio.position(SYMBOL) > 0.0 => {
                    let pos = engine.portfolio.position(SYMBOL);
                    engine.submit(Order::market(0, SYMBOL, Side::Sell, pos, candle.ts));
                    let events = engine.on_candle(SYMBOL, candle);
                    print_events(&events, "SELL");
                    trade_count += 1;
                }
                _ => { engine.on_candle(SYMBOL, candle); }
            }
            last_signal = signal;
        } else {
            engine.on_candle(SYMBOL, candle);
        }
    }

    // ── 3. Close any open position at last candle ────────────────────────────
    let pos = engine.portfolio.position(SYMBOL);
    if pos > 0.0 {
        let last = candles.last().unwrap();
        engine.submit(Order::market(0, SYMBOL, Side::Sell, pos, last.ts));
        engine.on_candle(SYMBOL, last);
        trade_count += 1;
    }

    // ── 4. Print results ─────────────────────────────────────────────────────
    let last_price = candles.last().unwrap().close;
    let prices     = HashMap::from([(SYMBOL.to_string(), last_price)]);

    println!("\n─── Strategy Results ────────────────────────────────");
    println!("Trades:        {}", trade_count);
    println!("Realized P&L:  ${:.2}", engine.portfolio.realized_pnl);
    println!("Total P&L:     ${:.2}", engine.portfolio.total_pnl(&prices));
    println!("Cash:          ${:.2}", engine.portfolio.cash);
    println!("Position:      {:.4} BTC", engine.portfolio.position(SYMBOL));
}

// ── helpers ───────────────────────────────────────────────────────────────────

#[derive(PartialEq, Clone, Copy)]
enum Signal { None, Long, Short }

fn sma(candles: &[Candle]) -> f64 {
    candles.iter().map(|c| c.close).sum::<f64>() / candles.len() as f64
}

fn print_events(events: &[TradeEvent], label: &str) {
    for event in events {
        if let TradeEvent::Fill(f) = event {
            println!("[{}] ts={} qty={:.4} @ ${:.2}", label, f.ts, f.qty, f.price);
        }
    }
}

/// Generate synthetic BTC-like candles with a trend + noise cycle.
fn synthetic_btc_candles(n: usize) -> Vec<Candle> {
    let mut candles = Vec::with_capacity(n);
    let mut price   = 50_000.0f64;
    let mut ts      = 1_700_000_000_000i64;

    for i in 0..n {
        // Simple sine-wave trend + small noise
        let trend  = (i as f64 * 0.08).sin() * 500.0;
        let noise  = ((i as f64 * 1.7).sin() * 100.0) + ((i as f64 * 3.1).cos() * 50.0);
        price     += trend * 0.1 + noise * 0.05;
        price      = price.max(30_000.0);

        let open   = price;
        let high   = price + price * 0.002;
        let low    = price - price * 0.002;
        let close  = price + ((i as f64 * 2.3).sin() * price * 0.001);
        let volume = 10.0 + (i as f64 * 0.5).sin().abs() * 5.0;

        candles.push(Candle { ts, open, high, low, close, volume });
        ts += 60_000; // 1 minute apart
    }
    candles
}
