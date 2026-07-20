use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use thiserror::Error;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{Candle, CandleStore};

const WS_BASE: &str = "wss://stream.binance.com:9443/stream";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

// ── public types ──────────────────────────────────────────────────────────────

/// One symbol + interval subscription, e.g. ("BTCUSDT", "1m").
#[derive(Debug, Clone)]
pub struct Subscription {
    pub symbol:   String,
    pub interval: String,
}

impl Subscription {
    pub fn new(symbol: impl Into<String>, interval: impl Into<String>) -> Self {
        Self { symbol: symbol.into(), interval: interval.into() }
    }

    /// Stream name used in the Binance WebSocket URL.
    fn stream_name(&self) -> String {
        format!("{}@kline_{}", self.symbol.to_lowercase(), self.interval)
    }

    /// Key used in `CandleStore` — e.g. "BTCUSDT:1m"
    pub fn store_key(&self) -> String {
        format!("{}:{}", self.symbol.to_uppercase(), self.interval)
    }
}

#[derive(Debug, Error)]
pub enum FeedError {
    #[error("websocket: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("no subscriptions")]
    NoSubscriptions,
}

// ── raw Binance JSON structures ───────────────────────────────────────────────

#[derive(Deserialize)]
struct CombinedMsg {
    data: KlineEvent,
}

#[derive(Deserialize)]
struct KlineEvent {
    k: KlineData,
}

#[derive(Deserialize)]
struct KlineData {
    #[serde(rename = "t")] ts:       i64,
    #[serde(rename = "o")] open:     String,
    #[serde(rename = "h")] high:     String,
    #[serde(rename = "l")] low:      String,
    #[serde(rename = "c")] close:    String,
    #[serde(rename = "v")] volume:   String,
    #[serde(rename = "x")] is_closed: bool,
    #[serde(rename = "s")] symbol:   String,
    #[serde(rename = "i")] interval: String,
}

impl KlineData {
    fn to_candle(&self) -> Option<Candle> {
        Some(Candle {
            ts:     self.ts,
            open:   self.open.parse().ok()?,
            high:   self.high.parse().ok()?,
            low:    self.low.parse().ok()?,
            close:  self.close.parse().ok()?,
            volume: self.volume.parse().ok()?,
        })
    }

    fn store_key(&self) -> String {
        format!("{}:{}", self.symbol.to_uppercase(), self.interval)
    }
}

// ── feed ──────────────────────────────────────────────────────────────────────

pub struct BinanceFeed {
    store: Arc<CandleStore>,
}

impl BinanceFeed {
    pub fn new(store: Arc<CandleStore>) -> Self {
        Self { store }
    }

    /// Run forever — connects, streams closed candles into the store, and
    /// automatically reconnects after both errors and clean server closes
    /// (Binance force-disconnects every stream after ~24 h; a graceful
    /// close must never stop ingestion).
    ///
    /// There is no built-in shutdown signal: callers stop the feed by
    /// dropping/aborting the future. The only early return is
    /// [`FeedError::NoSubscriptions`].
    pub async fn run(&self, subs: Vec<Subscription>) -> Result<(), FeedError> {
        if subs.is_empty() { return Err(FeedError::NoSubscriptions); }

        loop {
            match self.stream_once(&subs).await {
                // Clean close from the server — treat exactly like an
                // error: log and reconnect after the standard delay.
                Ok(()) => eprintln!(
                    "[candlestore] feed closed by server — reconnecting in {}s",
                    RECONNECT_DELAY.as_secs()
                ),
                Err(e) => eprintln!(
                    "[candlestore] feed error: {e} — reconnecting in {}s",
                    RECONNECT_DELAY.as_secs()
                ),
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    async fn stream_once(&self, subs: &[Subscription]) -> Result<(), FeedError> {
        let streams: Vec<String> = subs.iter().map(|s| s.stream_name()).collect();
        let url = format!("{}?streams={}", WS_BASE, streams.join("/"));

        let (ws, _) = connect_async(&url).await?;
        let (_, mut reader) = ws.split();

        println!("[candlestore] connected — watching: {}", streams.join(", "));

        while let Some(msg) = reader.next().await {
            let msg = msg?;
            if let Message::Text(text) = msg
                && let Ok(combined) = serde_json::from_str::<CombinedMsg>(&text)
            {
                let k = &combined.data.k;
                if k.is_closed
                    && let Some(candle) = k.to_candle()
                {
                    self.store.append(&k.store_key(), candle);
                }
            }
        }
        Ok(())
    }
}
