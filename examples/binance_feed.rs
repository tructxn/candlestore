use std::sync::Arc;
use std::time::Duration;

use candlestore::{BinanceFeed, CandleStore, Subscription};

#[tokio::main]
async fn main() {
    let store = Arc::new(
        CandleStore::from_hardware(20)
            .with_data_dir("/tmp/candlestore"),
    );

    let subs = vec![
        Subscription::new("BTCUSDT", "1m"),
        Subscription::new("ETHUSDT", "1m"),
        Subscription::new("SOLUSDT", "1m"),
    ];

    // Spawn a stats printer every 30s
    let store_stats = Arc::clone(&store);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // skip first immediate tick
        loop {
            interval.tick().await;
            println!(
                "[stats] hot symbols in store: {}",
                store_stats.symbol_count()
            );
            for key in ["BTCUSDT:1m", "ETHUSDT:1m", "SOLUSDT:1m"] {
                let recent = store_stats.range(key, 0, i64::MAX);
                if let Some(last) = recent.last() {
                    println!(
                        "  {key:15} candles={:>5}  last_close={:.2}  ts={}",
                        recent.len(), last.close, last.ts
                    );
                }
            }
        }
    });

    println!("Connecting to Binance WebSocket...");
    println!("Ctrl+C to stop.\n");

    let feed = BinanceFeed::new(Arc::clone(&store));
    if let Err(e) = feed.run(subs).await {
        eprintln!("Fatal feed error: {e}");
        std::process::exit(1);
    }
}
