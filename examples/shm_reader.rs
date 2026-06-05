//! Cross-process SPSC reader demo.
//!
//! Usage:
//!   cargo run --release --example shm_reader
//!
//! Run shm_writer in another terminal first; this will retry for up to 5s.

use candlestore::ShmRingReader;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SHM_NAME: &str = "/candlestore_demo";
const CAPACITY: usize = 65536;
const N: usize = 5_000_000;

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

fn main() {
    // Retry opening the shm segment for up to 5 seconds in case the writer
    // hasn't started yet.
    let deadline = Instant::now() + Duration::from_secs(5);
    let reader = loop {
        match ShmRingReader::open(SHM_NAME, CAPACITY) {
            Ok(r) => break r,
            Err(e) => {
                if Instant::now() >= deadline {
                    eprintln!("Could not open shm segment after 5s: {e}");
                    std::process::exit(1);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };

    println!("reader connected, reading {N} candles...");

    let mut latencies_ns: Vec<i64> = Vec::with_capacity(N);
    let start = Instant::now();

    for _ in 0..N {
        let candle = reader.pop();
        let latency_ns = now_nanos() - candle.ts;
        latencies_ns.push(latency_ns);
    }

    let elapsed = start.elapsed();
    let throughput = N as f64 / elapsed.as_secs_f64();

    // Compute statistics
    latencies_ns.sort_unstable();
    let min_ns = latencies_ns[0];
    let max_ns = latencies_ns[N - 1];
    let sum_ns: i64 = latencies_ns.iter().sum();
    let avg_ns = sum_ns / N as i64;
    let p99_idx = (N as f64 * 0.99) as usize;
    let p99_ns = latencies_ns[p99_idx.min(N - 1)];

    println!(
        "Done: read {N} candles in {:.3}s  →  {:.0} candles/sec",
        elapsed.as_secs_f64(),
        throughput
    );
    println!(
        "Latency (ns): min={min_ns}  avg={avg_ns}  p99={p99_ns}  max={max_ns}"
    );
}
