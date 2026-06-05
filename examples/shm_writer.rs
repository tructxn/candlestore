//! Cross-process SPSC writer demo.
//!
//! Usage:
//!   cargo run --release --example shm_writer
//!
//! Start shm_reader in a second terminal while this is running.

use candlestore::{Candle, ShmRingWriter};
use std::io::BufRead;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    let writer = ShmRingWriter::create(SHM_NAME, CAPACITY)
        .expect("Failed to create shared memory segment");

    println!("writer ready — start shm_reader in another terminal");
    println!("Will write {N} candles...");

    let start = Instant::now();

    for _ in 0..N {
        let candle = Candle {
            ts:     now_nanos(),
            open:   100.0,
            high:   101.0,
            low:    99.0,
            close:  100.5,
            volume: 1.0,
        };
        writer.push(candle);
    }

    let elapsed = start.elapsed();
    let throughput = N as f64 / elapsed.as_secs_f64();
    println!(
        "Done: wrote {N} candles in {:.3}s  →  {:.0} candles/sec",
        elapsed.as_secs_f64(),
        throughput
    );

    println!("Press Enter to exit (this will unlink the shm segment)...");
    let stdin = std::io::stdin();
    let _ = stdin.lock().lines().next();

    drop(writer); // explicit: munmap + shm_unlink
    println!("shm segment unlinked.");
}
