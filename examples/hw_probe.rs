fn main() {
    let hw = candlestore::HardwareProfile::detect();
    println!("L3 cache:        {} MB ({} bytes)", hw.l3_cache_bytes / 1024 / 1024, hw.l3_cache_bytes);
    println!("Cache line:      {} bytes", hw.cache_line_bytes);
    println!("Physical cores:  {}", hw.physical_cores);
    println!("Ring cap @ 10 symbols: {} candles", hw.ring_capacity_for(10));
    println!("Ring cap @ 50 symbols: {} candles", hw.ring_capacity_for(50));
    println!("Candles per cache line: {}", hw.candles_per_cache_line());
}
