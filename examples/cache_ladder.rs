/// Memory hierarchy visualiser — pointer-chasing latency across L1 → DRAM.
///
/// Sequential reads are hidden by hardware prefetch and tell you bandwidth.
/// Random access (pointer chasing) cannot be prefetched and reveals *latency*
/// at each cache level — the number that actually matters for a range scan
/// that misses cache on every element.
///
/// Run: cargo run --release --example cache_ladder
///
/// Reference: Ulrich Drepper, "What Every Programmer Should Know About Memory"
/// §3.3 — Measurement, Red Hat 2007.  akkadia.org/drepper/cpumemory.pdf
use std::time::Instant;

/// Build a random pointer-chase chain, one node per cache line.
/// Each node holds the index of the next node; chasing it forces a
/// load-use dependency the CPU cannot pipeline or prefetch away.
fn build_chase_chain(size_bytes: usize, cache_line: usize) -> Vec<usize> {
    let stride = cache_line / std::mem::size_of::<usize>(); // usizes per cache line
    let n = (size_bytes / cache_line).max(2);               // number of nodes
    let mut chain = vec![0usize; n * stride];               // one node = one cache line

    // Build a random cyclic permutation via Fisher-Yates (LCG, no external deps)
    let mut perm: Vec<usize> = (0..n).collect();
    let mut rng = 0x123456789usize;
    for i in (1..n).rev() {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        perm.swap(i, rng % (i + 1));
    }

    // Store next-node index at the start of each cache-line-aligned slot
    for i in 0..n {
        chain[i * stride] = perm[i] * stride;
    }
    chain
}

fn measure_latency_ns(size_bytes: usize, cache_line: usize) -> f64 {
    let chain = build_chase_chain(size_bytes, cache_line);
    let n_nodes = size_bytes / cache_line;
    let n_hops  = (50_000_000 / n_nodes.max(1)).max(5) * n_nodes; // ~50M hops total

    // warm-up pass
    let mut pos = 0usize;
    for _ in 0..n_nodes {
        pos = unsafe { *chain.get_unchecked(pos) };
    }
    std::hint::black_box(pos);

    let start = Instant::now();
    let mut pos = 0usize;
    for _ in 0..n_hops {
        pos = unsafe { *chain.get_unchecked(pos) };
    }
    std::hint::black_box(pos);

    start.elapsed().as_nanos() as f64 / n_hops as f64
}

fn fmt_size(b: usize) -> String {
    if b >= 1024 * 1024 { format!("{} MB", b / 1024 / 1024) }
    else                { format!("{} KB", b / 1024) }
}

fn main() {
    let hw = candlestore::HardwareProfile::detect();
    println!("L3 cache     : {} MB", hw.l3_cache_bytes / 1024 / 1024);
    println!("Cache line   : {} B", hw.cache_line_bytes);
    println!("Physical cores: {}", hw.physical_cores);
    println!("Usable L3 (1/3): {} KB\n", hw.usable_l3_bytes() / 1024);

    println!("{:>10}  {:>10}  {:>10}  {}", "size", "latency", "vs L1", "zone");
    println!("{}", "─".repeat(54));

    let sizes: &[(usize, &str)] = &[
        (4   * 1024,              "L1"),
        (16  * 1024,              "L1"),
        (64  * 1024,              "L1→L2"),
        (256 * 1024,              "L2"),
        (hw.l3_cache_bytes / 4,   "L2→L3"),
        (hw.l3_cache_bytes / 3,   "L3 (candlestore)"),
        (hw.l3_cache_bytes,       "L3 ceiling"),
        (hw.l3_cache_bytes * 2,   "L3 overflow"),
        (hw.l3_cache_bytes * 8,   "DRAM"),
        (hw.l3_cache_bytes * 16,  "DRAM"),
    ];

    let mut l1_ns = 0f64;
    for &(sz, zone) in sizes {
        let ns = measure_latency_ns(sz, hw.cache_line_bytes);
        if l1_ns == 0.0 { l1_ns = ns; }
        let ratio = ns / l1_ns;
        let flag = if sz == hw.l3_cache_bytes / 3 { " ◄" } else { "" };
        println!("{:>9}  {:>8.1} ns  {:>6.1}×   {}{}", fmt_size(sz), ns, ratio, zone, flag);
    }

    println!();
    println!("Each access in the chain depends on the previous result —");
    println!("prefetch cannot hide the latency. The ratio column shows");
    println!("how many times slower each level is than L1.");
    println!();
    println!("candlestore ring_capacity_for(N) targets the ◄ row:");
    println!("  usable L3 ({} KB) / max_symbols = {} candles/symbol",
        hw.usable_l3_bytes() / 1024,
        hw.ring_capacity_for(10));
    println!("  → hot data stays in the fast zone, not DRAM.");
}
