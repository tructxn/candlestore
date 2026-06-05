use std::mem::size_of;
use crate::Candle;

/// Detected hardware profile used to auto-tune store parameters.
///
/// Default `resource_fraction` is **1/3** — assumes the machine is shared
/// (OS, other processes, browser, etc.) and candlestore should not monopolise
/// the L3 cache or all cores.  Use `dedicated()` or `with_fraction(1.0)` on
/// machines fully reserved for trading workloads.
#[derive(Debug, Clone)]
pub struct HardwareProfile {
    /// L3 (last-level) cache in bytes. Falls back to 8 MB if undetectable.
    pub l3_cache_bytes: usize,
    /// CPU cache line size in bytes (64 on x86_64, 128 on Apple M-series).
    pub cache_line_bytes: usize,
    /// Physical (not logical/hyperthreaded) core count.
    pub physical_cores: usize,
    /// Fraction of hardware resources candlestore may use (0.0–1.0).
    /// Default: 1/3  (shared machine).  Set to 1.0 for dedicated servers.
    pub resource_fraction: f64,
}

impl HardwareProfile {
    /// Detect hardware and assume a **shared** machine (uses 1/3 of L3 + cores).
    pub fn detect() -> Self {
        Self {
            l3_cache_bytes:   detect_l3_cache(),
            cache_line_bytes: detect_cache_line(),
            physical_cores:   num_cpus::get_physical(),
            resource_fraction: 1.0 / 3.0,
        }
    }

    /// Detect hardware and assume a **dedicated** server (uses 100% of resources).
    pub fn dedicated() -> Self {
        Self { resource_fraction: 1.0, ..Self::detect() }
    }

    /// Override the resource fraction (clamped to 0.05–1.0).
    pub fn with_fraction(mut self, fraction: f64) -> Self {
        self.resource_fraction = fraction.clamp(0.05, 1.0);
        self
    }

    /// Usable L3 bytes after applying the resource fraction.
    pub fn usable_l3_bytes(&self) -> usize {
        (self.l3_cache_bytes as f64 * self.resource_fraction) as usize
    }

    /// How many physical cores candlestore may use.
    pub fn usable_cores(&self) -> usize {
        ((self.physical_cores as f64 * self.resource_fraction).ceil() as usize).max(1)
    }

    /// Optimal ring buffer capacity for one symbol so its hot data fits in
    /// the usable portion of L3.  Clamped to [256, 1_000_000].
    pub fn ring_capacity_for(&self, max_symbols: usize) -> usize {
        let bytes_per_symbol = self.usable_l3_bytes() / max_symbols.max(1);
        let candles = bytes_per_symbol / size_of::<Candle>();
        candles.clamp(256, 1_000_000)
    }

    /// How many Candles fit in one cache line on this machine.
    pub fn candles_per_cache_line(&self) -> usize {
        (self.cache_line_bytes / size_of::<Candle>()).max(1)
    }
}

// ── platform-specific detection ──────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn detect_l3_cache() -> usize {
    let l3 = sysctl_u64("hw.l3cachesize");
    if l3 > 0 { return l3 as usize; }
    let l2 = sysctl_u64("hw.l2cachesize");
    if l2 > 0 { return l2 as usize; }
    default_l3()
}

#[cfg(target_os = "linux")]
fn detect_l3_cache() -> usize {
    for idx in 0..8 {
        let level_path = format!("/sys/devices/system/cpu/cpu0/cache/index{}/level", idx);
        let size_path  = format!("/sys/devices/system/cpu/cpu0/cache/index{}/size",  idx);
        if let (Ok(level), Ok(size_str)) = (
            std::fs::read_to_string(&level_path),
            std::fs::read_to_string(&size_path),
        ) {
            if level.trim() == "3" {
                if let Some(bytes) = parse_cache_size(size_str.trim()) {
                    return bytes;
                }
            }
        }
    }
    default_l3()
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_l3_cache() -> usize { default_l3() }

fn default_l3() -> usize { 8 * 1024 * 1024 }

#[cfg(target_arch = "aarch64")]
fn detect_cache_line() -> usize {
    #[cfg(target_os = "macos")]
    {
        let v = sysctl_u64("hw.cachelinesize");
        if v > 0 { return v as usize; }
    }
    128
}

#[cfg(target_arch = "x86_64")]
fn detect_cache_line() -> usize { 64 }

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
fn detect_cache_line() -> usize { 64 }

#[cfg(target_os = "macos")]
fn sysctl_u64(name: &str) -> u64 {
    use std::ffi::CString;
    let name_c = CString::new(name).unwrap();
    let mut val: u64 = 0;
    let mut size = size_of::<u64>();
    let ret = unsafe {
        libc::sysctlbyname(
            name_c.as_ptr(),
            &mut val as *mut u64 as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if ret == 0 { val } else { 0 }
}

#[cfg(target_os = "linux")]
fn parse_cache_size(s: &str) -> Option<usize> {
    if let Some(k) = s.strip_suffix('K') {
        k.trim().parse::<usize>().ok().map(|v| v * 1024)
    } else if let Some(m) = s.strip_suffix('M') {
        m.trim().parse::<usize>().ok().map(|v| v * 1024 * 1024)
    } else {
        s.parse::<usize>().ok()
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_returns_plausible_values() {
        let hw = HardwareProfile::detect();
        assert!(hw.l3_cache_bytes >= 1024 * 1024);
        assert!(hw.l3_cache_bytes <= 512 * 1024 * 1024);
        assert!(hw.cache_line_bytes == 64 || hw.cache_line_bytes == 128);
        assert!(hw.physical_cores >= 1);
        assert_eq!(hw.resource_fraction, 1.0 / 3.0);
    }

    #[test]
    fn dedicated_uses_full_resources() {
        let hw = HardwareProfile::dedicated();
        assert_eq!(hw.resource_fraction, 1.0);
        assert_eq!(hw.usable_l3_bytes(), hw.l3_cache_bytes);
        assert_eq!(hw.usable_cores(), hw.physical_cores);
    }

    #[test]
    fn with_fraction_clamps_correctly() {
        let hw = HardwareProfile::detect().with_fraction(0.0);
        assert_eq!(hw.resource_fraction, 0.05); // clamped to min

        let hw = HardwareProfile::detect().with_fraction(2.0);
        assert_eq!(hw.resource_fraction, 1.0);  // clamped to max
    }

    #[test]
    fn shared_uses_one_third_of_l3() {
        let hw = HardwareProfile::detect();
        let expected = (hw.l3_cache_bytes as f64 / 3.0) as usize;
        assert_eq!(hw.usable_l3_bytes(), expected);
    }

    #[test]
    fn ring_capacity_scales_with_symbols() {
        let hw = HardwareProfile::detect();
        assert!(hw.ring_capacity_for(10) > hw.ring_capacity_for(100));
    }

    #[test]
    fn ring_capacity_is_clamped() {
        let hw = HardwareProfile::detect();
        assert!(hw.ring_capacity_for(1)    <= 1_000_000);
        assert!(hw.ring_capacity_for(9999) >= 256);
    }

    #[test]
    fn dedicated_gives_larger_capacity_than_shared() {
        let shared    = HardwareProfile::detect();
        let dedicated = HardwareProfile::dedicated();
        assert!(dedicated.ring_capacity_for(10) >= shared.ring_capacity_for(10));
    }

    #[test]
    fn candles_per_cache_line_is_nonzero() {
        let hw = HardwareProfile::detect();
        assert!(hw.candles_per_cache_line() >= 1);
        println!(
            "cache_line={}B  candle={}B  fit={}  fraction={:.0}%  usable_l3={}MB",
            hw.cache_line_bytes, size_of::<Candle>(), hw.candles_per_cache_line(),
            hw.resource_fraction * 100.0,
            hw.usable_l3_bytes() / 1024 / 1024,
        );
    }
}
