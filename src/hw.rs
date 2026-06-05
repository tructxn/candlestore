use std::mem::size_of;
use crate::Candle;

/// Detected hardware profile used to auto-tune store parameters.
#[derive(Debug, Clone)]
pub struct HardwareProfile {
    /// L3 (last-level) cache in bytes. Falls back to 8 MB if undetectable.
    pub l3_cache_bytes: usize,
    /// CPU cache line size in bytes (64 on x86_64, 128 on Apple M-series).
    pub cache_line_bytes: usize,
    /// Physical (not logical/hyperthreaded) core count.
    pub physical_cores: usize,
}

impl HardwareProfile {
    pub fn detect() -> Self {
        Self {
            l3_cache_bytes:   detect_l3_cache(),
            cache_line_bytes: detect_cache_line(),
            physical_cores:   num_cpus::get_physical(),
        }
    }

    /// Optimal ring buffer capacity for one symbol so its data fits in L3.
    ///
    /// Divides L3 evenly across `max_symbols`, capped at 1M candles to avoid
    /// runaway allocation on machines with very large caches.
    pub fn ring_capacity_for(&self, max_symbols: usize) -> usize {
        let bytes_per_symbol = self.l3_cache_bytes / max_symbols.max(1);
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
    // sysctl hw.l3cachesize — returns 0 on M-series if no dedicated L3,
    // fall through to hw.cacheconfig in that case.
    let l3 = sysctl_u64("hw.l3cachesize");
    if l3 > 0 { return l3 as usize; }
    // Apple M-series exposes shared L2/SLC as "hw.l2cachesize"
    let l2 = sysctl_u64("hw.l2cachesize");
    if l2 > 0 { return l2 as usize; }
    default_l3()
}

#[cfg(target_os = "linux")]
fn detect_l3_cache() -> usize {
    // Walk /sys/devices/system/cpu/cpu0/cache/index* looking for level 3.
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

fn default_l3() -> usize { 8 * 1024 * 1024 } // 8 MB safe default

// ── cache line ────────────────────────────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
fn detect_cache_line() -> usize {
    // Apple M-series and most ARM64 use 128-byte cache lines.
    // Verify via sysctl on macOS; fall back to 128.
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

// ── macOS sysctl helper ───────────────────────────────────────────────────────

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

// ── Linux cache size string parser ("8192K", "32M", "512K") ──────────────────

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
        assert!(hw.l3_cache_bytes >= 1024 * 1024,   "L3 < 1MB — unlikely");
        assert!(hw.l3_cache_bytes <= 512 * 1024 * 1024, "L3 > 512MB — unlikely");
        assert!(hw.cache_line_bytes == 64 || hw.cache_line_bytes == 128);
        assert!(hw.physical_cores >= 1);
    }

    #[test]
    fn ring_capacity_scales_with_symbols() {
        let hw = HardwareProfile::detect();
        let cap_10  = hw.ring_capacity_for(10);
        let cap_100 = hw.ring_capacity_for(100);
        assert!(cap_10 > cap_100, "more symbols → smaller per-symbol capacity");
    }

    #[test]
    fn ring_capacity_is_clamped() {
        let hw = HardwareProfile::detect();
        assert!(hw.ring_capacity_for(1)    <= 1_000_000);
        assert!(hw.ring_capacity_for(9999) >= 256);
    }

    #[test]
    fn candles_per_cache_line_is_nonzero() {
        let hw = HardwareProfile::detect();
        assert!(hw.candles_per_cache_line() >= 1);
        println!(
            "cache_line={}B  candle={}B  fit={}",
            hw.cache_line_bytes, size_of::<Candle>(), hw.candles_per_cache_line()
        );
    }
}
