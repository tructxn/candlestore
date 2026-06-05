/// C ABI for Go/cgo bindings.
///
/// Memory contract:
///   - `candlestore_new` allocates a `CandleStore` on the heap and returns an opaque pointer.
///   - The caller owns the pointer and must call `candlestore_free` exactly once.
///   - All string arguments must be valid, null-terminated UTF-8.
///   - `candlestore_range` writes into a caller-allocated buffer; returns -1 on error.
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};

use crate::{Candle, CandleStore, HardwareProfile};

// ── lifecycle ─────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn candlestore_new(max_symbols: c_int) -> *mut CandleStore {
    Box::into_raw(Box::new(CandleStore::new(max_symbols as usize)))
}

#[unsafe(no_mangle)]
pub extern "C" fn candlestore_new_hardware(max_symbols: c_int) -> *mut CandleStore {
    Box::into_raw(Box::new(CandleStore::from_hardware(max_symbols as usize)))
}

/// # Safety: `ptr` must be a valid pointer returned by `candlestore_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn candlestore_free(ptr: *mut CandleStore) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)); }
    }
}

// ── write ─────────────────────────────────────────────────────────────────────

/// Returns 0 on success, -1 on invalid arguments.
/// # Safety: `ptr` and `symbol` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn candlestore_append(
    ptr:    *mut CandleStore,
    symbol: *const c_char,
    candle: Candle,
) -> c_int {
    if ptr.is_null() || symbol.is_null() { return -1; }
    unsafe {
        let sym = match CStr::from_ptr(symbol).to_str() { Ok(s) => s, Err(_) => return -1 };
        (*ptr).append(sym, candle);
    }
    0
}

// ── read ──────────────────────────────────────────────────────────────────────

/// Fills `out[0..return_value]` with matching candles.
/// Returns the number written, or -1 on error.
/// # Safety: `ptr`, `symbol`, and `out` must be valid; `out` must hold at least `max_len` elements.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn candlestore_range(
    ptr:     *const CandleStore,
    symbol:  *const c_char,
    from_ts: i64,
    to_ts:   i64,
    out:     *mut Candle,
    max_len: c_int,
) -> c_int {
    if ptr.is_null() || symbol.is_null() || out.is_null() { return -1; }
    unsafe {
        let sym = match CStr::from_ptr(symbol).to_str() { Ok(s) => s, Err(_) => return -1 };
        let candles = (*ptr).range(sym, from_ts, to_ts);
        let count   = candles.len().min(max_len as usize);
        for (i, c) in candles[..count].iter().enumerate() {
            *out.add(i) = *c;
        }
        count as c_int
    }
}

// ── metadata ──────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub unsafe extern "C" fn candlestore_symbol_count(ptr: *const CandleStore) -> c_int {
    if ptr.is_null() { return -1; }
    unsafe { (*ptr).symbol_count() as c_int }
}

#[unsafe(no_mangle)]
pub extern "C" fn candlestore_l3_cache_bytes() -> u64 {
    HardwareProfile::detect().l3_cache_bytes as u64
}
