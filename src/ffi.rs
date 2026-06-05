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

/// Free a CandleStore previously allocated by `candlestore_new`.
///
/// # Safety
/// `ptr` must be a valid pointer returned by [`candlestore_new`] or
/// [`candlestore_new_hardware`] that has not been freed yet. Passing a
/// null pointer is allowed (no-op). Passing the same pointer twice or
/// any other pointer is undefined behaviour.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn candlestore_free(ptr: *mut CandleStore) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)); }
    }
}

// ── write ─────────────────────────────────────────────────────────────────────

/// Append a candle to the store under `symbol`.
///
/// Returns 0 on success, -1 on invalid arguments.
///
/// # Safety
/// `ptr` must be a valid pointer returned by `candlestore_new` and not
/// yet freed. `symbol` must be a valid, null-terminated UTF-8 C string.
/// Passing a null `ptr` or null `symbol` returns -1 safely; any other
/// invalid pointer is undefined behaviour.
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

/// Range query — fills `out[0..return_value]` with matching candles.
/// Returns the number written, or -1 on error.
///
/// # Safety
/// `ptr` must be a valid pointer returned by `candlestore_new` and not
/// yet freed. `symbol` must be a valid, null-terminated UTF-8 C string.
/// `out` must point to a contiguous buffer of at least `max_len` `Candle`
/// values that is writable for the duration of the call. Any of these
/// being null returns -1 safely; other invalid pointers are UB.
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

/// Number of symbols currently held in RAM.
///
/// # Safety
/// `ptr` must be a valid pointer returned by `candlestore_new` and not
/// yet freed. A null pointer returns -1 safely; other invalid pointers
/// are undefined behaviour.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn candlestore_symbol_count(ptr: *const CandleStore) -> c_int {
    if ptr.is_null() { return -1; }
    unsafe { (*ptr).symbol_count() as c_int }
}

#[unsafe(no_mangle)]
pub extern "C" fn candlestore_l3_cache_bytes() -> u64 {
    HardwareProfile::detect().l3_cache_bytes as u64
}
