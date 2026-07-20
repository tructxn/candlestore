/// C ABI for Go/cgo bindings.
///
/// Memory contract:
///   - `candlestore_new` allocates a `CandleStore` on the heap and returns an opaque pointer.
///   - The caller owns the pointer and must call `candlestore_free` exactly once.
///   - All string arguments must be valid, null-terminated UTF-8.
///   - `candlestore_range` writes into a caller-allocated buffer; returns -1 on error.
///
/// Panic contract: a panic unwinding out of an `extern "C"` fn is a
/// guaranteed process abort on edition 2024, so every entry point runs its
/// body under [`ffi_guard`], which catches the panic, logs it, and returns
/// the function's error value (null for pointers, -1 for counts) instead.
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::null_mut;

use crate::{Candle, CandleStore, HardwareProfile};

// ── panic barrier ─────────────────────────────────────────────────────────────

/// Run `body` behind a panic barrier: on panic, log the payload and return
/// `err` instead of unwinding into the C caller (which would abort).
fn ffi_guard<T>(err: T, body: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(body)).unwrap_or_else(|payload| {
        let msg = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("<non-string panic payload>");
        tracing::error!(panic = msg, "panic caught at FFI boundary");
        err
    })
}

// ── lifecycle ─────────────────────────────────────────────────────────────────

/// Allocate a store sized for `max_symbols` hot symbols.
/// Returns null if `max_symbols <= 0` or on internal error.
#[unsafe(no_mangle)]
pub extern "C" fn candlestore_new(max_symbols: c_int) -> *mut CandleStore {
    if max_symbols <= 0 { return null_mut(); }
    ffi_guard(null_mut(), || {
        Box::into_raw(Box::new(CandleStore::new(max_symbols as usize)))
    })
}

/// Like [`candlestore_new`], but auto-tuned to the host's L3 cache.
/// Returns null if `max_symbols <= 0` or on internal error.
#[unsafe(no_mangle)]
pub extern "C" fn candlestore_new_hardware(max_symbols: c_int) -> *mut CandleStore {
    if max_symbols <= 0 { return null_mut(); }
    ffi_guard(null_mut(), || {
        Box::into_raw(Box::new(CandleStore::from_hardware(max_symbols as usize)))
    })
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
    ffi_guard((), || {
        if !ptr.is_null() {
            unsafe { drop(Box::from_raw(ptr)); }
        }
    });
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
    ffi_guard(-1, || {
        let sym = match unsafe { CStr::from_ptr(symbol) }.to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };
        unsafe { (*ptr).append(sym, candle) };
        0
    })
}

// ── read ──────────────────────────────────────────────────────────────────────

/// Range query — fills `out[0..return_value]` with matching candles.
/// Returns the number written, or -1 on error (including `max_len < 0`,
/// which would otherwise wrap to a huge unsigned length and overrun the
/// caller's buffer).
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
    if ptr.is_null() || symbol.is_null() || out.is_null() || max_len < 0 { return -1; }
    ffi_guard(-1, || {
        let sym = match unsafe { CStr::from_ptr(symbol) }.to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        };
        let candles = unsafe { &*ptr }.range(sym, from_ts, to_ts);
        let count   = candles.len().min(max_len as usize);
        for (i, c) in candles[..count].iter().enumerate() {
            unsafe { *out.add(i) = *c };
        }
        count as c_int
    })
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
    ffi_guard(-1, || unsafe { (*ptr).symbol_count() as c_int })
}

#[unsafe(no_mangle)]
pub extern "C" fn candlestore_l3_cache_bytes() -> u64 {
    ffi_guard(0, || HardwareProfile::detect().l3_cache_bytes as u64)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffi_guard_returns_err_value_on_panic() {
        let v = ffi_guard(-1, || -> c_int { panic!("boom") });
        assert_eq!(v, -1, "panic must be swallowed and mapped to the error value");
    }

    #[test]
    fn ffi_guard_handles_formatted_string_panic_payload() {
        let v = ffi_guard(0u64, || panic!("formatted {}", 42));
        assert_eq!(v, 0);
    }

    #[test]
    fn ffi_guard_passes_success_through() {
        assert_eq!(ffi_guard(-1, || 7), 7);
    }
}
