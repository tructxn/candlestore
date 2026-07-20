// CandleStore is intentionally NOT #[repr(C)] — it's used as an opaque
// pointer through the FFI surface (standard C-API idiom). Suppress the
// improper_ctypes warning that fires on each extern signature here; the
// safety contract documented on each `unsafe extern fn` (src/ffi.rs)
// requires only that the pointer be valid, not that the type be laid out
// in any particular way.
#![allow(improper_ctypes)]

//! Integration tests for the C ABI surface (`src/ffi.rs`).
//!
//! These tests exercise the FFI from the OUTSIDE — same way a Go cgo or
//! Python ctypes consumer would. The `unsafe extern "C"` functions have
//! carefully documented safety contracts; without tests, a Go consumer
//! crashing in production is the first signal that one of them was
//! violated. R4 from the deep review.
//!
//! Everything is `unsafe` because we're calling FFI directly. The asserts
//! that follow each call would catch a UB-causing input (segfault) only
//! by process abort — which IS a meaningful failure mode for these tests.

use candlestore::{Candle, CandleStore};
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

// Re-declare the FFI prototypes so this integration test does NOT depend
// on the implementation details of src/ffi.rs. If someone changes the
// signatures, this test fails to compile — that's the contract.
unsafe extern "C" {
    fn candlestore_new(max_symbols: c_int) -> *mut CandleStore;
    fn candlestore_new_hardware(max_symbols: c_int) -> *mut CandleStore;
    fn candlestore_free(ptr: *mut CandleStore);
    fn candlestore_append(
        ptr:    *mut CandleStore,
        symbol: *const c_char,
        candle: Candle,
    ) -> c_int;
    fn candlestore_range(
        ptr:     *const CandleStore,
        symbol:  *const c_char,
        from_ts: i64,
        to_ts:   i64,
        out:     *mut Candle,
        max_len: c_int,
    ) -> c_int;
    fn candlestore_symbol_count(ptr: *const CandleStore) -> c_int;
    fn candlestore_l3_cache_bytes() -> u64;
}

fn good_candle(ts: i64) -> Candle {
    Candle { ts, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1.0 }
}

#[test]
fn happy_path_new_append_range_free() {
    unsafe {
        let store = candlestore_new(10);
        assert!(!store.is_null(), "candlestore_new must not return null");

        let sym = CString::new("BTC/USDT:1m").unwrap();
        for i in 1..=5 {
            let rc = candlestore_append(store, sym.as_ptr(), good_candle(i));
            assert_eq!(rc, 0, "append must succeed on candle {i}");
        }

        let count = candlestore_symbol_count(store);
        assert_eq!(count, 1, "exactly one symbol");

        let mut out: Vec<Candle> = vec![good_candle(0); 16];
        let written = candlestore_range(store, sym.as_ptr(), 0, 9999, out.as_mut_ptr(), out.len() as c_int);
        assert_eq!(written, 5, "range must report 5 candles written");
        for (i, c) in out[..5].iter().enumerate() {
            assert_eq!(c.ts, (i + 1) as i64);
            assert_eq!(c.close, 100.5);
        }

        candlestore_free(store);
    }
}

#[test]
fn from_hardware_constructor_also_works() {
    unsafe {
        let store = candlestore_new_hardware(8);
        assert!(!store.is_null());
        candlestore_free(store);
    }
}

#[test]
fn append_null_store_returns_minus_one_no_segfault() {
    unsafe {
        let sym = CString::new("BTC").unwrap();
        let rc = candlestore_append(ptr::null_mut(), sym.as_ptr(), good_candle(1));
        assert_eq!(rc, -1, "null store ptr must return -1, not panic/segfault");
    }
}

#[test]
fn append_null_symbol_returns_minus_one() {
    unsafe {
        let store = candlestore_new(4);
        let rc = candlestore_append(store, ptr::null(), good_candle(1));
        assert_eq!(rc, -1);
        candlestore_free(store);
    }
}

#[test]
fn range_null_args_return_minus_one() {
    unsafe {
        let store = candlestore_new(4);
        let sym = CString::new("BTC").unwrap();
        let mut out: Vec<Candle> = vec![good_candle(0); 4];

        // null store
        let rc = candlestore_range(
            ptr::null(), sym.as_ptr(), 0, 100, out.as_mut_ptr(), 4
        );
        assert_eq!(rc, -1);

        // null symbol
        let rc = candlestore_range(
            store, ptr::null(), 0, 100, out.as_mut_ptr(), 4
        );
        assert_eq!(rc, -1);

        // null out buffer
        let rc = candlestore_range(
            store, sym.as_ptr(), 0, 100, ptr::null_mut(), 4
        );
        assert_eq!(rc, -1);

        candlestore_free(store);
    }
}

#[test]
fn range_buffer_smaller_than_results_clamps_safely() {
    unsafe {
        let store = candlestore_new(4);
        let sym = CString::new("BTC").unwrap();
        for i in 1..=10 {
            assert_eq!(candlestore_append(store, sym.as_ptr(), good_candle(i)), 0);
        }

        // Buffer holds 3, but 10 candles exist. Must clamp.
        let mut out: Vec<Candle> = vec![good_candle(0); 3];
        let written = candlestore_range(store, sym.as_ptr(), 0, 100, out.as_mut_ptr(), 3);
        assert_eq!(written, 3, "must clamp to max_len without writing past buffer");

        // Sentinel: the 4th slot of a slightly bigger Vec must remain untouched.
        let mut bigger: Vec<Candle> = vec![good_candle(-999); 10];
        let written = candlestore_range(store, sym.as_ptr(), 0, 100, bigger.as_mut_ptr(), 3);
        assert_eq!(written, 3);
        assert_eq!(bigger[3].ts, -999, "slots past max_len must not be written");

        candlestore_free(store);
    }
}

#[test]
fn range_negative_max_len_returns_minus_one_and_writes_nothing() {
    unsafe {
        let store = candlestore_new(4);
        let sym = CString::new("BTC").unwrap();
        for i in 1..=5 {
            assert_eq!(candlestore_append(store, sym.as_ptr(), good_candle(i)), 0);
        }

        // Before the fix, max_len = -1 became usize::MAX and the full
        // result set was written past the caller's buffer. It must be
        // rejected up front, with the sentinel buffer left untouched.
        let mut out: Vec<Candle> = vec![good_candle(-999); 8];
        let rc = candlestore_range(store, sym.as_ptr(), 0, 100, out.as_mut_ptr(), -1);
        assert_eq!(rc, -1, "negative max_len must return -1");
        for c in &out {
            assert_eq!(c.ts, -999, "buffer must not be written when max_len is rejected");
        }

        // max_len = 0 is a valid (if useless) request: 0 written, no error.
        let rc = candlestore_range(store, sym.as_ptr(), 0, 100, out.as_mut_ptr(), 0);
        assert_eq!(rc, 0, "max_len = 0 must write nothing and return 0");
        assert_eq!(out[0].ts, -999);

        candlestore_free(store);
    }
}

#[test]
fn new_with_nonpositive_max_symbols_returns_null_instead_of_aborting() {
    unsafe {
        // -1 as usize used to become usize::MAX inside CandleStore::new,
        // panicking (→ abort across the FFI boundary). Both constructors
        // must now reject non-positive sizes by returning null.
        assert!(candlestore_new(-1).is_null());
        assert!(candlestore_new(0).is_null());
        assert!(candlestore_new_hardware(-1).is_null());
        assert!(candlestore_new_hardware(0).is_null());
    }
}

#[test]
fn symbol_count_null_returns_minus_one() {
    unsafe {
        let rc = candlestore_symbol_count(ptr::null());
        assert_eq!(rc, -1);
    }
}

#[test]
fn l3_cache_bytes_returns_plausible_value() {
    unsafe {
        let n = candlestore_l3_cache_bytes();
        // Any modern CPU has at least 1 MiB of L3 (or its fallback default
        // of 8 MB if detection fails). Reject obviously broken returns.
        assert!(n >= 1 << 20, "L3 should be >= 1 MiB, got {n}");
        assert!(n < 1 << 32, "L3 should be < 4 GiB on any sane CPU, got {n}");
    }
}

#[test]
fn free_null_is_safe() {
    unsafe {
        candlestore_free(ptr::null_mut()); // documented to be a no-op
    }
}

#[test]
fn invalid_candle_via_ffi_returns_zero_but_does_not_panic() {
    // candlestore_append wraps the store's fire-and-forget append(), which
    // does NOT propagate InvalidCandle back through the FFI return code
    // (returning -1 would conflict with bad-argument detection). It logs
    // and counts internally. Just verify it doesn't crash the process.
    unsafe {
        let store = candlestore_new(4);
        let sym = CString::new("BTC").unwrap();
        let mut bad = good_candle(1);
        bad.close = f64::NAN;
        let rc = candlestore_append(store, sym.as_ptr(), bad);
        // append() returns void in Rust; the FFI wraps it to return 0 on
        // success-of-the-FFI-call (i.e. valid pointers / strings). Bad
        // candle is silently dropped + counted internally.
        assert_eq!(rc, 0, "FFI return code is about argument validity, not domain validity");

        // Symbol not actually present because nothing valid was pushed.
        assert_eq!(candlestore_symbol_count(store), 0);

        candlestore_free(store);
    }
}
