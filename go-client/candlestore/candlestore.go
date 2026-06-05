// Package candlestore wraps the Rust candlestore library via cgo.
//
// Build requirements:
//   cargo build --release   (from the repo root)
//   go build ./...          (from go-client/)
package candlestore

/*
#cgo CFLAGS:  -I${SRCDIR}/../../include
#cgo LDFLAGS: -L${SRCDIR}/../../target/release -lcandlestore
#cgo darwin  LDFLAGS: -Wl,-rpath,${SRCDIR}/../../target/release
#cgo linux   LDFLAGS: -Wl,-rpath,${SRCDIR}/../../target/release
#include "candlestore.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Candle is an OHLCV bar.
type Candle struct {
	Ts     int64
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

// Store is an opaque handle to a CandleStore instance.
type Store struct {
	ptr *C.CandleStore
}

// New creates a store sized for up to maxSymbols hot symbols.
func New(maxSymbols int) *Store {
	return &Store{ptr: C.candlestore_new(C.int(maxSymbols))}
}

// NewHardware creates a store auto-tuned to the host machine's L3 cache.
func NewHardware(maxSymbols int) *Store {
	return &Store{ptr: C.candlestore_new_hardware(C.int(maxSymbols))}
}

// Close frees the underlying Rust allocation. Must be called exactly once.
func (s *Store) Close() {
	C.candlestore_free(s.ptr)
	s.ptr = nil
}

// Append adds a candle for the given symbol.
func (s *Store) Append(symbol string, c Candle) {
	sym := C.CString(symbol)
	defer C.free(unsafe.Pointer(sym))
	cc := C.CCandle{
		ts:     C.int64_t(c.Ts),
		open:   C.double(c.Open),
		high:   C.double(c.High),
		low:    C.double(c.Low),
		close:  C.double(c.Close),
		volume: C.double(c.Volume),
	}
	C.candlestore_append(s.ptr, sym, cc)
}

// Range returns candles for symbol where fromTs ≤ ts ≤ toTs.
func (s *Store) Range(symbol string, fromTs, toTs int64) []Candle {
	sym := C.CString(symbol)
	defer C.free(unsafe.Pointer(sym))

	const maxLen = 100_000
	buf := make([]C.CCandle, maxLen)
	count := C.candlestore_range(
		s.ptr, sym,
		C.int64_t(fromTs), C.int64_t(toTs),
		(*C.CCandle)(unsafe.Pointer(&buf[0])),
		C.int(maxLen),
	)
	if count <= 0 {
		return nil
	}
	out := make([]Candle, count)
	for i := range out {
		out[i] = Candle{
			Ts:     int64(buf[i].ts),
			Open:   float64(buf[i].open),
			High:   float64(buf[i].high),
			Low:    float64(buf[i].low),
			Close:  float64(buf[i].close),
			Volume: float64(buf[i].volume),
		}
	}
	return out
}

// SymbolCount returns the number of hot symbols currently in RAM.
func (s *Store) SymbolCount() int {
	return int(C.candlestore_symbol_count(s.ptr))
}

// L3CacheBytes returns the detected L3 cache size in bytes.
func L3CacheBytes() uint64 {
	return uint64(C.candlestore_l3_cache_bytes())
}
