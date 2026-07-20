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

// Candle is an OHLCV bar. Ts is a unix timestamp in nanoseconds.
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

const (
	// defaultRangeCap is the initial buffer capacity used by Range.
	defaultRangeCap = 4096
	// maxRangeCap bounds buffer growth (10M candles ≈ 480 MB).
	maxRangeCap = 10_000_000
)

// Range returns candles for symbol where fromTs ≤ ts ≤ toTs.
// The buffer grows automatically (up to maxRangeCap candles), so results
// are never silently truncated. Use RangeN to tune the initial capacity.
func (s *Store) Range(symbol string, fromTs, toTs int64) []Candle {
	return s.RangeN(symbol, fromTs, toTs, defaultRangeCap)
}

// RangeN is Range with a caller-chosen initial buffer capacity of max
// candles. If the result fills the buffer exactly (possible truncation),
// it retries with a doubled buffer, capped at maxRangeCap candles.
func (s *Store) RangeN(symbol string, fromTs, toTs int64, max int) []Candle {
	if max <= 0 {
		max = defaultRangeCap
	}
	if max > maxRangeCap {
		max = maxRangeCap
	}

	sym := C.CString(symbol)
	defer C.free(unsafe.Pointer(sym))

	for {
		buf := make([]C.CCandle, max)
		count := int(C.candlestore_range(
			s.ptr, sym,
			C.int64_t(fromTs), C.int64_t(toTs),
			(*C.CCandle)(unsafe.Pointer(&buf[0])),
			C.int(max),
		))
		if count <= 0 {
			return nil
		}
		if count == max && max < maxRangeCap {
			// Buffer full — results may be truncated. Retry doubled.
			max *= 2
			if max > maxRangeCap {
				max = maxRangeCap
			}
			continue
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
}

// SymbolCount returns the number of hot symbols currently in RAM.
func (s *Store) SymbolCount() int {
	return int(C.candlestore_symbol_count(s.ptr))
}

// L3CacheBytes returns the detected L3 cache size in bytes.
func L3CacheBytes() uint64 {
	return uint64(C.candlestore_l3_cache_bytes())
}
