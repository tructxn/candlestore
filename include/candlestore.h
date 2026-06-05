#ifndef CANDLESTORE_H
#define CANDLESTORE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* OHLCV candle — matches Rust #[repr(C)] Candle exactly (48 bytes). */
typedef struct {
    int64_t ts;      /* unix timestamp ms */
    double  open;
    double  high;
    double  low;
    double  close;
    double  volume;
} CCandle;

/* Opaque store handle — forward-declared struct so cgo generates a typed pointer. */
typedef struct CandleStore CandleStore;

/* Lifecycle */
CandleStore* candlestore_new(int max_symbols);
CandleStore* candlestore_new_hardware(int max_symbols);
void         candlestore_free(CandleStore* store);

/* Write */
int candlestore_append(CandleStore* store, const char* symbol, CCandle candle);

/* Read — fills out[0..return_value] with matching candles.
   Returns the number of candles written, or -1 on error. */
int candlestore_range(const CandleStore* store,
                      const char* symbol,
                      int64_t from_ts, int64_t to_ts,
                      CCandle* out, int max_len);

/* Metadata */
int    candlestore_symbol_count(const CandleStore* store);
uint64_t candlestore_l3_cache_bytes(void);

#ifdef __cplusplus
}
#endif

#endif /* CANDLESTORE_H */
