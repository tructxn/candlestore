use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use thiserror::Error;

use crate::Candle;

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

// ── schema ────────────────────────────────────────────────────────────────────

fn candle_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts",     DataType::Int64,   false),
        Field::new("open",   DataType::Float64, false),
        Field::new("high",   DataType::Float64, false),
        Field::new("low",    DataType::Float64, false),
        Field::new("close",  DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]))
}

// ── path helpers ──────────────────────────────────────────────────────────────

/// "BTC/USDT:1m" → "BTC_USDT_1m"  (safe for filesystem paths)
pub fn escape_symbol(symbol: &str) -> String {
    symbol.chars().map(|c| if c.is_alphanumeric() || c == '-' { c } else { '_' }).collect()
}

/// `{data_dir}/{escaped_symbol}/{ts_start}_{ts_end}.parquet`
pub fn cold_file_path(data_dir: &Path, symbol: &str, ts_start: i64, ts_end: i64) -> PathBuf {
    data_dir
        .join(escape_symbol(symbol))
        .join(format!("{}_{}.parquet", ts_start, ts_end))
}

/// Parse ts_start / ts_end from a cold file name.
fn parse_ts_range(path: &Path) -> Option<(i64, i64)> {
    let stem = path.file_stem()?.to_str()?;
    let (a, b) = stem.split_once('_')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

// ── write ─────────────────────────────────────────────────────────────────────

pub fn spill(data_dir: &Path, symbol: &str, candles: &[Candle]) -> Result<(), SpillError> {
    if candles.is_empty() { return Ok(()); }

    let ts_start = candles.first().unwrap().ts;
    let ts_end   = candles.last().unwrap().ts;
    let path     = cold_file_path(data_dir, symbol, ts_start, ts_end);

    fs::create_dir_all(path.parent().unwrap())?;

    let schema = candle_schema();

    let ts:     Int64Array   = candles.iter().map(|c| c.ts).collect();
    let open:   Float64Array = candles.iter().map(|c| c.open).collect();
    let high:   Float64Array = candles.iter().map(|c| c.high).collect();
    let low:    Float64Array = candles.iter().map(|c| c.low).collect();
    let close:  Float64Array = candles.iter().map(|c| c.close).collect();
    let volume: Float64Array = candles.iter().map(|c| c.volume).collect();

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(ts), Arc::new(open), Arc::new(high),
        Arc::new(low), Arc::new(close), Arc::new(volume),
    ])?;

    let file = fs::File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

// ── read ──────────────────────────────────────────────────────────────────────

fn read_file(path: &Path) -> Result<Vec<Candle>, SpillError> {
    let file = fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

    let mut out = Vec::new();
    for batch in reader {
        let batch  = batch?;
        let ts     = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let open   = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let high   = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let low    = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let close  = batch.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let volume = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            out.push(Candle {
                ts:     ts.value(i),
                open:   open.value(i),
                high:   high.value(i),
                low:    low.value(i),
                close:  close.value(i),
                volume: volume.value(i),
            });
        }
    }
    Ok(out)
}

/// Read candles from all cold files for a symbol whose ts range overlaps [from_ts, to_ts].
pub fn query_cold(data_dir: &Path, symbol: &str, from_ts: i64, to_ts: i64) -> Vec<Candle> {
    let sym_dir = data_dir.join(escape_symbol(symbol));
    let entries = match fs::read_dir(&sym_dir) {
        Ok(e) => e,
        Err(_) => return vec![],
    };

    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") { continue; }

        // skip files whose ts range doesn't overlap with [from_ts, to_ts]
        if let Some((file_start, file_end)) = parse_ts_range(&path) {
            if file_end < from_ts || file_start > to_ts { continue; }
        }

        if let Ok(candles) = read_file(&path) {
            out.extend(candles.into_iter().filter(|c| c.ts >= from_ts && c.ts <= to_ts));
        }
    }

    out.sort_unstable_by_key(|c| c.ts);
    out
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(ts: i64, close: f64) -> Candle {
        Candle { ts, open: close, high: close + 1.0, low: close - 1.0, close, volume: 100.0 }
    }

    #[test]
    fn round_trip_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let candles: Vec<_> = (0..10).map(|i| candle(i * 60_000, 50_000.0 + i as f64)).collect();

        spill(dir.path(), "BTC/USDT:1m", &candles).unwrap();

        let path = cold_file_path(dir.path(), "BTC/USDT:1m", 0, 9 * 60_000);
        let loaded = read_file(&path).unwrap();

        assert_eq!(loaded.len(), 10);
        assert_eq!(loaded[0].ts,    candles[0].ts);
        assert_eq!(loaded[9].close, candles[9].close);
    }

    #[test]
    fn query_cold_filters_by_time_range() {
        let dir = tempfile::tempdir().unwrap();
        let candles: Vec<_> = (0..20).map(|i| candle(i * 60_000, 100.0)).collect();
        spill(dir.path(), "ETH/USDT:1m", &candles).unwrap();

        let result = query_cold(dir.path(), "ETH/USDT:1m", 5 * 60_000, 9 * 60_000);
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].ts, 5 * 60_000);
        assert_eq!(result[4].ts, 9 * 60_000);
    }

    #[test]
    fn query_cold_skips_non_overlapping_files() {
        let dir = tempfile::tempdir().unwrap();
        // two separate spills — non-overlapping time ranges
        let early: Vec<_> = (0..5).map(|i| candle(i * 60_000, 100.0)).collect();
        let late:  Vec<_> = (100..105).map(|i| candle(i * 60_000, 200.0)).collect();
        spill(dir.path(), "SOL/USDT:1m", &early).unwrap();
        spill(dir.path(), "SOL/USDT:1m", &late).unwrap();

        // query only the late range
        let result = query_cold(dir.path(), "SOL/USDT:1m", 100 * 60_000, 104 * 60_000);
        assert_eq!(result.len(), 5);
        assert!(result.iter().all(|c| c.close == 200.0));
    }

    #[test]
    fn escape_symbol_is_filesystem_safe() {
        assert_eq!(escape_symbol("BTC/USDT:1m"), "BTC_USDT_1m");
        assert_eq!(escape_symbol("ETH-USD"),     "ETH-USD");
    }
}
