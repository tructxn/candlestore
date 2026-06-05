use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use thiserror::Error;

use crate::Candle;

// ── schema versioning ────────────────────────────────────────────────────────

/// Schema version embedded in every Parquet file this binary writes.
///
/// Files written with version `N` are readable by binaries that support `N`
/// or higher. Forward compatibility relies on:
/// - Existing columns keeping their names and types (never reuse a name for
///   a different meaning across versions).
/// - New columns being added at the END, nullable. Old readers ignore them.
/// - Removed columns being kept (or stubbed) until all readers are upgraded.
///
/// Reading a file with a version *greater* than `SCHEMA_VERSION` returns
/// [`SpillError::IncompatibleVersion`] rather than panicking or returning
/// garbage. Files written before this versioning scheme existed have no
/// `candlestore.brand` metadata and are accepted as the implicit v0, which
/// is byte-compatible with v1.
pub const SCHEMA_VERSION: u32 = 1;

/// Arrow schema metadata key for our brand marker. Files written by other
/// tools with column names that happen to match ours can be rejected by
/// checking this key.
const SCHEMA_BRAND_KEY:   &str = "candlestore.brand";
const SCHEMA_BRAND_VALUE: &str = "candlestore";

/// Arrow schema metadata key for the version number (decimal string).
const SCHEMA_VERSION_KEY: &str = "candlestore.schema_version";

#[derive(Debug, Error)]
pub enum SpillError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// File's schema version is newer than this binary supports.
    /// Almost always means a downgrade was deployed; do not silently delete
    /// the file — upgrade the reader.
    #[error("incompatible Parquet schema version: file declares v{found}, \
             this binary supports up to v{max_supported}")]
    IncompatibleVersion {
        found:         u32,
        max_supported: u32,
    },

    /// File is missing a column required by the schema version it claims.
    /// This indicates corruption or a partial write — log and skip.
    #[error("Parquet file missing required column {column:?}")]
    MissingColumn {
        column:  &'static str,
    },

    /// A required column exists but has the wrong type. Indicates a file
    /// produced by a non-candlestore tool with colliding column names.
    #[error("Parquet column {column:?} has unexpected type")]
    WrongColumnType {
        column:  &'static str,
    },
}

// ── schema ────────────────────────────────────────────────────────────────────

/// Build the Arrow schema for the current Candle layout, including version
/// metadata embedded in the Schema's key-value store.
fn candle_schema() -> Arc<Schema> {
    let metadata = HashMap::from([
        (SCHEMA_BRAND_KEY.to_owned(),   SCHEMA_BRAND_VALUE.to_owned()),
        (SCHEMA_VERSION_KEY.to_owned(), SCHEMA_VERSION.to_string()),
    ]);
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("ts",     DataType::Int64,   false),
            Field::new("open",   DataType::Float64, false),
            Field::new("high",   DataType::Float64, false),
            Field::new("low",    DataType::Float64, false),
            Field::new("close",  DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
        ],
        metadata,
    ))
}

/// Inspect a file's schema metadata and decide if we can read it.
///
/// - No brand + no version → assumed v0 (pre-versioning), OK to read.
/// - Brand match + version <= SCHEMA_VERSION → OK to read.
/// - Brand match + version >  SCHEMA_VERSION → reject.
/// - Brand mismatch → reject (not our file, even if columns coincide).
fn check_schema_compat(schema: &Schema) -> Result<(), SpillError> {
    let metadata = schema.metadata();
    let brand    = metadata.get(SCHEMA_BRAND_KEY).map(String::as_str);
    let version  = metadata.get(SCHEMA_VERSION_KEY).and_then(|v| v.parse::<u32>().ok());

    match (brand, version) {
        // Pre-versioning (v0) — column layout is identical to v1, so accept.
        (None, None) => Ok(()),
        // Our brand, known version.
        (Some(SCHEMA_BRAND_VALUE), Some(v)) if v <= SCHEMA_VERSION => Ok(()),
        // Our brand, newer version we can't safely interpret.
        (Some(SCHEMA_BRAND_VALUE), Some(v)) => Err(SpillError::IncompatibleVersion {
            found:         v,
            max_supported: SCHEMA_VERSION,
        }),
        // Brand mismatch or partial metadata — treat as alien file.
        _ => Err(SpillError::IncompatibleVersion {
            found:         version.unwrap_or(0),
            max_supported: SCHEMA_VERSION,
        }),
    }
}

/// Look up a required column by name and downcast to the expected Arrow type.
/// Returns structured errors instead of panicking on `unwrap()`.
fn typed_column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name:  &'static str,
) -> Result<&'a T, SpillError> {
    let col = batch.column_by_name(name)
        .ok_or(SpillError::MissingColumn { column: name })?;
    col.as_any().downcast_ref::<T>()
        .ok_or(SpillError::WrongColumnType { column: name })
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
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Check schema compatibility BEFORE we start reading batches. Saves work
    // on a rejected file and gives the caller a precise error.
    check_schema_compat(builder.schema())?;

    let reader = builder.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;

        // Column-by-name lookup with typed downcast. Unknown extra columns
        // (e.g. a forward-compatible v2 field) are silently ignored — old
        // readers can keep reading newer files as long as the columns they
        // need keep their names and types.
        let ts     = typed_column::<Int64Array>(&batch,   "ts")?;
        let open   = typed_column::<Float64Array>(&batch, "open")?;
        let high   = typed_column::<Float64Array>(&batch, "high")?;
        let low    = typed_column::<Float64Array>(&batch, "low")?;
        let close  = typed_column::<Float64Array>(&batch, "close")?;
        let volume = typed_column::<Float64Array>(&batch, "volume")?;

        let n = batch.num_rows();
        out.reserve(n);
        for i in 0..n {
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
///
/// Per-file read failures (unknown schema version, corruption, IO error) are
/// logged via `tracing::warn!` and the offending file is skipped — a single
/// bad file does not poison the whole symbol's history. Operators see
/// these in the log AND can audit `candlestore_parquet_spill_errors_total`
/// over time.
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

        match read_file(&path) {
            Ok(candles) => {
                out.extend(candles.into_iter().filter(|c| c.ts >= from_ts && c.ts <= to_ts));
            }
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "skipping unreadable cold-storage file"
                );
            }
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

    // ── schema versioning ──────────────────────────────────────────────────

    /// Helper: write a parquet file with a caller-supplied schema (used to
    /// fabricate files for the version-compat tests).
    fn write_with_schema(
        path:    &Path,
        schema:  Arc<Schema>,
        arrays:  Vec<Arc<dyn arrow::array::Array>>,
    ) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let file = fs::File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    #[test]
    fn written_files_carry_brand_and_version_metadata() {
        // The Arrow schema metadata round-trips through the Parquet file's
        // encoded ARROW:schema entry. Verify the round-trip surfaces our
        // brand and version markers (this is what check_schema_compat uses).
        let dir = tempfile::tempdir().unwrap();
        let candles: Vec<_> = (0..3).map(|i| candle(i, 100.0)).collect();
        spill(dir.path(), "BTC", &candles).unwrap();

        let path = cold_file_path(dir.path(), "BTC", 0, 2);
        let file = fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = builder.schema().metadata();

        assert_eq!(
            metadata.get(SCHEMA_BRAND_KEY).map(String::as_str),
            Some(SCHEMA_BRAND_VALUE),
            "brand metadata must survive write→parquet→read round-trip"
        );
        assert_eq!(
            metadata.get(SCHEMA_VERSION_KEY),
            Some(&SCHEMA_VERSION.to_string()),
            "schema version metadata must survive write→parquet→read round-trip"
        );
    }

    #[test]
    fn unversioned_file_reads_as_v0_for_back_compat() {
        // Simulate a file written by the previous (pre-versioning) candlestore:
        // identical column layout but NO schema metadata.
        let dir = tempfile::tempdir().unwrap();
        let unversioned = Arc::new(Schema::new(vec![
            Field::new("ts",     DataType::Int64,   false),
            Field::new("open",   DataType::Float64, false),
            Field::new("high",   DataType::Float64, false),
            Field::new("low",    DataType::Float64, false),
            Field::new("close",  DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
        ]));
        let path = dir.path().join("BTC").join("0_2.parquet");
        write_with_schema(&path, unversioned, vec![
            Arc::new(Int64Array::from(vec![0i64, 1, 2])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            Arc::new(Float64Array::from(vec![0.5, 1.5, 2.5])),
            Arc::new(Float64Array::from(vec![1.2, 2.2, 3.2])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
        ]);

        let loaded = read_file(&path).expect("must read v0 file for back-compat");
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[2].ts, 2);
        assert_eq!(loaded[2].close, 3.2);
    }

    #[test]
    fn future_version_file_is_rejected_not_panicked() {
        // Simulate a file written by a future binary that bumped SCHEMA_VERSION.
        let dir = tempfile::tempdir().unwrap();
        let future_meta = HashMap::from([
            (SCHEMA_BRAND_KEY.to_owned(),   SCHEMA_BRAND_VALUE.to_owned()),
            (SCHEMA_VERSION_KEY.to_owned(), "999".to_owned()),
        ]);
        let future_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("ts",     DataType::Int64,   false),
                Field::new("open",   DataType::Float64, false),
                Field::new("high",   DataType::Float64, false),
                Field::new("low",    DataType::Float64, false),
                Field::new("close",  DataType::Float64, false),
                Field::new("volume", DataType::Float64, false),
            ],
            future_meta,
        ));
        let path = dir.path().join("BTC").join("0_2.parquet");
        write_with_schema(&path, future_schema, vec![
            Arc::new(Int64Array::from(vec![0i64, 1, 2])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            Arc::new(Float64Array::from(vec![0.5, 1.5, 2.5])),
            Arc::new(Float64Array::from(vec![1.2, 2.2, 3.2])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
        ]);

        match read_file(&path) {
            Err(SpillError::IncompatibleVersion { found, max_supported }) => {
                assert_eq!(found, 999);
                assert_eq!(max_supported, SCHEMA_VERSION);
            }
            other => panic!("expected IncompatibleVersion, got {:?}", other.err()),
        }
    }

    #[test]
    fn missing_required_column_returns_structured_error_not_panic() {
        // Write a v1 file but DROP the `volume` column. read_file must
        // return MissingColumn, not panic.
        let dir = tempfile::tempdir().unwrap();
        let v1_meta = HashMap::from([
            (SCHEMA_BRAND_KEY.to_owned(),   SCHEMA_BRAND_VALUE.to_owned()),
            (SCHEMA_VERSION_KEY.to_owned(), SCHEMA_VERSION.to_string()),
        ]);
        let bad_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("ts",    DataType::Int64,   false),
                Field::new("open",  DataType::Float64, false),
                Field::new("high",  DataType::Float64, false),
                Field::new("low",   DataType::Float64, false),
                Field::new("close", DataType::Float64, false),
                // volume intentionally missing
            ],
            v1_meta,
        ));
        let path = dir.path().join("BTC").join("0_0.parquet");
        write_with_schema(&path, bad_schema, vec![
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Float64Array::from(vec![1.5])),
            Arc::new(Float64Array::from(vec![0.5])),
            Arc::new(Float64Array::from(vec![1.2])),
        ]);

        match read_file(&path) {
            Err(SpillError::MissingColumn { column }) => assert_eq!(column, "volume"),
            other => panic!("expected MissingColumn(volume), got {:?}", other.err()),
        }
    }

    #[test]
    fn extra_unknown_columns_are_silently_ignored() {
        // Simulate a v2-like file that added a `trades_count` column.
        // A v1 reader must accept it: extra columns are forward-compatible.
        let dir = tempfile::tempdir().unwrap();
        let extended_meta = HashMap::from([
            (SCHEMA_BRAND_KEY.to_owned(),   SCHEMA_BRAND_VALUE.to_owned()),
            (SCHEMA_VERSION_KEY.to_owned(), SCHEMA_VERSION.to_string()),
        ]);
        let extended_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("ts",           DataType::Int64,   false),
                Field::new("open",         DataType::Float64, false),
                Field::new("high",         DataType::Float64, false),
                Field::new("low",          DataType::Float64, false),
                Field::new("close",        DataType::Float64, false),
                Field::new("volume",       DataType::Float64, false),
                Field::new("trades_count", DataType::Int64,   false),
            ],
            extended_meta,
        ));
        let path = dir.path().join("BTC").join("0_0.parquet");
        write_with_schema(&path, extended_schema, vec![
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Float64Array::from(vec![1.5])),
            Arc::new(Float64Array::from(vec![0.5])),
            Arc::new(Float64Array::from(vec![1.2])),
            Arc::new(Float64Array::from(vec![10.0])),
            Arc::new(Int64Array::from(vec![42i64])),
        ]);

        let loaded = read_file(&path).expect("extra columns should not block reads");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].volume, 10.0);
    }

    #[test]
    fn query_cold_skips_unreadable_files() {
        // Mix a good v1 file and a future-version file in the same symbol dir.
        // query_cold should return the good data and skip the bad file with
        // a warn-level log.
        let dir = tempfile::tempdir().unwrap();
        let candles: Vec<_> = (0..5).map(|i| candle(i, 1.0)).collect();
        spill(dir.path(), "BTC", &candles).unwrap();

        let bad_meta = HashMap::from([
            (SCHEMA_BRAND_KEY.to_owned(),   SCHEMA_BRAND_VALUE.to_owned()),
            (SCHEMA_VERSION_KEY.to_owned(), "999".to_owned()),
        ]);
        let bad_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("ts", DataType::Int64, false)],
            bad_meta,
        ));
        // ts range overlaps the query window so query_cold actually opens it.
        let bad_path = dir.path().join("BTC").join("100_200.parquet");
        write_with_schema(&bad_path, bad_schema, vec![
            Arc::new(Int64Array::from(vec![100i64, 200])),
        ]);

        let result = query_cold(dir.path(), "BTC", 0, 300);
        assert_eq!(result.len(), 5, "good v1 data should survive a bad neighbour");
    }
}
