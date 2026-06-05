pub mod candle;
pub mod hw;
pub mod ring_buffer;
pub mod store;
pub mod parquet;
pub mod matching;
pub mod ffi;
pub mod shm;
pub mod signal;
pub mod affinity;
#[cfg(feature = "feed")]
pub mod feed;

pub use candle::Candle;
pub use hw::HardwareProfile;
pub use store::{CandleStore, StoreSnapshot, AppendError};
pub use parquet::{SpillError, SCHEMA_VERSION as PARQUET_SCHEMA_VERSION};
pub use shm::{SpscWriter, SpscReader, SpscRing};
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub use shm::{ShmRingWriter, ShmRingReader, ShmIngester, IngesterStats};
pub use signal::{Signal, Side};
pub use affinity::{pin_to_core, available_cores};
#[cfg(feature = "feed")]
pub use feed::{BinanceFeed, Subscription};
