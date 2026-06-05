//! SPSC ring buffers: heap-backed (in-process) and POSIX shared memory (cross-process).
//!
//! # Design
//!
//! Both variants share the same conceptual interface but differ in backing store:
//!
//! - [`SpscRing`] / [`SpscWriter`] / [`SpscReader`]: heap-allocated, for in-process use
//!   and benchmarking. Slots live in a `Box<[UnsafeCell<Candle>]>` and head/tail
//!   cursors are `AtomicU64` each padded to 128 bytes (Apple M-series cache line).
//!
//! - [`ShmRingWriter`] / [`ShmRingReader`]: POSIX `shm_open` / `mmap` backed, for
//!   cross-process streaming. A fixed [`ShmHeader`] (384 bytes, 3 × 128-byte cache
//!   lines) precedes the slot array in the shared mapping.
//!
//! # Memory ordering
//!
//! SPSC requires only two ordering guarantees:
//! - Writer: `Acquire`-load tail (see if space exists), write slot, `Release`-store head
//! - Reader: `Acquire`-load head (see if data exists), read slot, `Release`-store tail
//!
//! This is sufficient because there is exactly one producer and one consumer.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::Candle;

// ── constants ────────────────────────────────────────────────────────────────

/// Magic value written to `ShmHeader::ready` once the writer has initialised
/// the shared-memory segment. The reader spins until it sees this value.
pub const READY_MAGIC: u64 = 0xCAFE_CA1D_1E5748;

/// Apple M-series (and many modern x86) cache line = 128 bytes.
const CACHE_LINE: usize = 128;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Returns the current time in nanoseconds since the UNIX epoch.
#[inline]
pub fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

// ─────────────────────────────────────────────────────────────────────────────
// SpscRing — heap-backed, in-process SPSC
// ─────────────────────────────────────────────────────────────────────────────

/// Aligned wrapper that places an `AtomicU64` cursor on its own 128-byte cache line,
/// preventing false sharing between head and tail on wide-cache-line CPUs (Apple M-series).
#[repr(C, align(128))]
struct CachePadded {
    value: AtomicU64,
    #[allow(dead_code)]
    _pad: [u8; CACHE_LINE - 8], // AtomicU64 is 8 bytes; pad to 128
}

impl CachePadded {
    const fn new(v: u64) -> Self {
        Self {
            value: AtomicU64::new(v),
            _pad: [0u8; CACHE_LINE - 8],
        }
    }
}

/// Shared inner state for the heap-backed SPSC ring.
///
/// # Safety invariants
///
/// - Only the writer (owner of `SpscWriter`) may advance `head`.
/// - Only the reader (owner of `SpscReader`) may advance `tail`.
/// - `slots[i % capacity]` may only be written when the slot is logically empty
///   (i.e., `head - tail < capacity`), and only by the writer.
/// - `slots[i % capacity]` may only be read when the slot is logically full
///   (i.e., `head > tail`), and only by the reader.
struct SpscInner {
    slots:    Box<[UnsafeCell<Candle>]>,
    capacity: usize,
    head:     CachePadded, // producer cursor — written only by writer
    tail:     CachePadded, // consumer cursor — written only by reader
}

/// SAFETY: `SpscWriter` and `SpscReader` each hold a non-overlapping exclusive
/// role (producer / consumer), enforced at construction time. The `UnsafeCell`
/// slots are accessed in a non-overlapping manner by the two roles.
unsafe impl Send for SpscInner {}
unsafe impl Sync for SpscInner {}

use std::sync::Arc;

/// Produces candles into a heap-backed SPSC ring buffer.
pub struct SpscWriter {
    inner: Arc<SpscInner>,
}

/// Consumes candles from a heap-backed SPSC ring buffer.
pub struct SpscReader {
    inner: Arc<SpscInner>,
}

/// Heap-backed SPSC ring buffer factory.
///
/// `capacity` must be a power of two for efficient index masking.
pub struct SpscRing;

impl SpscRing {
    /// Create a linked writer/reader pair backed by a heap-allocated ring of `capacity` slots.
    ///
    /// # Panics
    /// Panics if `capacity` is zero or not a power of two.
    pub fn new(capacity: usize) -> (SpscWriter, SpscReader) {
        assert!(capacity.is_power_of_two(), "SpscRing capacity must be a power of two");
        assert!(capacity > 0, "SpscRing capacity must be > 0");

        let zero = Candle { ts: 0, open: 0.0, high: 0.0, low: 0.0, close: 0.0, volume: 0.0 };
        let slots: Box<[UnsafeCell<Candle>]> = (0..capacity)
            .map(|_| UnsafeCell::new(zero))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let inner = Arc::new(SpscInner {
            slots,
            capacity,
            head: CachePadded::new(0),
            tail: CachePadded::new(0),
        });

        (SpscWriter { inner: Arc::clone(&inner) }, SpscReader { inner })
    }
}

impl SpscWriter {
    /// Push a candle, spinning until space is available.
    #[inline]
    pub fn push(&self, candle: Candle) {
        loop {
            if self.try_push(candle) {
                return;
            }
            std::hint::spin_loop();
        }
    }

    /// Try to push a candle without blocking.
    ///
    /// Returns `true` if the candle was written, `false` if the ring was full.
    #[inline]
    pub fn try_push(&self, candle: Candle) -> bool {
        let head = self.inner.head.value.load(Ordering::Relaxed);
        let tail = self.inner.tail.value.load(Ordering::Acquire);

        if head.wrapping_sub(tail) >= self.inner.capacity as u64 {
            return false; // ring full
        }

        let slot = head as usize & (self.inner.capacity - 1);

        // SAFETY: We verified `head - tail < capacity`, so this slot belongs
        // exclusively to the writer. No reader can access it until we advance head.
        unsafe {
            *self.inner.slots[slot].get() = candle;
        }

        self.inner.head.value.store(head.wrapping_add(1), Ordering::Release);
        true
    }
}

impl SpscReader {
    /// Pop a candle, spinning until one is available.
    #[inline]
    pub fn pop(&self) -> Candle {
        loop {
            if let Some(c) = self.try_pop() {
                return c;
            }
            std::hint::spin_loop();
        }
    }

    /// Try to pop a candle without blocking.
    ///
    /// Returns `None` if the ring is empty.
    #[inline]
    pub fn try_pop(&self) -> Option<Candle> {
        let tail = self.inner.tail.value.load(Ordering::Relaxed);
        let head = self.inner.head.value.load(Ordering::Acquire);

        if head == tail {
            return None; // ring empty
        }

        let slot = tail as usize & (self.inner.capacity - 1);

        // SAFETY: We verified `head > tail`, so this slot was written by the
        // writer and is now exclusively available to the reader. The writer will
        // not touch this slot again until tail advances past capacity slots ahead.
        let candle = unsafe { *self.inner.slots[slot].get() };

        self.inner.tail.value.store(tail.wrapping_add(1), Ordering::Release);
        Some(candle)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ShmRing — POSIX shared-memory SPSC (cross-process)
// ─────────────────────────────────────────────────────────────────────────────

/// Layout of the fixed header at offset 0 of the shared-memory segment.
///
/// Three 128-byte cache lines:
/// - Line 0 (offset   0): `ready` + `capacity` + 112 bytes padding
/// - Line 1 (offset 128): `head` + 120 bytes padding
/// - Line 2 (offset 256): `tail` + 120 bytes padding
///
/// Total header size: 384 bytes.
#[repr(C)]
pub struct ShmHeader {
    // Cache line 0
    pub ready:    AtomicU64,
    pub capacity: u64,
    #[allow(dead_code)]
    _pad0: [u8; 112], // 128 - 8 - 8

    // Cache line 1
    pub head:  AtomicU64,
    #[allow(dead_code)]
    _pad1: [u8; 120], // 128 - 8

    // Cache line 2
    pub tail:  AtomicU64,
    #[allow(dead_code)]
    _pad2: [u8; 120], // 128 - 8
}

const _: () = assert!(std::mem::size_of::<ShmHeader>() == 384);
const _: () = assert!(std::mem::offset_of!(ShmHeader, head) == 128);
const _: () = assert!(std::mem::offset_of!(ShmHeader, tail) == 256);

#[cfg(any(target_os = "macos", target_os = "linux"))]
mod shm_impl {
    use super::*;
    use std::io;

    use libc::{
        MAP_FAILED, MAP_SHARED, PROT_READ, PROT_WRITE, c_char, ftruncate, mmap, munmap,
        shm_open, shm_unlink, O_CREAT, O_RDWR, O_TRUNC,
    };

    /// Byte offset from the start of the mapping where candle slots begin.
    const SLOTS_OFFSET: usize = 384; // = size_of::<ShmHeader>()

    /// Mapping size for a ring of `capacity` candles.
    fn mapping_size(capacity: usize) -> usize {
        SLOTS_OFFSET + capacity * std::mem::size_of::<Candle>()
    }

    // ── ShmRingWriter ────────────────────────────────────────────────────────

    /// Producer side of a cross-process SPSC ring backed by POSIX shared memory.
    ///
    /// Dropping this type calls `munmap` + `shm_unlink`, removing the segment.
    pub struct ShmRingWriter {
        ptr:      *mut u8,
        map_size: usize,
        name:     String,
        capacity: usize,
    }

    /// SAFETY: The pointer `ptr` owns the mapped region exclusively until drop.
    /// No other `ShmRingWriter` for the same name can exist simultaneously because
    /// `shm_open(O_CREAT|O_TRUNC)` truncates the segment, making any previous
    /// reader mapping stale.
    unsafe impl Send for ShmRingWriter {}

    impl ShmRingWriter {
        /// Create a new shared-memory ring with the given POSIX name and capacity.
        ///
        /// `name` must start with `/` per POSIX convention.
        /// `capacity` must be a power of two.
        ///
        /// # Errors
        ///
        /// Returns `io::Error` on `shm_open`, `ftruncate`, or `mmap` failure.
        pub fn create(name: &str, capacity: usize) -> io::Result<Self> {
            assert!(capacity.is_power_of_two(), "ShmRingWriter capacity must be a power of two");

            let cname = std::ffi::CString::new(name)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            // Unlink any stale segment from a previous crash before creating a
            // fresh one. On macOS, `O_TRUNC` on an already-mapped segment returns
            // EINVAL; unlinking first avoids that.
            // SAFETY: `shm_unlink` is safe to call with a valid C string even if
            // the segment doesn't exist (it will just return -1 which we ignore).
            unsafe { shm_unlink(cname.as_ptr() as *const c_char) };

            // SAFETY: `shm_open` is a standard POSIX syscall. `cname` is valid.
            let fd = unsafe {
                shm_open(
                    cname.as_ptr() as *const c_char,
                    O_CREAT | O_RDWR | O_TRUNC,
                    0o600,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            let map_size = mapping_size(capacity);

            // SAFETY: `ftruncate` sets the segment size. `fd` is valid.
            let rc = unsafe { ftruncate(fd, map_size as i64) };
            if rc < 0 {
                // SAFETY: `fd` is a valid open file descriptor.
                unsafe { libc::close(fd) };
                return Err(io::Error::last_os_error());
            }

            // SAFETY: `mmap` maps `map_size` bytes of the shm segment. `fd` is valid.
            let ptr = unsafe {
                mmap(
                    std::ptr::null_mut(),
                    map_size,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    fd,
                    0,
                )
            };

            // SAFETY: `fd` is no longer needed after mmap; the mapping persists.
            unsafe { libc::close(fd) };

            if ptr == MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            let ptr = ptr as *mut u8;

            // Initialise header fields — capacity first, then signal ready.
            // SAFETY: `ptr` points to a valid, writable mapping of at least `SLOTS_OFFSET` bytes.
            // We use `&mut *` for the capacity field (plain u64, not atomic) and
            // `&*` for the atomic fields (which provide interior mutability).
            let header_mut = unsafe { &mut *(ptr as *mut ShmHeader) };
            header_mut.capacity = capacity as u64;
            header_mut.head.store(0, Ordering::Relaxed);
            header_mut.tail.store(0, Ordering::Relaxed);

            // Release fence ensures all prior stores are visible before ready is set.
            header_mut.ready.store(READY_MAGIC, Ordering::Release);

            Ok(Self { ptr, map_size, name: name.to_owned(), capacity })
        }

        /// Push a candle, spinning until space is available.
        #[inline]
        pub fn push(&self, candle: Candle) {
            loop {
                if self.try_push(candle) {
                    return;
                }
                std::hint::spin_loop();
            }
        }

        /// Try to push without blocking. Returns `false` if the ring is full.
        #[inline]
        pub fn try_push(&self, candle: Candle) -> bool {
            // SAFETY: `ptr` is a valid mapping containing an `ShmHeader` at offset 0.
            let header = unsafe { &*(self.ptr as *const ShmHeader) };
            let head = header.head.load(Ordering::Relaxed);
            let tail = header.tail.load(Ordering::Acquire);

            if head.wrapping_sub(tail) >= self.capacity as u64 {
                return false;
            }

            let slot = head as usize & (self.capacity - 1);
            let slot_ptr = unsafe {
                self.ptr
                    .add(SLOTS_OFFSET + slot * std::mem::size_of::<Candle>())
                    as *mut Candle
            };

            // SAFETY: Slot belongs exclusively to writer when head - tail < capacity.
            unsafe { slot_ptr.write(candle) };

            header.head.store(head.wrapping_add(1), Ordering::Release);
            true
        }
    }

    impl Drop for ShmRingWriter {
        fn drop(&mut self) {
            // SAFETY: `ptr` and `map_size` describe a live mmap region created in `create`.
            unsafe { munmap(self.ptr as *mut libc::c_void, self.map_size) };

            if let Ok(cname) = std::ffi::CString::new(self.name.as_str()) {
                // SAFETY: `cname` is a valid C string holding the shm segment name.
                unsafe { shm_unlink(cname.as_ptr() as *const c_char) };
            }
        }
    }

    // ── ShmRingReader ────────────────────────────────────────────────────────

    /// Consumer side of a cross-process SPSC ring backed by POSIX shared memory.
    ///
    /// Dropping this type calls `munmap` only; the writer owns `shm_unlink`.
    pub struct ShmRingReader {
        ptr:      *mut u8,
        map_size: usize,
        capacity: usize,
    }

    /// SAFETY: The pointer `ptr` is valid for the lifetime of `ShmRingReader`.
    /// Only the reader role modifies `tail`; the writer only modifies `head`.
    unsafe impl Send for ShmRingReader {}

    impl ShmRingReader {
        /// Open an existing shared-memory ring created by [`ShmRingWriter::create`].
        ///
        /// Spins until the writer signals `READY_MAGIC` or `timeout` elapses.
        ///
        /// # Errors
        ///
        /// Returns `io::Error` on `shm_open` or `mmap` failure, or if the writer
        /// does not become ready within 5 seconds.
        pub fn open(name: &str, capacity: usize) -> io::Result<Self> {
            assert!(capacity.is_power_of_two(), "ShmRingReader capacity must be a power of two");

            let cname = std::ffi::CString::new(name)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            // Open with O_RDWR because the reader must write to the `tail` cursor
            // in the shared header. An O_RDONLY fd cannot back a PROT_WRITE mapping.
            // SAFETY: standard POSIX syscall with a valid C string.
            let fd = unsafe {
                shm_open(cname.as_ptr() as *const c_char, O_RDWR, 0o600)
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            let map_size = mapping_size(capacity);

            // SAFETY: `mmap` maps the existing segment read-write so the reader
            // can advance the tail cursor.
            let ptr = unsafe {
                mmap(
                    std::ptr::null_mut(),
                    map_size,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    fd,
                    0,
                )
            };

            // SAFETY: fd can be closed after mmap.
            unsafe { libc::close(fd) };

            if ptr == MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            let ptr = ptr as *mut u8;

            // Spin until writer signals ready (up to 5 seconds).
            // SAFETY: `ptr` points to a valid mapping containing `ShmHeader`.
            let header = unsafe { &*(ptr as *const ShmHeader) };
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                if header.ready.load(Ordering::Acquire) == READY_MAGIC {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    // SAFETY: unmapping the region we just created.
                    unsafe { munmap(ptr as *mut libc::c_void, map_size) };
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "shm writer did not become ready within 5s",
                    ));
                }
                std::hint::spin_loop();
            }

            Ok(Self { ptr, map_size, capacity })
        }

        /// Pop a candle, spinning until one is available.
        #[inline]
        pub fn pop(&self) -> Candle {
            loop {
                if let Some(c) = self.try_pop() {
                    return c;
                }
                std::hint::spin_loop();
            }
        }

        /// Try to pop without blocking. Returns `None` if the ring is empty.
        #[inline]
        pub fn try_pop(&self) -> Option<Candle> {
            // SAFETY: `ptr` is a valid mapping containing an `ShmHeader` at offset 0.
            let header = unsafe { &*(self.ptr as *const ShmHeader) };
            let tail = header.tail.load(Ordering::Relaxed);
            let head = header.head.load(Ordering::Acquire);

            if head == tail {
                return None;
            }

            let slot = tail as usize & (self.capacity - 1);
            let slot_ptr = unsafe {
                self.ptr
                    .add(SLOTS_OFFSET + slot * std::mem::size_of::<Candle>())
                    as *const Candle
            };

            // SAFETY: Slot was written by writer; head > tail guarantees it is ready.
            let candle = unsafe { slot_ptr.read() };

            header.tail.store(tail.wrapping_add(1), Ordering::Release);
            Some(candle)
        }
    }

    impl Drop for ShmRingReader {
        fn drop(&mut self) {
            // SAFETY: `ptr` and `map_size` describe the live mmap region.
            unsafe { munmap(self.ptr as *mut libc::c_void, self.map_size) };
        }
    }

    // ── ShmIngester ──────────────────────────────────────────────────────────

    /// Background thread that continuously drains a [`ShmRingReader`] into a
    /// [`CandleStore`] by calling `store.append(symbol, candle)` on every pop.
    ///
    /// The thread spins on `try_pop`. Dropping the ingester (or calling
    /// [`stop`](ShmIngester::stop)) signals the thread to exit and joins it.
    ///
    /// # Usage
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use candlestore::{CandleStore, ShmRingWriter, ShmRingReader, ShmIngester};
    ///
    /// let store  = Arc::new(CandleStore::from_hardware(10));
    /// let writer = ShmRingWriter::create("/my_feed", 65536).unwrap();
    /// let reader = ShmRingReader::open("/my_feed", 65536).unwrap();
    /// let _ingest = ShmIngester::start(reader, Arc::clone(&store), "BTCUSDT:1m");
    ///
    /// // writer.push(candle) in another thread / process
    /// ```
    pub struct ShmIngester {
        running: Arc<AtomicBool>,
        handle:  Option<std::thread::JoinHandle<()>>,
    }

    impl ShmIngester {
        /// Spawn the ingester thread.
        ///
        /// The thread owns `reader` and `store`. It pops candles and calls
        /// `store.append(symbol, candle)` until [`stop`](Self::stop) is called.
        pub fn start(
            reader: ShmRingReader,
            store:  Arc<crate::store::CandleStore>,
            symbol: impl Into<String>,
        ) -> Self {
            let running  = Arc::new(AtomicBool::new(true));
            let running2 = Arc::clone(&running);
            let symbol   = symbol.into();

            let handle = std::thread::spawn(move || {
                while running2.load(Ordering::Relaxed) {
                    match reader.try_pop() {
                        Some(candle) => store.append(&symbol, candle),
                        None         => std::hint::spin_loop(),
                    }
                }
            });

            Self { running, handle: Some(handle) }
        }

        /// Signal the ingester thread to stop and wait for it to exit.
        pub fn stop(&mut self) {
            self.running.store(false, Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    impl Drop for ShmIngester {
        fn drop(&mut self) {
            self.stop();
        }
    }
}

// Re-export platform types at module level.
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub use shm_impl::{ShmRingReader, ShmRingWriter, ShmIngester};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(ts: i64) -> Candle {
        Candle { ts, open: 1.0, high: 2.0, low: 0.5, close: 1.5, volume: 10.0 }
    }

    // ── SpscRing ────────────────────────────────────────────────────────────

    #[test]
    fn spsc_try_push_pop_roundtrip() {
        let (w, r) = SpscRing::new(4);
        assert!(w.try_push(candle(1)));
        assert!(w.try_push(candle(2)));
        assert_eq!(r.try_pop().unwrap().ts, 1);
        assert_eq!(r.try_pop().unwrap().ts, 2);
        assert!(r.try_pop().is_none());
    }

    #[test]
    fn spsc_try_push_returns_false_when_full() {
        let (w, _r) = SpscRing::new(2);
        assert!(w.try_push(candle(1)));
        assert!(w.try_push(candle(2)));
        assert!(!w.try_push(candle(3))); // full
    }

    #[test]
    fn spsc_push_pop_across_threads() {
        let (w, r) = SpscRing::new(64);
        let n = 1_000usize;

        let writer = std::thread::spawn(move || {
            for i in 0..n {
                w.push(candle(i as i64));
            }
        });

        let reader = std::thread::spawn(move || {
            for i in 0..n {
                let c = r.pop();
                assert_eq!(c.ts, i as i64);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn spsc_wraps_correctly() {
        let (w, r) = SpscRing::new(4);
        // Fill, drain, fill again — tests wrap-around
        for i in 0..4i64 { assert!(w.try_push(candle(i))); }
        for i in 0..4i64 { assert_eq!(r.try_pop().unwrap().ts, i); }
        for i in 4..8i64 { assert!(w.try_push(candle(i))); }
        for i in 4..8i64 { assert_eq!(r.try_pop().unwrap().ts, i); }
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn spsc_non_power_of_two_panics() {
        let _ = SpscRing::new(3);
    }

    // ── ShmHeader layout ────────────────────────────────────────────────────

    #[test]
    fn shm_header_size_and_offsets() {
        use std::mem::{offset_of, size_of};
        assert_eq!(size_of::<ShmHeader>(), 384);
        assert_eq!(offset_of!(ShmHeader, head), 128);
        assert_eq!(offset_of!(ShmHeader, tail), 256);
    }

    // ── ShmRingWriter / ShmRingReader (macOS / Linux only) ──────────────────

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shm_writer_reader_roundtrip() {
        // On macOS, O_TRUNC on an already-mapped segment returns EINVAL.
        // ShmRingWriter::create calls shm_unlink first to clear any stale
        // segment from a previous test run before opening fresh.
        let name = "/spsc1";
        let cap = 64usize;
        let n = 100usize;

        let w = ShmRingWriter::create(name, cap).expect("create shm writer");

        // Open reader in a separate thread — it will spin until ready.
        let reader_thread = std::thread::spawn(move || {
            let r = ShmRingReader::open(name, cap).expect("open shm reader");
            let mut received = Vec::with_capacity(n);
            for _ in 0..n {
                received.push(r.pop());
            }
            received
        });

        for i in 0..n {
            w.push(candle(i as i64));
        }

        let received = reader_thread.join().unwrap();
        assert_eq!(received.len(), n);
        for (i, c) in received.iter().enumerate() {
            assert_eq!(c.ts, i as i64);
        }
    }
}
