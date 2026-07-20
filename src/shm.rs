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
pub const READY_MAGIC: u64 = 0x00CA_FECA_1D1E_5748;

/// Apple M-series (and many modern x86) cache line = 128 bytes.
const CACHE_LINE: usize = 128;

// ─────────────────────────────────────────────────────────────────────────────
// SpscRing — heap-backed, in-process SPSC
// ─────────────────────────────────────────────────────────────────────────────

/// Aligned wrapper that places an `AtomicU64` counter on its own 128-byte cache line,
/// preventing false sharing between adjacent hot counters on wide-cache-line CPUs
/// (Apple M-series).
///
/// Shared crate-wide: used here for the SPSC head/tail cursors and by
/// `store::CandleStore` to pad its globally-contended `version`/`tick` counters.
#[repr(C, align(128))]
pub(crate) struct CachePadded {
    pub(crate) value: AtomicU64,
    #[allow(dead_code)]
    _pad: [u8; CACHE_LINE - 8], // AtomicU64 is 8 bytes; pad to 128
}

impl CachePadded {
    pub(crate) const fn new(v: u64) -> Self {
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
struct SpscInner<T> {
    slots:    Box<[UnsafeCell<T>]>,
    capacity: usize,
    head:     CachePadded, // producer cursor — written only by writer
    tail:     CachePadded, // consumer cursor — written only by reader
}

/// SAFETY: `SpscWriter` and `SpscReader` each hold a non-overlapping exclusive
/// role (producer / consumer), enforced at construction time. The `UnsafeCell`
/// slots are accessed in a non-overlapping manner by the two roles.
unsafe impl<T: Send> Send for SpscInner<T> {}
unsafe impl<T: Send> Sync for SpscInner<T> {}

use std::sync::Arc;

/// Produces items into a heap-backed SPSC ring buffer.
pub struct SpscWriter<T> {
    inner: Arc<SpscInner<T>>,
}

/// Consumes items from a heap-backed SPSC ring buffer.
pub struct SpscReader<T> {
    inner: Arc<SpscInner<T>>,
}

/// Heap-backed SPSC ring buffer factory.
///
/// `T` must be `Copy + Default + Send`. `capacity` must be a power of two.
pub struct SpscRing;

impl SpscRing {
    /// Create a linked writer/reader pair backed by a heap-allocated ring of `capacity` slots.
    ///
    /// `SpscRing` is a factory namespace (not an instantiable type) — `new`
    /// returns the producer/consumer halves rather than `Self`.
    ///
    /// # Panics
    /// Panics if `capacity` is zero or not a power of two.
    #[allow(clippy::new_ret_no_self)]
    pub fn new<T: Copy + Default + Send>(capacity: usize) -> (SpscWriter<T>, SpscReader<T>) {
        // Check zero FIRST: `is_power_of_two()` is false for 0, so the reverse
        // order would report "must be a power of two" for a zero capacity.
        assert!(capacity > 0, "SpscRing capacity must be > 0");
        assert!(capacity.is_power_of_two(), "SpscRing capacity must be a power of two");

        let slots: Box<[UnsafeCell<T>]> = (0..capacity)
            .map(|_| UnsafeCell::new(T::default()))
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

impl<T: Copy> SpscWriter<T> {
    /// Push an item, spinning until space is available.
    #[inline]
    pub fn push(&self, item: T) {
        loop {
            if self.try_push(item) { return; }
            std::hint::spin_loop();
        }
    }

    /// Try to push without blocking. Returns `false` if the ring is full.
    #[inline]
    pub fn try_push(&self, item: T) -> bool {
        let head = self.inner.head.value.load(Ordering::Relaxed);
        let tail = self.inner.tail.value.load(Ordering::Acquire);

        if head.wrapping_sub(tail) >= self.inner.capacity as u64 {
            return false; // ring full
        }

        let slot = head as usize & (self.inner.capacity - 1);

        // SAFETY: We verified `head - tail < capacity`, so this slot belongs
        // exclusively to the writer. No reader can access it until we advance head.
        unsafe { *self.inner.slots[slot].get() = item; }

        self.inner.head.value.store(head.wrapping_add(1), Ordering::Release);
        true
    }
}

impl<T: Copy> SpscReader<T> {
    /// Pop an item, spinning until one is available.
    #[inline]
    pub fn pop(&self) -> T {
        loop {
            if let Some(v) = self.try_pop() { return v; }
            std::hint::spin_loop();
        }
    }

    /// Try to pop without blocking. Returns `None` if the ring is empty.
    #[inline]
    pub fn try_pop(&self) -> Option<T> {
        let tail = self.inner.tail.value.load(Ordering::Relaxed);
        let head = self.inner.head.value.load(Ordering::Acquire);

        if head == tail { return None; }

        let slot = tail as usize & (self.inner.capacity - 1);

        // SAFETY: We verified `head > tail`, so this slot was written by the
        // writer and is now exclusively available to the reader. The writer will
        // not touch this slot again until tail advances past capacity slots ahead.
        let item = unsafe { *self.inner.slots[slot].get() };

        self.inner.tail.value.store(tail.wrapping_add(1), Ordering::Release);
        Some(item)
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
        shm_open, shm_unlink, O_CREAT, O_EXCL, O_RDWR,
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
        ptr:       *mut u8,
        map_size:  usize,
        name:      String,
        capacity:  usize,
        /// Lifetime counter — incremented once per `try_push` that returns
        /// `false` and forced the caller to wait or back off. Read out via
        /// [`stats`](Self::stats); useful for spotting "consumer is behind"
        /// scenarios in production metrics.
        push_full: AtomicU64,
    }

    /// Lifetime stats for an `ShmRingWriter`, exposed for metrics export.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct WriterStats {
        /// Number of times the ring was full when the writer tried to push.
        /// Sustained nonzero growth means the consumer cannot keep up.
        pub push_full_events: u64,
        /// Configured ring capacity (in slots).
        pub ring_capacity:    u64,
    }

    /// SAFETY: The pointer `ptr` owns the mapped region exclusively until drop.
    /// With `O_CREAT|O_EXCL` (the default in [`ShmRingWriter::create`]) no
    /// second producer can create a segment with the same name while ours is
    /// alive, so the mapping is uniquely ours for its lifetime.
    unsafe impl Send for ShmRingWriter {}

    /// SAFETY: `Sync` is sound for `ShmRingWriter` because all *mutating*
    /// methods (`push`, `try_push`, `push_until`) maintain the SPSC
    /// invariant: exactly one producer thread advances `head`. All other
    /// methods (`stats`) only do atomic loads on counters owned by this
    /// struct — safe from any number of concurrent observer threads (e.g.
    /// the metrics poller). The SPSC contract is a documented user
    /// obligation; the type does not enforce it.
    unsafe impl Sync for ShmRingWriter {}

    impl ShmRingWriter {
        /// Create a new shared-memory ring with the given POSIX name and capacity.
        ///
        /// Opens with `O_CREAT | O_EXCL` — fails with [`io::ErrorKind::AlreadyExists`]
        /// if a segment with that name already exists. This protects against two
        /// racing producers each calling `create` with the same name (which would
        /// silently corrupt each other under the old "unlink then create" pattern).
        ///
        /// For crash recovery (where the previous producer didn't run its Drop
        /// and the segment was left behind), use [`create_force`](Self::create_force).
        ///
        /// `name` must start with `/` per POSIX convention.
        /// `capacity` must be a power of two.
        ///
        /// # Errors
        ///
        /// - [`io::ErrorKind::AlreadyExists`] if the segment is in use.
        /// - Other `io::Error` on `shm_open`, `ftruncate`, or `mmap` failure.
        pub fn create(name: &str, capacity: usize) -> io::Result<Self> {
            Self::create_inner(name, capacity, /*force=*/ false)
        }

        /// Create a SHM ring, removing any stale segment with the same name first.
        ///
        /// Use only for crash recovery — the normal path is [`create`](Self::create)
        /// which fails loudly if the name is in use. `create_force` will silently
        /// stomp on a running producer's segment, which is exactly the bug
        /// `create` exists to prevent.
        ///
        /// Emits `tracing::warn!` so the action is visible in logs.
        pub fn create_force(name: &str, capacity: usize) -> io::Result<Self> {
            tracing::warn!(name, "ShmRingWriter::create_force — bypassing exclusivity check; \
                                   any concurrently-running producer with this name will be corrupted");
            Self::create_inner(name, capacity, /*force=*/ true)
        }

        fn create_inner(name: &str, capacity: usize, force: bool) -> io::Result<Self> {
            assert!(capacity.is_power_of_two(), "ShmRingWriter capacity must be a power of two");

            let cname = std::ffi::CString::new(name)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

            if force {
                // SAFETY: `shm_unlink` is safe to call with a valid C string even if
                // the segment doesn't exist (returns -1, ignored).
                unsafe { shm_unlink(cname.as_ptr() as *const c_char) };
            }

            // O_EXCL + O_CREAT ⇒ atomic create-or-fail. The race window between
            // shm_unlink and shm_open in the old implementation is gone.
            let mut flags = O_CREAT | O_RDWR;
            if !force { flags |= O_EXCL; }
            // SAFETY: `shm_open` is a standard POSIX syscall. `cname` is valid.
            let fd = unsafe { shm_open(cname.as_ptr() as *const c_char, flags, 0o600) };
            if fd < 0 {
                let err = io::Error::last_os_error();
                if !force && err.kind() == io::ErrorKind::AlreadyExists {
                    tracing::error!(
                        name, error = %err,
                        "SHM segment already exists — another producer running, or a \
                         crashed producer left a stale segment. Use create_force for recovery."
                    );
                }
                return Err(err);
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

            Ok(Self {
                ptr, map_size,
                name: name.to_owned(),
                capacity,
                push_full: AtomicU64::new(0),
            })
        }

        /// Push a candle, spinning until space is available.
        ///
        /// **Warning**: spins forever if the consumer disappears. Prefer
        /// [`push_until`](Self::push_until) for any production code path —
        /// without an external cancel signal, SIGTERM cannot interrupt this.
        #[inline]
        pub fn push(&self, candle: Candle) {
            if !self.try_push(candle) {
                self.push_full.fetch_add(1, Ordering::Relaxed);
                while !self.try_push(candle) {
                    std::hint::spin_loop();
                }
            }
        }

        /// Push a candle, spinning until space is available OR `cancel` flips
        /// to `true`. Returns `true` if the candle was pushed, `false` if the
        /// caller cancelled first.
        ///
        /// Use this in any loop that must honour graceful shutdown. Without
        /// it, [`push`](Self::push) hangs the calling thread forever when
        /// the consumer is gone — which is exactly the bug that hid in
        /// `feed_handler` before this method existed.
        ///
        /// `cancel` is loaded with `Relaxed` ordering on every spin
        /// iteration; the cost is negligible against the `try_push` itself.
        #[inline]
        pub fn push_until(&self, candle: Candle, cancel: &AtomicBool) -> bool {
            if self.try_push(candle) { return true; }
            // Ring was full when we entered — count it once per stall, not
            // per spin iteration (which would explode the counter).
            self.push_full.fetch_add(1, Ordering::Relaxed);
            while !self.try_push(candle) {
                if cancel.load(Ordering::Relaxed) { return false; }
                std::hint::spin_loop();
            }
            true
        }

        /// Lifetime counter snapshot for metrics export.
        #[inline]
        pub fn stats(&self) -> WriterStats {
            WriterStats {
                push_full_events: self.push_full.load(Ordering::Relaxed),
                ring_capacity:    self.capacity as u64,
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

    /// SAFETY: `Sync` is sound for `ShmRingReader` because all *mutating*
    /// methods (`try_pop`, `pop`) maintain the SPSC invariant: exactly one
    /// consumer thread advances `tail`. All other methods (`depth`,
    /// `capacity`) only do atomic loads on the shared header — safe from any
    /// number of concurrent observer threads (e.g. a metrics sidecar). The
    /// SPSC contract is a documented user obligation; the type does not
    /// enforce it.
    unsafe impl Sync for ShmRingReader {}

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

            // CRITICAL: verify the writer's capacity matches what the reader
            // expects. If they disagree, the reader's modulo `tail & (cap-1)`
            // indexes slots the writer never wrote, leading to silent UB
            // (off-mapping reads, garbage data, segfault).
            let writer_capacity = header.capacity as usize;
            if writer_capacity != capacity {
                // SAFETY: unmapping the region we just created.
                unsafe { munmap(ptr as *mut libc::c_void, map_size) };
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "ShmRingReader capacity mismatch: reader requested {capacity}, \
                         writer created segment with {writer_capacity}. \
                         The producer and consumer must agree on the ring size."
                    ),
                ));
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

        /// Current ring depth — number of slots holding unread data.
        ///
        /// Reads `head` and `tail` with `Acquire` ordering. Safe to call from
        /// any thread (does not mutate the consumer's logical position) — only
        /// the consumer's own thread should be calling `try_pop` concurrently.
        #[inline]
        pub fn depth(&self) -> u64 {
            // SAFETY: `ptr` is a valid mapping containing an `ShmHeader` at offset 0.
            let header = unsafe { &*(self.ptr as *const ShmHeader) };
            let head = header.head.load(Ordering::Acquire);
            let tail = header.tail.load(Ordering::Acquire);
            head.wrapping_sub(tail)
        }

        /// Configured ring capacity (in slots).
        #[inline]
        pub fn capacity(&self) -> usize { self.capacity }

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
    /// [`crate::CandleStore`] by calling `store.append(symbol, candle)` on every pop.
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
    /// Lifetime stats for an `ShmIngester`. Polled by the observability
    /// sidecar; see `src/bin/market_hub.rs` for an example.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct IngesterStats {
        /// Lifetime candles popped from the ring and forwarded to the store.
        pub popped_total: u64,
        /// Instantaneous ring depth (unread messages waiting).
        pub ring_depth:   u64,
        /// Configured ring capacity.
        pub ring_capacity: u64,
    }

    pub struct ShmIngester {
        running: Arc<AtomicBool>,
        handle:  Option<std::thread::JoinHandle<()>>,
        popped:  Arc<AtomicU64>,
        reader:  Arc<ShmRingReader>,
    }

    impl ShmIngester {
        /// Spawn the ingester thread, optionally pinning it to `core_id`.
        ///
        /// `core_id = Some(n)`: the inner thread calls `pin_to_core(n)` before
        /// starting its pop loop (Linux: hard pin via `sched_setaffinity`;
        /// macOS: best-effort `thread_policy_set` hint).
        /// `core_id = None`: thread runs unpinned.
        ///
        /// The thread shares `reader` via `Arc` so the host process can query
        /// `stats()` (depth, popped) for observability. Only the ingester
        /// thread calls `try_pop`, preserving the SPSC invariant.
        pub fn start_on_core(
            reader:  ShmRingReader,
            store:   Arc<crate::store::CandleStore>,
            symbol:  impl Into<String>,
            core_id: Option<usize>,
        ) -> Self {
            let running  = Arc::new(AtomicBool::new(true));
            let popped   = Arc::new(AtomicU64::new(0));
            let reader   = Arc::new(reader);

            let running2 = Arc::clone(&running);
            let popped2  = Arc::clone(&popped);
            let reader2  = Arc::clone(&reader);
            let symbol   = symbol.into();

            let handle = std::thread::Builder::new()
                .name("shm-ingester".into())
                .spawn(move || {
                    if let Some(c) = core_id {
                        crate::affinity::pin_to_core(c);
                    }
                    while running2.load(Ordering::Relaxed) {
                        match reader2.try_pop() {
                            Some(candle) => {
                                store.append(&symbol, candle);
                                popped2.fetch_add(1, Ordering::Relaxed);
                            }
                            None => std::hint::spin_loop(),
                        }
                    }
                })
                .expect("spawn shm-ingester thread");

            Self { running, handle: Some(handle), popped, reader }
        }

        /// Spawn the ingester thread on whichever core the OS scheduler picks.
        /// Equivalent to `start_on_core(reader, store, symbol, None)`.
        pub fn start(
            reader: ShmRingReader,
            store:  Arc<crate::store::CandleStore>,
            symbol: impl Into<String>,
        ) -> Self {
            Self::start_on_core(reader, store, symbol, None)
        }

        /// Signal the ingester thread to stop and wait for it to exit.
        pub fn stop(&mut self) {
            self.running.store(false, Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }

        /// Signal the ingester thread to exit at its next iteration —
        /// without joining. Takes `&self` so it works through `Arc<Self>`
        /// (the natural sharing pattern when multiple components — main,
        /// metrics poller — hold the ingester).
        ///
        /// Use this at the START of shutdown to halt the ingester's hot
        /// loop before the strategy/executor threads join. Without this,
        /// the ingester keeps pumping data into a store no one is reading
        /// during the join window — small CPU waste but visible in metrics.
        /// The full join happens at [`Drop`](Self::drop), when the last
        /// `Arc<ShmIngester>` clone is released.
        pub fn stop_signal(&self) {
            self.running.store(false, Ordering::Relaxed);
        }

        /// Lifetime counter snapshot for metrics export.
        pub fn stats(&self) -> IngesterStats {
            IngesterStats {
                popped_total:  self.popped.load(Ordering::Relaxed),
                ring_depth:    self.reader.depth(),
                ring_capacity: self.reader.capacity() as u64,
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
pub use shm_impl::{ShmRingReader, ShmRingWriter, ShmIngester, IngesterStats, WriterStats};

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
        let _ = SpscRing::new::<Candle>(3);
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

    /// Unique per-test SHM segment name so failed tests don't leak state
    /// across runs. Uses a static counter + the test thread id.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn unique_shm_name(prefix: &str) -> String {
        use std::sync::atomic::AtomicU64;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!(
            "/cs_test_{prefix}_{id}_{:?}",
            std::thread::current().id()
        ).replace(|c: char| !c.is_alphanumeric() && c != '_' && c != '/', "")
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shm_writer_reader_roundtrip() {
        let name = unique_shm_name("rt");
        let cap = 64usize;
        let n = 100usize;

        let w = ShmRingWriter::create(&name, cap).expect("create shm writer");

        let name2 = name.clone();
        let reader_thread = std::thread::spawn(move || {
            let r = ShmRingReader::open(&name2, cap).expect("open shm reader");
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

    // ── S1: O_EXCL race-prevention ─────────────────────────────────────────

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shm_writer_create_rejects_duplicate_with_already_exists() {
        let name = unique_shm_name("excl");
        let _w1 = ShmRingWriter::create(&name, 64).expect("first create succeeds");

        // Second create on the SAME name must fail with AlreadyExists.
        // Without O_EXCL the old code would silently corrupt the first writer.
        match ShmRingWriter::create(&name, 64) {
            Ok(_) => panic!("second create must fail"),
            Err(e) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::AlreadyExists,
                "expected AlreadyExists, got {e:?}"
            ),
        }
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shm_writer_create_force_overrides_existing_segment() {
        let name = unique_shm_name("force");
        let _w1 = ShmRingWriter::create(&name, 64).expect("first create succeeds");

        // create_force is the documented recovery path — it unlinks first.
        // This is racy by design (a running producer would be stomped) but
        // we want the operator-controlled escape hatch.
        let _w2 = ShmRingWriter::create_force(&name, 64).expect("create_force succeeds");
    }

    // ── S2: capacity-mismatch detection ────────────────────────────────────

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shm_reader_open_rejects_capacity_mismatch() {
        let name = unique_shm_name("cap");
        let _w = ShmRingWriter::create(&name, 64).expect("create with cap=64");

        // Reader requests a different capacity → must fail rather than
        // mapping a wrong-sized region and silently reading garbage.
        match ShmRingReader::open(&name, 128) {
            Ok(_) => panic!("mismatched capacity must fail"),
            Err(e) => {
                assert_eq!(
                    e.kind(),
                    std::io::ErrorKind::InvalidData,
                    "expected InvalidData, got {e:?}"
                );
                assert!(
                    e.to_string().contains("capacity mismatch"),
                    "error message must mention capacity mismatch: {e}"
                );
            }
        }
    }

    // ── S3: push_until honours the cancel signal ───────────────────────────

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn shm_writer_push_until_returns_false_on_cancel() {
        use std::time::Duration;
        let name = unique_shm_name("cancel");
        let cap = 4usize;
        let writer = ShmRingWriter::create(&name, cap).expect("create");

        // Fill the ring so try_push fails. No reader → writer would spin
        // forever in `push`; `push_until` must bail on the cancel signal.
        for i in 0..cap {
            assert!(writer.try_push(candle(i as i64)));
        }

        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_signal = Arc::clone(&cancel);
        let cancel_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancel_signal.store(true, Ordering::Release);
        });

        let start = std::time::Instant::now();
        let pushed = writer.push_until(candle(99), &cancel);
        let elapsed = start.elapsed();
        cancel_thread.join().unwrap();

        assert!(!pushed, "push_until must return false when cancel fires");
        assert!(
            elapsed < Duration::from_secs(1),
            "push_until must exit promptly after cancel, took {elapsed:?}"
        );

        // Backpressure counter incremented (consumer was behind).
        let stats = writer.stats();
        assert_eq!(stats.push_full_events, 1, "exactly one stall expected");
    }
}
