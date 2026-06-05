/// CPU core affinity helpers.
///
/// On Linux: uses `sched_setaffinity` for hard pinning.
/// On macOS: uses Mach thread affinity tags (soft hint — OS honours it but
///           does not strictly guarantee the thread stays on one core).

/// Pin the calling thread to `core_id`. Returns `true` on success.
///
/// - Linux: hard pinning via `sched_setaffinity` — thread is strictly bound
/// - macOS: soft affinity tag via Mach `thread_policy_set` — the scheduler
///   honours it as a co-location hint but cannot guarantee strict binding
///
/// Call at thread startup before the hot loop begins.
pub fn pin_to_core(core_id: usize) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux::set_affinity(core_id)
    }
    #[cfg(target_os = "macos")]
    {
        macos::set_affinity_tag(core_id)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = core_id;
        false
    }
}

/// Number of logical cores visible to the process.
pub fn available_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

// ── Linux ─────────────────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod linux {
    pub fn set_affinity(core_id: usize) -> bool {
        use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
        use std::mem;

        unsafe {
            let mut set: cpu_set_t = mem::zeroed();
            CPU_ZERO(&mut set);
            CPU_SET(core_id, &mut set);
            sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &set) == 0
        }
    }
}

// ── macOS ─────────────────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
mod macos {
    use libc::c_int;

    // Mach thread affinity constants not exposed by libc — declare manually.
    const THREAD_AFFINITY_POLICY:       c_int = 4;
    const THREAD_AFFINITY_POLICY_COUNT: u32   = 1;

    #[repr(C)]
    struct ThreadAffinityPolicyData { affinity_tag: c_int }

    unsafe extern "C" {
        fn mach_thread_self() -> u32;
        fn thread_policy_set(
            thread:  u32,
            flavor:  c_int,
            info:    *const ThreadAffinityPolicyData,
            count:   u32,
        ) -> c_int;
    }

    pub fn set_affinity_tag(core_id: usize) -> bool {
        // Tags start at 1; tag 0 means "no affinity preference".
        let policy = ThreadAffinityPolicyData { affinity_tag: (core_id + 1) as c_int };
        unsafe {
            thread_policy_set(
                mach_thread_self(),
                THREAD_AFFINITY_POLICY,
                &policy,
                THREAD_AFFINITY_POLICY_COUNT,
            ) == 0
        }
    }
}
