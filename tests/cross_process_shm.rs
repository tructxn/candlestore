//! Cross-process SHM integration tests. R3 from the deep review.
//!
//! The "production" path for SHM rings is `feed_handler` (PID A) writing to
//! `market_hub` (PID B) — two OS processes sharing a POSIX shm segment.
//! Until now, every test for `ShmRingWriter`/`Reader` was same-process —
//! the cross-process protocol (O_EXCL, ready-magic handshake, capacity
//! verification, segment cleanup on Drop) had ZERO automated coverage.
//!
//! These tests use the `std::env::current_exe()` self-spawn pattern: a
//! test process spawns ITSELF as a child with a known env var (the role)
//! and the child runs as the reader/writer half. The parent does the
//! other half, joins, and asserts.
//!
//! Linux/macOS only (the lib is POSIX-shm only).

#![cfg(any(target_os = "linux", target_os = "macos"))]

use candlestore::{Candle, ShmRingReader, ShmRingWriter};
use std::process::{Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CHILD_ROLE_ENV: &str = "CSCT_CHILD_ROLE";
const SHM_NAME_ENV:   &str = "CSCT_SHM_NAME";
const SHM_CAP_ENV:    &str = "CSCT_SHM_CAP";
const N_MSG_ENV:      &str = "CSCT_N_MSG";

/// Build a SHM name unique to this test invocation so two test runs in the
/// same shell don't collide. macOS limits POSIX shm names to 31 chars
/// total (NAME_MAX = 31), so we keep this short.
fn unique_shm_name(tag: &str) -> String {
    // pid % 1_000_000  (≤ 6 digits)
    let pid_short = std::process::id() % 1_000_000;
    let tag_short: String = tag.chars().take(4).collect();
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_micros();
    // total: /xp_{4}_{6}_{6} = at most 1+3+4+1+6+1+6 = 22 chars. Safe.
    format!("/xp_{tag_short}_{pid_short}_{micros}")
}

/// If invoked with `CSCT_CHILD_ROLE=reader`, run the reader half and exit.
/// Called at the top of each test. Returns `true` if the test should continue
/// as the parent (and `false` if we already ran as a child and exited).
fn maybe_run_as_child() {
    let role = match std::env::var(CHILD_ROLE_ENV) {
        Ok(r) => r,
        Err(_) => return,
    };
    let name = std::env::var(SHM_NAME_ENV).expect("child needs SHM name");
    let cap: usize = std::env::var(SHM_CAP_ENV).expect("child needs SHM cap").parse().unwrap();
    let n:   usize = std::env::var(N_MSG_ENV).expect("child needs N").parse().unwrap();

    match role.as_str() {
        "reader" => {
            let r = ShmRingReader::open(&name, cap).expect("child: open reader");
            let mut received = Vec::with_capacity(n);
            let deadline = Instant::now() + Duration::from_secs(10);
            while received.len() < n {
                if let Some(c) = r.try_pop() {
                    received.push(c);
                } else if Instant::now() > deadline {
                    eprintln!("child: timeout waiting for {n} messages, got {}", received.len());
                    std::process::exit(2);
                } else {
                    std::hint::spin_loop();
                }
            }
            // Verify monotonic ts starting at 1.
            for (i, c) in received.iter().enumerate() {
                if c.ts != (i + 1) as i64 {
                    eprintln!("child: ts mismatch at {i}: expected {}, got {}", i + 1, c.ts);
                    std::process::exit(3);
                }
            }
            std::process::exit(0);
        }
        "reader_wrong_cap" => {
            // The child opens with the WRONG capacity. We want this to FAIL,
            // and we don't care exactly where in the chain it fails:
            //
            // - Linux: mmap silently expands → our header.capacity check
            //   catches it → InvalidData
            // - macOS: mmap rejects oversize mapping → InvalidInput (EINVAL)
            //
            // Both prove the bug is caught BEFORE we read corrupt data.
            // Open-succeeded is the only failure mode for this test.
            match ShmRingReader::open(&name, cap) {
                Ok(_) => {
                    eprintln!("child: open with wrong cap unexpectedly succeeded");
                    std::process::exit(1);
                }
                Err(e) => {
                    use std::io::ErrorKind::*;
                    let acceptable = matches!(e.kind(), InvalidData | InvalidInput);
                    if acceptable {
                        eprintln!("child: open correctly rejected wrong cap: {e}");
                        std::process::exit(0);
                    } else {
                        eprintln!("child: open failed with UNEXPECTED error: {e:?}");
                        std::process::exit(4);
                    }
                }
            }
        }
        other => {
            eprintln!("child: unknown role {other:?}");
            std::process::exit(5);
        }
    }
}

fn candle(ts: i64) -> Candle {
    Candle { ts, open: 100.0, high: 101.0, low: 99.0, close: 100.5, volume: 1.0 }
}

#[test]
fn writer_in_parent_reader_in_child_roundtrip_succeeds() {
    maybe_run_as_child();

    let name = unique_shm_name("roundtrip");
    let cap  = 1024usize;
    let n    = 200usize;

    // Parent creates the segment first (so the child's open() finds it).
    let writer = ShmRingWriter::create(&name, cap).expect("parent: create writer");

    let me = std::env::current_exe().expect("current_exe");
    let child = Command::new(&me)
        .arg("--exact")
        .arg("writer_in_parent_reader_in_child_roundtrip_succeeds")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "reader")
        .env(SHM_NAME_ENV,   &name)
        .env(SHM_CAP_ENV,    cap.to_string())
        .env(N_MSG_ENV,      n.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child");

    // Parent writes N candles. push_until honours a cancel flag; we never
    // cancel here, but using push_until exercises the same lib API that
    // production binaries use.
    let cancel = Arc::new(AtomicBool::new(false));
    for i in 1..=n as i64 {
        let ok = writer.push_until(candle(i), &cancel);
        assert!(ok, "parent: push_until must succeed (cancel never fires)");
    }

    let output = child.wait_with_output().expect("child wait");
    assert!(
        output.status.success(),
        "child exited non-zero ({:?}): stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    // Dropping writer here runs ShmRingWriter::Drop → munmap + shm_unlink.
    drop(writer);
}

#[test]
fn reader_with_wrong_capacity_fails_loudly() {
    // S2 in production: a deployed binary built with the wrong default for
    // FEED_SHM_CAP would silently misread; the capacity check now makes it
    // a clean error.
    maybe_run_as_child();

    let name = unique_shm_name("capmismatch");
    let writer_cap = 1024usize;
    let reader_cap = 2048usize; // child opens with this — must fail

    let _writer = ShmRingWriter::create(&name, writer_cap).expect("parent: create writer");

    let me = std::env::current_exe().expect("current_exe");
    let child = Command::new(&me)
        .arg("--exact")
        .arg("reader_with_wrong_capacity_fails_loudly")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, "reader_wrong_cap")
        .env(SHM_NAME_ENV,   &name)
        .env(SHM_CAP_ENV,    reader_cap.to_string())
        .env(N_MSG_ENV,      "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child");

    let output = child.wait_with_output().expect("child wait");
    assert!(
        output.status.success(),
        "child must exit 0 (signalling it observed the expected InvalidData), got {:?}; stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn parent_create_fails_when_segment_already_exists() {
    // S1 in production: a second producer with the same segment name must
    // not be able to silently corrupt the first one. O_EXCL makes it a
    // hard error. This test is pure same-process but uses two writers
    // against the same name — same code path as cross-process collision.
    maybe_run_as_child();

    let name = unique_shm_name("excl_xp");
    let cap  = 64usize;

    let _w1 = ShmRingWriter::create(&name, cap).expect("first writer must succeed");

    match ShmRingWriter::create(&name, cap) {
        Ok(_) => panic!("second writer for same name must fail"),
        Err(e) => assert_eq!(
            e.kind(),
            std::io::ErrorKind::AlreadyExists,
            "expected AlreadyExists, got {e:?}"
        ),
    }
}
