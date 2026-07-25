//! Measures what each write-ahead log sync policy costs on this machine.
//!
//! ```text
//! cargo run --release --example wal_bench
//! ```
//!
//! Each configuration appends 16 byte keys and 100 byte values until it reaches
//! the append cap or the time cap, whichever comes first, then flushes once more
//! so every run ends fully durable.

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use lsmkv::{SyncPolicy, Wal};

const VALUE_SIZE: usize = 100;
const MAX_APPENDS: u64 = 100_000;
const MAX_TIME: Duration = Duration::from_secs(3);
const THREAD_COUNTS: [usize; 2] = [1, 8];

struct Measurement {
    policy: &'static str,
    threads: usize,
    appends: u64,
    flushes: u64,
    elapsed: Duration,
}

fn main() {
    let dir = std::env::temp_dir().join(format!("lsmkv-wal-bench-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("create the benchmark directory");

    let policies = [
        ("always", SyncPolicy::Always),
        ("group", SyncPolicy::Group),
        (
            "interval 10ms",
            SyncPolicy::Interval(Duration::from_millis(10)),
        ),
    ];

    let mut results = Vec::new();
    for (name, policy) in policies {
        for threads in THREAD_COUNTS {
            let path = dir.join(format!("{}-{threads}.wal", name.replace(' ', "-")));
            results.push(measure(name, policy, threads, &path));
            let _ = fs::remove_file(&path);
        }
    }

    report(&results);
    let _ = fs::remove_dir_all(&dir);
}

fn measure(policy: &'static str, sync: SyncPolicy, threads: usize, path: &Path) -> Measurement {
    let (wal, _) = Wal::recover(path, sync, 0).expect("open the log");
    let value = vec![b'v'; VALUE_SIZE];
    let per_thread = MAX_APPENDS / threads as u64;
    let appends = AtomicU64::new(0);

    let started = Instant::now();
    let deadline = started + MAX_TIME;
    thread::scope(|scope| {
        for writer in 0..threads {
            let (wal, value, appends) = (&wal, &value, &appends);
            scope.spawn(move || {
                let mut key = [0u8; 16];
                key[..8].copy_from_slice(&(writer as u64).to_be_bytes());
                let mut done = 0u64;
                while done < per_thread {
                    key[8..].copy_from_slice(&done.to_be_bytes());
                    wal.set(&key, value).expect("append");
                    done += 1;
                    // The clock is read every 16 appends so its cost stays out
                    // of the fastest policy's numbers.
                    if done.is_multiple_of(16) && Instant::now() >= deadline {
                        break;
                    }
                }
                appends.fetch_add(done, Ordering::Relaxed);
            });
        }
    });
    wal.sync().expect("final flush");

    Measurement {
        policy,
        threads,
        appends: appends.load(Ordering::Relaxed),
        flushes: wal.flush_count(),
        elapsed: started.elapsed(),
    }
}

// Append and flush counts stay far below 2^53, where f64 stops being exact.
#[allow(clippy::cast_precision_loss)]
fn report(results: &[Measurement]) {
    println!(
        "| policy | threads | appends | flushes | appends per flush | appends/s | mean latency |"
    );
    println!(
        "|--------|---------|---------|---------|-------------------|-----------|--------------|"
    );
    for m in results {
        let seconds = m.elapsed.as_secs_f64();
        let per_second = m.appends as f64 / seconds;
        let per_flush = m.appends as f64 / m.flushes as f64;
        let latency_us = seconds * 1e6 * m.threads as f64 / m.appends as f64;
        println!(
            "| {} | {} | {} | {} | {:.1} | {:.0} | {:.0} µs |",
            m.policy, m.threads, m.appends, m.flushes, per_flush, per_second, latency_us
        );
    }
}
