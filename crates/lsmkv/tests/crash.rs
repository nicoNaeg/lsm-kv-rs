//! Kills the process under a write load and checks that what the store
//! acknowledged is still there when it reopens.
//!
//! The README argues crash safety from three orderings: a table renamed into
//! place only once it is complete, a directory entry made durable before any
//! log is unlinked, and the logs unlinked last. This is the test that stops
//! those being an argument.
//!
//! Nothing here changes the engine or stands between it and the filesystem. The
//! process is killed with `SIGKILL`, which no destructor and no handler gets to
//! soften, so what survives is what actually reached the device.
//!
//! # The oracle
//!
//! A crash test has to answer what the dead process had acknowledged, and any
//! channel it might report through is subject to the same crash. This one has
//! no channel. The child writes `k0`, `k1`, `k2` in order under a policy where
//! an append returns only once it is durable, and never starts one before the
//! last returned. The keys that survive must therefore be a *prefix* of that
//! sequence: a hole is an acknowledged write that was lost, a wrong value is
//! corruption, and a key past the end is a write that was never acknowledged
//! surfacing anyway.
//!
//! Each run continues from the highest key it finds, so the store deepens
//! across iterations instead of rewriting the same prefix, and later crashes
//! land on a tree with more levels under it.
//!
//! # Why the memtable is tiny
//!
//! A random kill only finds an ordering bug if it lands in the window that bug
//! opens, and those windows are narrow: a couple of device flushes wide, a few
//! milliseconds. What decides whether this test finds anything is therefore the
//! fraction of its runtime the store spends inside one, which is set by how
//! often it flushes.
//!
//! That is not a detail. At a 2 KiB memtable this test passed against an engine
//! with the log unlinked before the manifest names the table, the exact defect
//! the ordering exists to prevent, because a flush every 300 ms with an 8 ms
//! window is 2 % of the run and thirty kills expect 0.7 hits. At 256 bytes the
//! store is almost always mid-flush, and the same defect fails at iteration 5.
//! The configuration below is the test's power, not its scenery.

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime};

use lsmkv::{Config, Engine, SyncPolicy};

/// Set on the child, holding the directory it should write to.
const CHILD_ENV: &str = "LSMKV_CRASH_CHILD";
const TEST_NAME: &str = "an_acknowledged_write_survives_a_crash";
/// Overridden for a longer campaign than CI wants to pay for.
const ITERATIONS_ENV: &str = "LSMKV_CRASH_ITERATIONS";
const DEFAULT_ITERATIONS: u32 = 30;
/// Keys past the end of the prefix that are checked to be absent.
const MARGIN: u64 = 64;
/// Below this the child barely ran and the iterations were close to vacuous.
/// A local run of the default iterations reaches a few thousand.
const MINIMUM_KEYS: u64 = 400;

/// Sized so the store is almost always mid-flush. This is what gives the test
/// its power: the window a crash has to land in to expose a bad ordering is a
/// couple of device flushes wide, so the run has to spend as much of its time
/// inside one as possible. At a few hundred bytes the table freezes every
/// handful of writes.
fn config() -> Config {
    Config {
        sync: SyncPolicy::Group,
        memtable_bytes: 256,
        l0_trigger: 2,
        fanout: 2,
        ..Config::default()
    }
}

fn key(n: u64) -> Vec<u8> {
    format!("k{n:012}").into_bytes()
}

fn value(n: u64) -> Vec<u8> {
    format!("v{n:012}").into_bytes()
}

#[test]
fn an_acknowledged_write_survives_a_crash() {
    if let Ok(dir) = env::var(CHILD_ENV) {
        write_until_killed(Path::new(&dir));
    }

    let dir = scratch();
    let iterations = env::var(ITERATIONS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ITERATIONS);

    // Printed so a failure can be rerun exactly. A crash test that cannot be
    // replayed only tells you that something is wrong.
    let seed = seed();
    println!(
        "crash test seed {seed}, {iterations} iterations, dir {}",
        dir.display()
    );
    let mut rng = Rng::new(seed);

    let mut highest = 0;
    let mut levels = 0;
    for iteration in 0..iterations {
        // Long enough to open the store and get several flushes in, short
        // enough that the kill lands while one is in flight rather than after.
        kill_after(&dir, Duration::from_millis(150 + rng.below(1050)));

        if iteration % 3 == 2 {
            // Killed while it is still replaying what the last crash left, the
            // case a store only reaches after it has already failed once.
            kill_after(&dir, Duration::from_millis(1 + rng.below(40)));
        }

        let (bound, depth) = verify(&dir, iteration);
        highest = bound;
        levels = levels.max(depth);
    }

    // Both of these guard against the run being vacuous rather than correct.
    // The output of a passing test is captured, so a crash test that quietly
    // stopped exercising anything would keep reporting success; these turn that
    // into a failure instead.
    assert!(
        highest >= MINIMUM_KEYS,
        "only {highest} keys were written across {iterations} iterations, \
         so the child barely ran and this proved little"
    );
    assert!(
        levels > 1,
        "the store never reached a second level, so no compaction ran and the \
         crashes landed on flushes at best"
    );
    println!("{highest} keys survived across {levels} levels, prefix intact");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The child: writes in order, forever, one acknowledged write at a time.
fn write_until_killed(dir: &Path) -> ! {
    let engine = Engine::open(dir, config()).expect("the child could not open the store");
    let mut n = first_absent(&engine);
    loop {
        engine
            .set(&key(n), &value(n))
            .expect("the child could not write");
        n += 1;
    }
}

/// Opens the store and checks the surviving keys are a prefix of the sequence,
/// returning how far the prefix reaches and how deep the tree got.
fn verify(dir: &Path, iteration: u32) -> (u64, usize) {
    let engine = Engine::open(dir, config())
        .unwrap_or_else(|err| panic!("iteration {iteration}: the store would not reopen: {err}"));

    let bound = first_absent(&engine);
    for n in 0..bound {
        let found = engine.get(&key(n)).expect("get");
        assert_eq!(
            found.as_deref(),
            Some(value(n).as_slice()),
            "iteration {iteration}: k{n} is missing or wrong below the end of the prefix at {bound}, \
             so a write the store acknowledged did not survive"
        );
    }
    for n in bound..bound + MARGIN {
        let found = engine.get(&key(n)).expect("get");
        assert!(
            found.is_none(),
            "iteration {iteration}: k{n} exists past the end of the prefix at {bound}, \
             so a write surfaced that was never acknowledged"
        );
    }
    (bound, engine.level_sizes().len())
}

/// The first key the store does not hold, found by doubling then bisecting.
///
/// Only ever used as a bound: the check above walks every key below it rather
/// than trusting the search, which would step over exactly the hole it is
/// looking for.
fn first_absent(engine: &Engine) -> u64 {
    let present = |n: u64| engine.get(&key(n)).expect("get").is_some();
    if !present(0) {
        return 0;
    }
    let mut low = 0;
    let mut high = 1;
    while present(high) {
        low = high;
        high *= 2;
    }
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if present(middle) {
            low = middle;
        } else {
            high = middle;
        }
    }
    high
}

fn kill_after(dir: &Path, delay: Duration) {
    let exe = env::current_exe().expect("the test binary");
    let mut child = Command::new(exe)
        .arg(TEST_NAME)
        .arg("--exact")
        .env(CHILD_ENV, dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the writer");

    std::thread::sleep(delay);
    // SIGKILL: no unwinding, no destructors, no last flush on the way out.
    child.kill().expect("kill the writer");
    child.wait().expect("reap the writer");
}

fn scratch() -> PathBuf {
    let dir = env::temp_dir().join(format!("lsmkv-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn seed() -> u64 {
    env::var("LSMKV_CRASH_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("a clock after 1970")
                .subsec_nanos()
                .into()
        })
}

/// splitmix64, so the delays are reproducible from the seed above.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}
