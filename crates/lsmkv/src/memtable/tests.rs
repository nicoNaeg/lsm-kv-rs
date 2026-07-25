//! The contract every [`Memtable`] has to satisfy, run against both
//! implementations.
//!
//! Anything specific to one structure (its size accounting, its internal
//! ordering) is tested next to that structure instead.

use std::thread;

use super::{BTreeMemtable, Memtable, SkipMemtable};
use crate::lookup::Lookup;

/// Large enough that no test here comes close to the skiplist arena.
const BUDGET: usize = 4 * 1024 * 1024;

/// Runs `check` against every implementation, naming the one that failed.
fn both(check: impl Fn(&dyn Memtable)) {
    check(&BTreeMemtable::new());
    check(&SkipMemtable::new(BUDGET));
}

fn keys(table: &dyn Memtable) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    table
        .for_each(&mut |key, _, _| {
            keys.push(key.to_vec());
            Ok(())
        })
        .expect("visit");
    keys
}

#[test]
fn a_value_is_read_back() {
    both(|table| {
        assert!(table.insert(b"key", 1, Some(b"value".to_vec())));

        assert_eq!(table.get(b"key"), Lookup::Found(b"value".to_vec()));
        assert_eq!(table.get(b"other"), Lookup::Missing);
    });
}

#[test]
fn a_tombstone_shadows_the_value_it_replaces() {
    both(|table| {
        table.insert(b"key", 1, Some(b"value".to_vec()));
        table.insert(b"key", 2, None);

        assert_eq!(table.get(b"key"), Lookup::Deleted);
        assert_eq!(table.len(), 1, "a tombstone still occupies the key");
    });
}

#[test]
fn a_later_sequence_number_wins_whatever_the_arrival_order() {
    both(|table| {
        assert!(table.insert(b"key", 2, Some(b"newer".to_vec())));
        assert!(
            !table.insert(b"key", 1, Some(b"older".to_vec())),
            "an older mutation must not overwrite a newer one"
        );

        assert_eq!(table.get(b"key"), Lookup::Found(b"newer".to_vec()));
        assert_eq!(table.len(), 1);
    });
}

#[test]
fn a_flush_sees_keys_in_sorted_order() {
    both(|table| {
        for (seq, key) in [b"c", b"a", b"b"].into_iter().enumerate() {
            table.insert(key, seq as u64 + 1, Some(b"v".to_vec()));
        }

        assert_eq!(keys(table), [b"a", b"b", b"c"]);
    });
}

#[test]
fn a_flush_sees_one_entry_per_key_however_many_versions_were_written() {
    both(|table| {
        table.insert(b"a", 1, Some(b"first".to_vec()));
        table.insert(b"a", 2, Some(b"second".to_vec()));
        table.insert(b"a", 3, Some(b"third".to_vec()));
        table.insert(b"b", 4, Some(b"only".to_vec()));

        let mut seen = Vec::new();
        table
            .for_each(&mut |key, seq, value| {
                seen.push((key.to_vec(), seq, value.map(<[u8]>::to_vec)));
                Ok(())
            })
            .expect("visit");

        assert_eq!(
            seen,
            vec![
                (b"a".to_vec(), 3, Some(b"third".to_vec())),
                (b"b".to_vec(), 4, Some(b"only".to_vec())),
            ]
        );
        assert_eq!(table.len(), 2);
    });
}

#[test]
fn an_empty_table_holds_nothing() {
    both(|table| {
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert_eq!(table.approx_bytes(), 0);
        assert_eq!(keys(table), Vec::<Vec<u8>>::new());

        table.insert(b"key", 1, Some(b"value".to_vec()));
        assert!(!table.is_empty());
        assert!(table.approx_bytes() >= 8, "key and value bytes are counted");
    });
}

#[test]
fn an_empty_key_and_an_empty_value_are_held_like_any_other() {
    both(|table| {
        table.insert(b"", 1, Some(Vec::new()));
        table.insert(b"a", 2, Some(b"v".to_vec()));

        assert_eq!(table.get(b""), Lookup::Found(Vec::new()));
        assert_eq!(keys(table), [b"".to_vec(), b"a".to_vec()]);
    });
}

#[test]
fn many_keys_are_all_found_and_come_back_sorted() {
    const KEYS: u64 = 2000;

    both(|table| {
        // Scattered, so insertion order is nothing like key order.
        for i in 0..KEYS {
            let key = (i * 7919 % KEYS).to_be_bytes();
            table.insert(&key, i + 1, Some(key.to_vec()));
        }

        assert_eq!(table.len() as u64, KEYS);
        for i in 0..KEYS {
            let key = i.to_be_bytes();
            assert_eq!(table.get(&key), Lookup::Found(key.to_vec()), "key {i}");
        }

        let visited = keys(table);
        assert_eq!(visited.len() as u64, KEYS);
        assert!(visited.windows(2).all(|pair| pair[0] < pair[1]));
    });
}

#[test]
fn concurrent_writers_converge_on_the_highest_sequence_number() {
    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 500;

    both(|table| {
        thread::scope(|scope| {
            for writer in 0..WRITERS {
                scope.spawn(move || {
                    for i in 0..PER_WRITER {
                        // Sequence numbers are handed out by the log, so they
                        // are unique across writers.
                        let seq = i * WRITERS + writer + 1;
                        table.insert(b"contended", seq, Some(seq.to_be_bytes().to_vec()));
                    }
                });
            }
        });

        let highest = PER_WRITER * WRITERS;
        assert_eq!(
            table.get(b"contended"),
            Lookup::Found(highest.to_be_bytes().to_vec())
        );
        assert_eq!(table.len(), 1);
    });
}

#[test]
fn concurrent_writers_on_distinct_keys_all_survive() {
    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 500;

    both(|table| {
        thread::scope(|scope| {
            for writer in 0..WRITERS {
                scope.spawn(move || {
                    for i in 0..PER_WRITER {
                        let seq = i * WRITERS + writer + 1;
                        table.insert(&seq.to_be_bytes(), seq, Some(seq.to_be_bytes().to_vec()));
                    }
                });
            }
        });

        let total = WRITERS * PER_WRITER;
        assert_eq!(table.len() as u64, total);
        for seq in 1..=total {
            let key = seq.to_be_bytes();
            assert_eq!(table.get(&key), Lookup::Found(key.to_vec()), "key {seq}");
        }
        assert!(keys(table).windows(2).all(|pair| pair[0] < pair[1]));
    });
}

#[test]
fn readers_see_a_consistent_table_while_writers_run() {
    const WRITERS: u64 = 4;
    const READERS: usize = 4;
    const PER_WRITER: u64 = 1000;

    both(|table| {
        thread::scope(|scope| {
            for writer in 0..WRITERS {
                scope.spawn(move || {
                    for i in 0..PER_WRITER {
                        let seq = i * WRITERS + writer + 1;
                        table.insert(&seq.to_be_bytes(), seq, Some(seq.to_be_bytes().to_vec()));
                    }
                });
            }
            for _ in 0..READERS {
                scope.spawn(move || {
                    for seq in 1..=WRITERS * PER_WRITER {
                        let key = seq.to_be_bytes();
                        // A key is either not there yet or holds its own value.
                        // Anything else means a reader saw a half-built node.
                        match table.get(&key) {
                            Lookup::Missing => {}
                            Lookup::Found(value) => assert_eq!(value, key.to_vec()),
                            Lookup::Deleted => panic!("nothing here writes a tombstone"),
                        }
                    }
                });
            }
        });
    });
}
