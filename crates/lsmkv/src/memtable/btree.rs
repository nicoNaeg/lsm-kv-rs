//! The baseline: a `BTreeMap` behind one `RwLock`.
//!
//! Readers run concurrently, writers take turns, and a writer excludes every
//! reader for the length of its insert. That last property is what the
//! skiplist next door was written against.

use std::collections::BTreeMap;
use std::sync::RwLock;

use super::{Memtable, Visit};
use crate::error::Result;
use crate::lookup::Lookup;

/// A panic while the table was being modified leaves its size accounting
/// unusable.
const POISONED: &str = "a writer panicked while holding the memtable";

/// The most recent value known for each key, sorted by key.
#[derive(Debug, Default)]
pub struct BTreeMemtable {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    entries: BTreeMap<Vec<u8>, Entry>,
    bytes: usize,
}

#[derive(Debug)]
struct Entry {
    seq: u64,
    /// `None` is a tombstone.
    value: Option<Vec<u8>>,
}

impl Entry {
    fn value_len(&self) -> usize {
        self.value.as_ref().map_or(0, Vec::len)
    }
}

impl BTreeMemtable {
    /// An empty table.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Memtable for BTreeMemtable {
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the table.
    fn insert(&self, key: &[u8], seq: u64, value: Option<Vec<u8>>) -> bool {
        let mut inner = self.inner.write().expect(POISONED);
        let Inner { entries, bytes } = &mut *inner;

        if let Some(entry) = entries.get_mut(key) {
            if entry.seq >= seq {
                return false;
            }
            *bytes -= entry.value_len();
            *bytes += value.as_ref().map_or(0, Vec::len);
            *entry = Entry { seq, value };
            return true;
        }

        *bytes += key.len() + value.as_ref().map_or(0, Vec::len);
        entries.insert(key.to_vec(), Entry { seq, value });
        true
    }

    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the table.
    fn get(&self, key: &[u8]) -> Lookup {
        let inner = self.inner.read().expect(POISONED);
        match inner.entries.get(key) {
            Some(Entry {
                value: Some(value), ..
            }) => Lookup::Found(value.clone()),
            Some(Entry { value: None, .. }) => Lookup::Deleted,
            None => Lookup::Missing,
        }
    }

    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the table.
    fn len(&self) -> usize {
        self.inner.read().expect(POISONED).entries.len()
    }

    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the table.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The table is held for reading throughout, so other readers are
    /// unaffected.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the table.
    fn for_each(&self, visit: Visit<'_>) -> Result<()> {
        let inner = self.inner.read().expect(POISONED);
        for (key, entry) in &inner.entries {
            visit(key, entry.seq, entry.value.as_deref())?;
        }
        Ok(())
    }

    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the table.
    fn approx_bytes(&self) -> usize {
        self.inner.read().expect(POISONED).bytes
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn keys_are_held_in_sorted_order() {
        let table = BTreeMemtable::new();
        for (seq, key) in [b"c", b"a", b"b"].into_iter().enumerate() {
            table.insert(key, seq as u64 + 1, Some(b"v".to_vec()));
        }

        let inner = table.inner.read().expect("read");
        let keys: Vec<&[u8]> = inner.entries.keys().map(Vec::as_slice).collect();
        assert_eq!(keys, [b"a", b"b", b"c"]);
    }

    #[test]
    fn size_follows_what_the_table_actually_holds() {
        let table = BTreeMemtable::new();
        assert_eq!(table.approx_bytes(), 0);

        table.insert(b"key", 1, Some(vec![0; 100]));
        assert_eq!(table.approx_bytes(), 103);

        table.insert(b"key", 2, Some(vec![0; 10]));
        assert_eq!(table.approx_bytes(), 13, "the replaced value is released");

        table.insert(b"key", 3, None);
        assert_eq!(table.approx_bytes(), 3, "a tombstone keeps only its key");
    }
}
