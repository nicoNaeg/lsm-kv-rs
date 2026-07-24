//! The answer every level of the store gives to a lookup.

/// What a lookup found in one memtable or one file.
///
/// The three cases are what makes a read across levels a plain loop: a value
/// and a tombstone are both answers and stop the search, and only `Missing`
/// sends it to the next, older level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// The key is bound to this value.
    Found(Vec<u8>),
    /// A tombstone shadows the key, so older levels must not be consulted.
    Deleted,
    /// Nothing here; the search continues in older levels.
    Missing,
}
