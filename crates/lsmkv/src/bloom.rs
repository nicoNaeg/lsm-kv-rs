//! Bloom filter: the cheapest way to not read a file.
//!
//! One filter per sorted file, held in memory. It answers "definitely not in
//! this file", which lets a lookup skip the file without touching the disk, or
//! "maybe", which costs one block read. It can never miss a key that is there.
//!
//! Sizing is 10 bits per key with seven probes, the default of `LevelDB`: 0.82%
//! false positives for 1.1% of the file kept in memory.
//!
//! The probes come from one hash per key, split in two halves and combined as
//! `h1 + i * h2` (Kirsch and Mitzenmacher). Seven probes therefore cost one
//! hash, not seven.

/// Bits reserved per key.
pub const BITS_PER_KEY: usize = 10;

/// Smallest bit array, so a filter over very few keys is not degenerate.
const MIN_BITS: usize = 64;

/// A set membership test that answers with certainty only when it says no.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    /// The bit array, with the probe count in the last byte. Stored this way so
    /// writing the filter out needs no copy and reading it back needs no
    /// separate header.
    data: Vec<u8>,
}

impl Bloom {
    /// Builds a filter over key hashes obtained from [`hash`].
    ///
    /// Hashes rather than keys, because the caller already walks the keys once
    /// and has nothing to gain from a second pass.
    pub fn build(hashes: &[u64], bits_per_key: usize) -> Self {
        let probes = optimal_probes(bits_per_key);
        let bytes = (hashes.len() * bits_per_key).max(MIN_BITS).div_ceil(8);
        let mut data = vec![0u8; bytes + 1];
        let bit_len = bit_len(bytes);

        for &key_hash in hashes {
            let (mut position, step) = split(key_hash);
            for _ in 0..probes {
                let bit = (position % bit_len) as usize;
                data[bit / 8] |= 1 << (bit % 8);
                position = position.wrapping_add(step);
            }
        }

        data[bytes] = probes;
        Self { data }
    }

    /// Reads a filter back from the bytes [`Bloom::as_bytes`] produced.
    ///
    /// Returns `None` if they cannot describe a filter.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let (&probes, _) = bytes.split_last()?;
        if bytes.len() < 2 || probes == 0 {
            return None;
        }
        Some(Self {
            data: bytes.to_vec(),
        })
    }

    /// Whether `key` may be in the set. `false` is certain, `true` is not.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.may_contain_hash(hash(key))
    }

    /// Same, for a hash already computed by [`hash`].
    pub fn may_contain_hash(&self, key_hash: u64) -> bool {
        let bit_len = bit_len(self.data.len() - 1);
        let (mut position, step) = split(key_hash);
        for _ in 0..self.probes() {
            let bit = (position % bit_len) as usize;
            if self.data[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
            position = position.wrapping_add(step);
        }
        true
    }

    /// The filter as it is stored.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Probes each lookup performs.
    pub fn probes(&self) -> u8 {
        self.data[self.data.len() - 1]
    }

    /// Bits the array holds.
    pub fn bits(&self) -> usize {
        (self.data.len() - 1) * 8
    }
}

/// Probe count that minimizes false positives at this density, `ln 2` bits per
/// bit per key.
fn optimal_probes(bits_per_key: usize) -> u8 {
    // ln 2 scaled to five digits, so the rounding stays in integer arithmetic.
    let probes = (bits_per_key * 69_315 + 50_000) / 100_000;
    u8::try_from(probes.clamp(1, 30)).unwrap_or(30)
}

/// Splits one hash into the two the probe sequence walks with.
fn split(key_hash: u64) -> (u32, u32) {
    let bytes = key_hash.to_le_bytes();
    let first = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let second = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    // A step of zero would probe the same bit every time.
    (first, second | 1)
}

/// Bit count as the probe arithmetic sees it.
///
/// Capped at `u32::MAX`, which no filter this engine builds comes near: even a
/// gigabyte of keys needs under a hundred million bits.
fn bit_len(bytes: usize) -> u32 {
    u32::try_from(bytes.saturating_mul(8)).unwrap_or(u32::MAX)
}

/// Hashes a key for [`Bloom`].
///
/// FNV-1a over the bytes, finished with the splitmix64 mixer so both halves of
/// the result avalanche well enough to act as independent hashes. Hand-written
/// to keep the engine free of a hashing dependency.
pub fn hash(key: &[u8]) -> u64 {
    let mut state: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in key {
        state ^= u64::from(byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }

    state ^= state >> 30;
    state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state ^= state >> 27;
    state = state.wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hashes(prefix: &str, count: usize) -> Vec<u64> {
        (0..count)
            .map(|i| hash(format!("{prefix}{i}").as_bytes()))
            .collect()
    }

    #[test]
    fn the_probe_count_follows_the_density() {
        assert_eq!(optimal_probes(4), 3);
        assert_eq!(optimal_probes(8), 6);
        assert_eq!(optimal_probes(10), 7);
        assert_eq!(optimal_probes(12), 8);
        assert_eq!(optimal_probes(16), 11);
    }

    #[test]
    fn every_key_that_was_added_is_reported_present() {
        let keys: Vec<String> = (0..10_000).map(|i| format!("key:{i}")).collect();
        let filter = Bloom::build(&hashes("key:", 10_000), BITS_PER_KEY);

        for key in &keys {
            assert!(
                filter.may_contain(key.as_bytes()),
                "a filter must never miss a key it holds: {key}"
            );
        }
    }

    #[test]
    fn the_false_positive_rate_matches_the_theory() {
        const KEYS: usize = 10_000;
        const PROBES: usize = 200_000;

        let filter = Bloom::build(&hashes("key:", KEYS), BITS_PER_KEY);
        let positives = (0..PROBES)
            .filter(|i| filter.may_contain(format!("absent:{i}").as_bytes()))
            .count();

        // Theory for ten bits per key at seven probes is 82 per 10 000. The
        // key set is fixed, so this measurement is the same on every run.
        let per_10k = positives * 10_000 / PROBES;
        assert!(
            (40..130).contains(&per_10k),
            "measured {per_10k} per 10 000, expected about 82"
        );
    }

    #[test]
    fn a_denser_filter_rejects_more() {
        let sparse = Bloom::build(&hashes("key:", 5_000), 4);
        let dense = Bloom::build(&hashes("key:", 5_000), 16);

        let count = |filter: &Bloom| {
            (0..20_000)
                .filter(|i| filter.may_contain(format!("absent:{i}").as_bytes()))
                .count()
        };
        assert!(
            count(&dense) < count(&sparse) / 10,
            "16 bits per key must reject far more than 4"
        );
    }

    #[test]
    fn a_filter_survives_a_roundtrip() {
        let filter = Bloom::build(&hashes("key:", 100), BITS_PER_KEY);
        let decoded = Bloom::decode(filter.as_bytes()).expect("decode");

        assert_eq!(decoded, filter);
        assert_eq!(decoded.probes(), 7);
        assert!(decoded.may_contain(b"key:42"));
    }

    #[test]
    fn a_filter_over_nothing_rejects_everything() {
        let filter = Bloom::build(&[], BITS_PER_KEY);

        assert_eq!(filter.bits(), MIN_BITS);
        assert!(!filter.may_contain(b"anything"));
    }

    #[test]
    fn a_single_key_is_found_and_almost_nothing_else() {
        let filter = Bloom::build(&[hash(b"only")], BITS_PER_KEY);

        assert!(filter.may_contain(b"only"));
        assert!(!filter.may_contain(b"other"));
    }

    #[test]
    fn bytes_that_cannot_be_a_filter_are_rejected() {
        assert!(Bloom::decode(&[]).is_none());
        assert!(Bloom::decode(&[7]).is_none(), "no room for any bits");
        assert!(Bloom::decode(&[0xFF, 0]).is_none(), "zero probes");
    }

    #[test]
    fn the_hash_avalanches_across_similar_keys() {
        // Keys differing in one byte must not land on neighbouring bits, which
        // is what the mixer after FNV-1a is there for.
        let a = hash(b"key:1000");
        let b = hash(b"key:1001");
        assert!(
            (a ^ b).count_ones() > 20,
            "{} bits differ",
            (a ^ b).count_ones()
        );
    }
}
