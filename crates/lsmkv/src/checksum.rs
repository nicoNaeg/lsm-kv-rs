//! CRC-32 (IEEE 802.3), the checksum stored with every record and every block
//! on disk.
//!
//! Slice-by-eight: eight tables, one per byte position in a word, so a step
//! consumes eight bytes and the eight lookups have no dependency on each other.
//! A byte-at-a-time loop cannot start its next lookup before the current one
//! lands, and that serial chain is what made the checksum 94 % of a warm
//! lookup when it was measured.
//!
//! The polynomial is unchanged, so this produces exactly the checksums the
//! byte-at-a-time version produced and files written by either are readable by
//! the other. `crc32_matches_the_reference_implementation` is what holds that.
//!
//! The tables are built at compile time, so the crate keeps no static
//! initialization and no dependency for this.

const POLYNOMIAL: u32 = 0xEDB8_8320;
const INIT: u32 = 0xFFFF_FFFF;
/// Bytes consumed per step, and therefore the number of tables.
const STRIDE: usize = 8;

const TABLES: [[u32; 256]; STRIDE] = build_tables();

const fn build_tables() -> [[u32; 256]; STRIDE] {
    let mut tables = [[0u32; 256]; STRIDE];

    let mut byte: u32 = 0;
    while byte < 256 {
        let mut crc = byte;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLYNOMIAL
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][byte as usize] = crc;
        byte += 1;
    }

    // Each later table is the one before it carried forward by another byte, so
    // table `k` answers "what does this byte contribute from `k` positions back".
    let mut table = 1;
    while table < STRIDE {
        let mut byte = 0;
        while byte < 256 {
            let carried = tables[table - 1][byte];
            tables[table][byte] = (carried >> 8) ^ tables[0][(carried & 0xFF) as usize];
            byte += 1;
        }
        table += 1;
    }

    tables
}

const fn update(mut crc: u32, bytes: &[u8]) -> u32 {
    let mut i = 0;

    while i + STRIDE <= bytes.len() {
        // The first word carries the running checksum into the lookups; the
        // second only contributes its own bytes.
        let head = crc
            ^ (bytes[i] as u32)
            ^ ((bytes[i + 1] as u32) << 8)
            ^ ((bytes[i + 2] as u32) << 16)
            ^ ((bytes[i + 3] as u32) << 24);
        let tail = (bytes[i + 4] as u32)
            | ((bytes[i + 5] as u32) << 8)
            | ((bytes[i + 6] as u32) << 16)
            | ((bytes[i + 7] as u32) << 24);

        crc = TABLES[7][(head & 0xFF) as usize]
            ^ TABLES[6][((head >> 8) & 0xFF) as usize]
            ^ TABLES[5][((head >> 16) & 0xFF) as usize]
            ^ TABLES[4][((head >> 24) & 0xFF) as usize]
            ^ TABLES[3][(tail & 0xFF) as usize]
            ^ TABLES[2][((tail >> 8) & 0xFF) as usize]
            ^ TABLES[1][((tail >> 16) & 0xFF) as usize]
            ^ TABLES[0][((tail >> 24) & 0xFF) as usize];
        i += STRIDE;
    }

    while i < bytes.len() {
        let index = ((crc ^ bytes[i] as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLES[0][index];
        i += 1;
    }

    crc
}

/// CRC-32 of `bytes`.
pub(crate) const fn crc32(bytes: &[u8]) -> u32 {
    !update(INIT, bytes)
}

/// CRC-32 of two chunks read back separately, as a record header and its
/// payload are.
pub(crate) const fn crc32_parts(first: &[u8], second: &[u8]) -> u32 {
    !update(update(INIT, first), second)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The implementation this replaced, kept to prove the replacement produces
    /// the same checksums for every input rather than only for the published
    /// check value.
    fn crc32_bytewise(bytes: &[u8]) -> u32 {
        let mut crc = INIT;
        for &byte in bytes {
            let index = ((crc ^ u32::from(byte)) & 0xFF) as usize;
            crc = (crc >> 8) ^ TABLES[0][index];
        }
        !crc
    }

    #[test]
    fn matches_the_standard_check_value() {
        // Check value published with the CRC-32 specification.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn crc32_matches_the_reference_implementation() {
        // Every length across the stride boundary, so the eight-byte steps and
        // the tail that follows them are both covered, and the payload is not
        // uniform enough for a wrong table to agree by luck.
        let mut data = Vec::new();
        for i in 0..1024u32 {
            data.push((i.wrapping_mul(2_654_435_761) >> 13).to_le_bytes()[0]);
            assert_eq!(
                crc32(&data),
                crc32_bytewise(&data),
                "{} bytes disagree",
                data.len()
            );
        }
    }

    #[test]
    fn splitting_the_input_does_not_change_the_result() {
        let whole = b"the quick brown fox jumps over the lazy dog, twice over";
        for split in 0..whole.len() {
            assert_eq!(
                crc32_parts(&whole[..split], &whole[split..]),
                crc32(whole),
                "split at {split}"
            );
        }
    }

    #[test]
    fn one_flipped_bit_changes_the_checksum() {
        let mut corrupted = *b"123456789";
        corrupted[3] ^= 0b0000_0001;
        assert_ne!(crc32(&corrupted), crc32(b"123456789"));
    }
}
