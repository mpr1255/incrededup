//! Hash utility functions extracted from Rensa (https://github.com/beowolx/rensa)

use rustc_hash::FxHasher;
use std::hash::Hasher;

/// Fast hash function for byte arrays.
/// Uses a simplified FxHash variant for speed.
#[inline]
pub fn calculate_hash_fast(data: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;

    // Process 8 bytes at a time
    let chunks = data.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let val = u64::from_le_bytes(chunk.try_into().unwrap());
        hash = hash.wrapping_mul(0x0100_0000_01b3).wrapping_add(val);
    }

    // Handle remainder bytes
    for &byte in remainder {
        hash = hash
            .wrapping_mul(0x0100_0000_01b3)
            .wrapping_add(u64::from(byte));
    }

    hash
}

/// Applies a permutation to a hash value.
/// This is the core transformation for MinHash.
#[inline]
pub const fn permute_hash(hash: u64, a: u64, b: u64) -> u32 {
    ((a.wrapping_mul(hash).wrapping_add(b)) >> 32) as u32
}

/// Calculates a hash value for a band of MinHash values.
/// Used for LSH band hashing.
///
/// NOTE: Existing `lsh.redb` files are tied to this exact hash implementation.
/// If this changes, rebuild the sidecar index with `--fresh`.
#[inline]
pub fn calculate_band_hash(band: &[u32]) -> u64 {
    let mut hasher = FxHasher::default();

    // Process 4 u32s at a time for better throughput
    let chunks = band.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        // Process as two u64s for better performance
        let val1 = u64::from(chunk[0]) | (u64::from(chunk[1]) << 32);
        let val2 = u64::from(chunk[2]) | (u64::from(chunk[3]) << 32);
        hasher.write_u64(val1);
        hasher.write_u64(val2);
    }

    // Handle remainder
    for &value in remainder {
        hasher.write_u32(value);
    }

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_consistency() {
        let data = b"hello world";
        let h1 = calculate_hash_fast(data);
        let h2 = calculate_hash_fast(data);
        assert_eq!(h1, h2, "Hash should be deterministic");
    }

    #[test]
    fn test_hash_different() {
        let h1 = calculate_hash_fast(b"hello");
        let h2 = calculate_hash_fast(b"world");
        assert_ne!(h1, h2, "Different inputs should have different hashes");
    }

    #[test]
    fn test_permute_hash() {
        let hash = 12345u64;
        let a = 67890u64 | 1; // Ensure odd
        let b = 11111u64;
        let result = permute_hash(hash, a, b);
        assert_eq!(result, permute_hash(hash, a, b));
    }

    #[test]
    fn test_band_hash() {
        let band = vec![1u32, 2, 3, 4, 5, 6, 7, 8];
        let h1 = calculate_band_hash(&band);
        let h2 = calculate_band_hash(&band);
        assert_eq!(h1, h2, "Band hash should be deterministic");
    }
}
