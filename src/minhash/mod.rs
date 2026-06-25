//! R-MinHash implementation derived from Rensa (https://github.com/beowolx/rensa).
//!
//! Copyright (c) 2024 beowulf. Incorporated under the MIT License and modified
//! for `incrededup`; see THIRD_PARTY_NOTICES.md.
//!
//! `incrededup` removes the PyO3/Python surface, exposes Rust-native helpers,
//! and uses the Rensa-derived signatures inside a disk-backed deduplication
//! pipeline.

mod hasher;

pub use hasher::{calculate_band_hash, calculate_hash_fast, permute_hash};

use rand::prelude::*;
use rand::Rng;
use rand_xoshiro::Xoshiro256PlusPlus;
use serde::{Deserialize, Serialize};

const PERM_CHUNK_SIZE: usize = 16;

/// MinHash signature configuration
pub const NUM_PERM: usize = 128;
pub const NUM_BANDS: usize = 16;
pub const ROWS_PER_BAND: usize = NUM_PERM / NUM_BANDS; // 8

/// RMinHash implements the MinHash algorithm for efficient similarity estimation.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RMinHash {
    num_perm: usize,
    seed: u64,
    hash_values: Vec<u32>,
    permutations: Vec<(u64, u64)>,
}

impl RMinHash {
    /// Creates a new RMinHash instance.
    ///
    /// # Arguments
    /// * `num_perm` - The number of permutations to use (typically 128)
    /// * `seed` - A seed value for the random number generator
    #[must_use]
    pub fn new(num_perm: usize, seed: u64) -> Self {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(seed);
        let permutations: Vec<(u64, u64)> = (0..num_perm)
            .map(|_| {
                // Ensure odd multiplier for better distribution
                let a = rng.gen::<u64>() | 1;
                let b = rng.gen::<u64>();
                (a, b)
            })
            .collect();

        Self {
            num_perm,
            seed,
            hash_values: vec![u32::MAX; num_perm],
            permutations,
        }
    }

    /// Updates the MinHash with a new set of string items.
    pub fn update(&mut self, items: &[String]) {
        self.update_internal(items);
    }

    /// Updates the MinHash with a new set of byte slices (more efficient).
    pub fn update_bytes(&mut self, items: &[&[u8]]) {
        const BATCH_SIZE: usize = 32;
        let mut hash_batch = Vec::with_capacity(BATCH_SIZE);

        for chunk in items.chunks(BATCH_SIZE) {
            hash_batch.clear();

            // First pass: compute all hashes
            for item in chunk {
                hash_batch.push(calculate_hash_fast(item));
            }

            self.update_from_hashes(&hash_batch);
        }
    }

    fn update_internal(&mut self, items: &[String]) {
        const BATCH_SIZE: usize = 32;
        let mut hash_batch = Vec::with_capacity(BATCH_SIZE);

        for chunk in items.chunks(BATCH_SIZE) {
            hash_batch.clear();

            // First pass: compute all hashes
            for item in chunk {
                hash_batch.push(calculate_hash_fast(item.as_bytes()));
            }

            self.update_from_hashes(&hash_batch);
        }
    }

    fn update_from_hashes(&mut self, hash_batch: &[u64]) {
        // Process in chunks for better vectorization
        let perm_chunks_iter = self.permutations.chunks_exact(PERM_CHUNK_SIZE);
        let hash_chunks_iter = self.hash_values.chunks_exact_mut(PERM_CHUNK_SIZE);

        // Process complete chunks
        for (perm_chunk, hash_chunk) in perm_chunks_iter.zip(hash_chunks_iter) {
            let mut current = [0u32; PERM_CHUNK_SIZE];
            current.copy_from_slice(hash_chunk);

            for &item_hash in hash_batch {
                for i in 0..PERM_CHUNK_SIZE {
                    let (a, b) = perm_chunk[i];
                    let hash = permute_hash(item_hash, a, b);
                    current[i] = current[i].min(hash);
                }
            }

            hash_chunk.copy_from_slice(&current);
        }

        // Handle remainder
        let remainder_start = (self.num_perm / PERM_CHUNK_SIZE) * PERM_CHUNK_SIZE;
        if remainder_start < self.num_perm {
            let perm_remainder = &self.permutations[remainder_start..];
            let hash_remainder = &mut self.hash_values[remainder_start..];

            for &item_hash in hash_batch {
                for (i, &(a, b)) in perm_remainder.iter().enumerate() {
                    let hash = permute_hash(item_hash, a, b);
                    hash_remainder[i] = hash_remainder[i].min(hash);
                }
            }
        }
    }

    /// Returns the current MinHash digest (signature).
    #[must_use]
    pub fn digest(&self) -> &[u32] {
        &self.hash_values
    }

    /// Returns an owned copy of the digest.
    #[must_use]
    pub fn digest_owned(&self) -> Vec<u32> {
        self.hash_values.clone()
    }

    /// Calculates the Jaccard similarity between this MinHash and another.
    #[must_use]
    pub fn jaccard(&self, other: &Self) -> f64 {
        jaccard_from_signatures(&self.hash_values, &other.hash_values)
    }

    /// Returns the number of permutations.
    #[must_use]
    pub const fn num_perm(&self) -> usize {
        self.num_perm
    }

    /// Returns the seed used.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }
}

impl Default for RMinHash {
    fn default() -> Self {
        Self::new(NUM_PERM, 42)
    }
}

/// Compute Jaccard similarity from two signature slices.
///
/// Returns `0.0` for invalid input. Use `try_jaccard_from_signatures` when
/// callers need an explicit error for malformed signatures.
#[must_use]
pub fn jaccard_from_signatures(sig1: &[u32], sig2: &[u32]) -> f64 {
    try_jaccard_from_signatures(sig1, sig2).unwrap_or(0.0)
}

/// Checked Jaccard similarity from two signature slices.
pub fn try_jaccard_from_signatures(sig1: &[u32], sig2: &[u32]) -> anyhow::Result<f64> {
    if sig1.len() != sig2.len() {
        anyhow::bail!(
            "Signatures must have same length (left={}, right={})",
            sig1.len(),
            sig2.len()
        );
    }
    if sig1.is_empty() {
        anyhow::bail!("Signatures must not be empty");
    }

    let mut equal_count = 0usize;
    let num_perm = sig1.len();

    // Process in chunks of 8 for CPU-friendly operations
    let chunks_a = sig1.chunks_exact(8);
    let chunks_b = sig2.chunks_exact(8);

    for (chunk_a, chunk_b) in chunks_a.zip(chunks_b) {
        // Manual unrolling for better performance
        equal_count += usize::from(chunk_a[0] == chunk_b[0]);
        equal_count += usize::from(chunk_a[1] == chunk_b[1]);
        equal_count += usize::from(chunk_a[2] == chunk_b[2]);
        equal_count += usize::from(chunk_a[3] == chunk_b[3]);
        equal_count += usize::from(chunk_a[4] == chunk_b[4]);
        equal_count += usize::from(chunk_a[5] == chunk_b[5]);
        equal_count += usize::from(chunk_a[6] == chunk_b[6]);
        equal_count += usize::from(chunk_a[7] == chunk_b[7]);
    }

    // Handle remainder
    let remainder_start = (num_perm / 8) * 8;
    if remainder_start < num_perm {
        equal_count += sig1[remainder_start..]
            .iter()
            .zip(&sig2[remainder_start..])
            .filter(|&(&a, &b)| a == b)
            .count();
    }

    Ok(equal_count as f64 / num_perm as f64)
}

/// Compute band hashes for LSH from a signature.
///
/// Returns an empty vector for invalid lengths. Use
/// `try_compute_band_hashes` when callers need an explicit error.
#[must_use]
pub fn compute_band_hashes(signature: &[u32]) -> Vec<u64> {
    try_compute_band_hashes(signature).unwrap_or_default()
}

/// Checked band hash computation for LSH from a signature.
pub fn try_compute_band_hashes(signature: &[u32]) -> anyhow::Result<Vec<u64>> {
    let num_bands = NUM_BANDS;
    if signature.is_empty() || !signature.len().is_multiple_of(num_bands) {
        anyhow::bail!(
            "Signature length {} is not compatible with {} LSH bands",
            signature.len(),
            num_bands
        );
    }
    let rows_per_band = signature.len() / num_bands;

    Ok((0..num_bands)
        .map(|band| {
            let start = band * rows_per_band;
            let end = start + rows_per_band;
            calculate_band_hash(&signature[start..end])
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minhash_basic() {
        let mut mh1 = RMinHash::new(128, 42);
        let mut mh2 = RMinHash::new(128, 42);

        let items = vec!["hello".to_string(), "world".to_string()];
        mh1.update(&items);
        mh2.update(&items);

        // Same items should have Jaccard = 1.0
        assert!((mh1.jaccard(&mh2) - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_minhash_different() {
        let mut mh1 = RMinHash::new(128, 42);
        let mut mh2 = RMinHash::new(128, 42);

        mh1.update(&["hello".to_string(), "world".to_string()]);
        mh2.update(&["foo".to_string(), "bar".to_string()]);

        // Different items should have low Jaccard
        assert!(mh1.jaccard(&mh2) < 0.5);
    }

    #[test]
    fn test_band_hashes() {
        let mut mh = RMinHash::new(128, 42);
        mh.update(&["test".to_string()]);

        let bands = compute_band_hashes(mh.digest());
        assert_eq!(bands.len(), NUM_BANDS);
    }
}
