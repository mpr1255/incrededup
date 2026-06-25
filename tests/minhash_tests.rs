//! MinHash signature tests.
//!
//! These tests verify:
//! - Determinism: same content → same signature
//! - Similarity: similar content → similar signatures (high Jaccard)
//! - Difference: different content → different signatures (low Jaccard)

mod common;

use common::{generate_different_pair, generate_duplicate_pair, generate_similar_pair};
use incrededup::minhash::{jaccard_from_signatures, RMinHash, NUM_PERM};

/// Tokenize content into 3-word shingles (matching the main implementation)
fn tokenize(content: &str) -> Vec<String> {
    let words: Vec<&str> = content.split_whitespace().filter(|w| w.len() > 1).collect();

    if words.len() < 3 {
        return words.iter().map(|s| s.to_string()).collect();
    }

    words.windows(3).map(|w| w.join(" ")).collect()
}

/// Compute MinHash signature for content
fn compute_signature(content: &str, seed: u64) -> Vec<u32> {
    let tokens = tokenize(content);
    let mut minhash = RMinHash::new(NUM_PERM, seed);
    minhash.update(&tokens);
    minhash.digest_owned()
}

#[test]
fn test_minhash_determinism() {
    // Same content should always produce the same signature
    let content = "The quick brown fox jumps over the lazy dog. \
                   This is additional content to make the document longer.";

    let sig1 = compute_signature(content, 42);
    let sig2 = compute_signature(content, 42);
    let sig3 = compute_signature(content, 42);

    assert_eq!(sig1, sig2, "Signatures should be deterministic");
    assert_eq!(sig2, sig3, "Signatures should be deterministic");
}

#[test]
fn test_minhash_different_seeds() {
    // Different seeds should produce different signatures
    let content = "The quick brown fox jumps over the lazy dog. \
                   This is additional content to make the document longer.";

    let sig1 = compute_signature(content, 42);
    let sig2 = compute_signature(content, 123);

    assert_ne!(
        sig1, sig2,
        "Different seeds should produce different signatures"
    );
}

#[test]
fn test_minhash_exact_duplicates() {
    // Exact duplicates should have Jaccard similarity of 1.0
    let (doc1, doc2) = generate_duplicate_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    let jaccard = jaccard_from_signatures(&sig1, &sig2);

    assert!(
        (jaccard - 1.0).abs() < f64::EPSILON,
        "Exact duplicates should have Jaccard = 1.0, got {}",
        jaccard
    );
}

#[test]
fn test_minhash_similar_documents() {
    // Similar documents should have high Jaccard (> 0.5)
    let (doc1, doc2) = generate_similar_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    let jaccard = jaccard_from_signatures(&sig1, &sig2);

    assert!(
        jaccard > 0.5,
        "Similar documents should have Jaccard > 0.5, got {}",
        jaccard
    );
}

#[test]
fn test_minhash_different_documents() {
    // Different documents should have low Jaccard (< 0.5)
    let (doc1, doc2) = generate_different_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    let jaccard = jaccard_from_signatures(&sig1, &sig2);

    assert!(
        jaccard < 0.5,
        "Different documents should have Jaccard < 0.5, got {}",
        jaccard
    );
}

#[test]
fn test_minhash_signature_length() {
    // Signatures should always have NUM_PERM elements
    let content = "Some test content for signature length verification.";
    let sig = compute_signature(content, 42);

    assert_eq!(
        sig.len(),
        NUM_PERM,
        "Signature should have {} elements",
        NUM_PERM
    );
}

#[test]
fn test_minhash_empty_content() {
    // Empty content should still produce a valid signature
    let sig = compute_signature("", 42);

    assert_eq!(
        sig.len(),
        NUM_PERM,
        "Empty content should still produce a signature"
    );
}

#[test]
fn test_minhash_short_content() {
    // Very short content (less than 3 words) should still work
    let sig = compute_signature("hello world", 42);

    assert_eq!(
        sig.len(),
        NUM_PERM,
        "Short content should still produce a signature"
    );
}

#[test]
fn test_jaccard_bounds() {
    // Jaccard should always be between 0 and 1
    let (doc1, _) = generate_duplicate_pair();
    let (doc2, _) = generate_different_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    let jaccard = jaccard_from_signatures(&sig1, &sig2);

    assert!(
        (0.0..=1.0).contains(&jaccard),
        "Jaccard should be in [0, 1], got {}",
        jaccard
    );
}
