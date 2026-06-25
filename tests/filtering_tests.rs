//! Document filtering tests.
//!
//! These tests verify:
//! - Short document filtering (min_content_length)
//! - Size difference filtering (size_diff_threshold)
//! - Edge cases in filtering logic

mod common;

use common::{generate_long_document, generate_short_document, TestDocument};

/// Default minimum content length (matching main implementation)
const DEFAULT_MIN_CONTENT_LENGTH: i32 = 500;

/// Default size difference threshold (30%)
const DEFAULT_SIZE_DIFF_THRESHOLD: f64 = 0.3;

/// Check if a document should be filtered due to length
fn should_filter_short(doc: &TestDocument, min_length: i32) -> bool {
    doc.content_len <= min_length
}

/// Check if a document pair should be filtered due to size difference
fn should_filter_size_diff(size1: i32, size2: i32, threshold: f64) -> bool {
    let max_size = size1.max(size2) as f64;
    let diff = (size1 - size2).abs() as f64;
    (diff / max_size) > threshold
}

// ============================================================================
// Short Document Filtering Tests
// ============================================================================

#[test]
fn test_short_document_filtered() {
    let doc = generate_short_document();
    assert!(
        should_filter_short(&doc, DEFAULT_MIN_CONTENT_LENGTH),
        "Short documents should be filtered"
    );
}

#[test]
fn test_long_document_not_filtered() {
    let content = generate_long_document("Test content for a sufficiently long document.", 600);
    let doc = TestDocument::new(&content);

    assert!(
        !should_filter_short(&doc, DEFAULT_MIN_CONTENT_LENGTH),
        "Long documents should not be filtered"
    );
}

#[test]
fn test_boundary_length_document() {
    // Exactly at the threshold
    let content = "x".repeat(DEFAULT_MIN_CONTENT_LENGTH as usize);
    let doc = TestDocument::new(&content);

    assert!(
        should_filter_short(&doc, DEFAULT_MIN_CONTENT_LENGTH),
        "Documents exactly at threshold should be filtered (<=)"
    );

    // One character over threshold
    let content_plus_one = "x".repeat((DEFAULT_MIN_CONTENT_LENGTH + 1) as usize);
    let doc_plus_one = TestDocument::new(&content_plus_one);

    assert!(
        !should_filter_short(&doc_plus_one, DEFAULT_MIN_CONTENT_LENGTH),
        "Documents one over threshold should not be filtered"
    );
}

#[test]
fn test_empty_document_filtered() {
    let doc = TestDocument::new("");
    assert!(
        should_filter_short(&doc, DEFAULT_MIN_CONTENT_LENGTH),
        "Empty documents should be filtered"
    );
}

#[test]
fn test_whitespace_document_filtered() {
    let doc = TestDocument::new("   \n\t  \n  ");
    assert!(
        should_filter_short(&doc, DEFAULT_MIN_CONTENT_LENGTH),
        "Whitespace-only documents should be filtered"
    );
}

// ============================================================================
// Size Difference Filtering Tests
// ============================================================================

#[test]
fn test_same_size_not_filtered() {
    assert!(
        !should_filter_size_diff(1000, 1000, DEFAULT_SIZE_DIFF_THRESHOLD),
        "Same size documents should not be filtered"
    );
}

#[test]
fn test_small_difference_not_filtered() {
    // 10% difference (below 30% threshold)
    assert!(
        !should_filter_size_diff(1000, 900, DEFAULT_SIZE_DIFF_THRESHOLD),
        "10% difference should not be filtered"
    );
}

#[test]
fn test_large_difference_filtered() {
    // 50% difference (above 30% threshold)
    assert!(
        should_filter_size_diff(1000, 500, DEFAULT_SIZE_DIFF_THRESHOLD),
        "50% difference should be filtered"
    );
}

#[test]
fn test_boundary_difference() {
    // Exactly 30% difference
    let size1 = 1000;
    let size2 = 700; // 30% smaller

    // At exactly threshold, should not be filtered (> not >=)
    assert!(
        !should_filter_size_diff(size1, size2, DEFAULT_SIZE_DIFF_THRESHOLD),
        "Exactly at threshold should not be filtered"
    );

    // Just over 30%
    let size3 = 699;
    assert!(
        should_filter_size_diff(size1, size3, DEFAULT_SIZE_DIFF_THRESHOLD),
        "Just over threshold should be filtered"
    );
}

#[test]
fn test_size_diff_symmetric() {
    // Order shouldn't matter
    let result1 = should_filter_size_diff(1000, 500, DEFAULT_SIZE_DIFF_THRESHOLD);
    let result2 = should_filter_size_diff(500, 1000, DEFAULT_SIZE_DIFF_THRESHOLD);

    assert_eq!(
        result1, result2,
        "Size difference filtering should be symmetric"
    );
}

#[test]
fn test_size_diff_with_zero() {
    // Zero size document (edge case)
    // This would cause division by zero if not handled
    let result = should_filter_size_diff(1000, 0, DEFAULT_SIZE_DIFF_THRESHOLD);
    assert!(result, "Zero-size document should trigger filtering");

    // Both zero (edge case)
    // 0/0 = NaN, which should probably be filtered
    // But in practice this shouldn't happen since empty docs are filtered first
}

#[test]
fn test_strict_size_threshold() {
    // With a stricter threshold (10%), more pairs should be filtered
    let strict_threshold = 0.1;

    assert!(
        !should_filter_size_diff(1000, 950, strict_threshold),
        "5% diff should pass 10% threshold"
    );

    assert!(
        should_filter_size_diff(1000, 850, strict_threshold),
        "15% diff should fail 10% threshold"
    );
}

#[test]
fn test_lenient_size_threshold() {
    // With a lenient threshold (50%), fewer pairs should be filtered
    let lenient_threshold = 0.5;

    assert!(
        !should_filter_size_diff(1000, 600, lenient_threshold),
        "40% diff should pass 50% threshold"
    );

    assert!(
        should_filter_size_diff(1000, 400, lenient_threshold),
        "60% diff should fail 50% threshold"
    );
}

// ============================================================================
// Combined Filtering Tests
// ============================================================================

#[test]
fn test_filtering_logic_combined() {
    let short_doc = generate_short_document();
    let long_doc1 = TestDocument::new(&generate_long_document("Content A.", 600));
    let long_doc2 = TestDocument::new(&generate_long_document("Content B.", 600));
    let very_long_doc = TestDocument::new(&generate_long_document("Content C.", 2000));

    // Short doc should be filtered regardless of pairing
    assert!(should_filter_short(&short_doc, DEFAULT_MIN_CONTENT_LENGTH));

    // Two similar-sized long docs should not be filtered
    assert!(!should_filter_short(&long_doc1, DEFAULT_MIN_CONTENT_LENGTH));
    assert!(!should_filter_short(&long_doc2, DEFAULT_MIN_CONTENT_LENGTH));
    assert!(!should_filter_size_diff(
        long_doc1.content_len,
        long_doc2.content_len,
        DEFAULT_SIZE_DIFF_THRESHOLD
    ));

    // Long doc vs very long doc should be filtered by size diff
    assert!(should_filter_size_diff(
        long_doc1.content_len,
        very_long_doc.content_len,
        DEFAULT_SIZE_DIFF_THRESHOLD
    ));
}

// ============================================================================
// Realistic Scenario Tests
// ============================================================================

#[test]
fn test_realistic_document_batch() {
    let documents = [
        TestDocument::new("short"), // Filtered: too short
        TestDocument::new(&generate_long_document("Medium doc.", 600)), // OK
        TestDocument::new(&generate_long_document("Another medium.", 650)), // OK
        TestDocument::new(&generate_long_document("Large document.", 2000)), // Size diff with medium
        TestDocument::new(""),                                               // Filtered: empty
        TestDocument::new(&"x".repeat(501)), // OK: just over threshold
    ];

    let indexable: Vec<_> = documents
        .iter()
        .filter(|d| !should_filter_short(d, DEFAULT_MIN_CONTENT_LENGTH))
        .collect();

    assert_eq!(indexable.len(), 4, "Should have 4 indexable documents");

    // Check which pairs would pass size filtering
    let medium1 = &documents[1];
    let medium2 = &documents[2];
    let large = &documents[3];

    assert!(
        !should_filter_size_diff(
            medium1.content_len,
            medium2.content_len,
            DEFAULT_SIZE_DIFF_THRESHOLD
        ),
        "Two medium docs should be comparable"
    );

    assert!(
        should_filter_size_diff(
            medium1.content_len,
            large.content_len,
            DEFAULT_SIZE_DIFF_THRESHOLD
        ),
        "Medium vs large should be filtered"
    );
}
