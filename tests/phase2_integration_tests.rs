//! Phase 2 integration tests.
//!
//! These tests create a synthetic LSH index (lsh.redb), run Phase 2
//! deduplication on it, and verify the matches.redb output.
//!
//! This tests the core deduplication logic WITHOUT requiring a database.

mod common;

use common::{create_temp_dir, generate_long_document, TestDocument};
use incrededup::lsh::DiskLSH;
use incrededup::minhash::{RMinHash, NUM_PERM};
use incrededup::run_disk_dedupe;
use incrededup::storage::{MatchRecord, MatchStore};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Tokenize and compute signature (matching main implementation)
fn compute_signature(content: &str, seed: u64) -> Vec<u32> {
    let words: Vec<&str> = content.split_whitespace().filter(|w| w.len() > 1).collect();

    let tokens: Vec<String> = if words.len() < 3 {
        words.iter().map(|s| s.to_string()).collect()
    } else {
        words.windows(3).map(|w| w.join(" ")).collect()
    };

    let mut minhash = RMinHash::new(NUM_PERM, seed);
    minhash.update(&tokens);
    minhash.digest_owned()
}

/// Build a test LSH index with known documents and drop the handle so the real
/// disk Phase 2 can open the same redb file.
fn build_test_index(
    temp_dir: &tempfile::TempDir,
    documents: &[TestDocument],
    seed: u64,
) -> PathBuf {
    let lsh_path = temp_dir.path().join("lsh.redb");
    {
        let lsh = DiskLSH::open(&lsh_path).expect("Failed to create DiskLSH");
        for doc in documents {
            let sig = compute_signature(&doc.content, seed);
            lsh.insert(doc.id, sig, doc.content_len as usize)
                .expect("Failed to insert into LSH");
        }
    }

    lsh_path
}

/// Run the real disk Phase 2 and return the real matches.redb records.
fn run_phase2(
    temp_dir: &tempfile::TempDir,
    lsh_path: &Path,
    threshold: f64,
    size_diff_threshold: f64,
) -> Vec<MatchRecord> {
    run_disk_dedupe(
        lsh_path,
        temp_dir.path(),
        4,
        threshold,
        size_diff_threshold,
        true,
        None,
    )
    .expect("real disk Phase 2 should succeed");

    MatchStore::open(temp_dir.path().join("matches.redb"))
        .expect("Should open matches store")
        .get_all_real()
        .expect("Should read real matches")
}

fn run_phase2_for_docs(
    temp_dir: &tempfile::TempDir,
    lsh_path: &Path,
    threshold: f64,
    size_diff_threshold: f64,
    doc_ids: Vec<Uuid>,
) -> Vec<MatchRecord> {
    run_disk_dedupe(
        lsh_path,
        temp_dir.path(),
        4,
        threshold,
        size_diff_threshold,
        true,
        Some(doc_ids),
    )
    .expect("real disk Phase 2 should succeed");

    MatchStore::open(temp_dir.path().join("matches.redb"))
        .expect("Should open matches store")
        .get_all_real()
        .expect("Should read real matches")
}

#[test]
fn test_phase2_finds_exact_duplicates() {
    let temp_dir = create_temp_dir();

    // Create two exact duplicate documents
    let content = generate_long_document(
        "The quick brown fox jumps over the lazy dog. \
         This is a test document for deduplication.",
        600,
    );

    let doc1 = TestDocument::new(&content);
    let doc2 = TestDocument::new(&content);

    let documents = vec![doc1.clone(), doc2.clone()];
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    let matches = run_phase2(&temp_dir, &lsh_path, 0.8, 0.3);

    assert_eq!(matches.len(), 1, "Should find exactly one duplicate pair");

    let record = &matches[0];
    assert!(
        (record.child_id == doc1.id && record.parent_id == doc2.id)
            || (record.child_id == doc2.id && record.parent_id == doc1.id),
        "Match should be between the two documents"
    );
    assert!(
        (record.jaccard_similarity - 1.0).abs() < 0.01,
        "Exact duplicates should have Jaccard ≈ 1.0, got {}",
        record.jaccard_similarity
    );
}

#[test]
fn test_phase2_preserves_all_raw_edges_in_dense_cluster() {
    let temp_dir = create_temp_dir();

    let content = generate_long_document(
        "Dense duplicate cluster content where every document should match every other document.",
        600,
    );
    let documents: Vec<TestDocument> = (0..4).map(|_| TestDocument::new(&content)).collect();
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    let matches = run_phase2(&temp_dir, &lsh_path, 0.8, 0.3);

    assert_eq!(
        matches.len(),
        6,
        "Four identical documents should preserve all n*(n-1)/2 raw edges"
    );
    assert!(matches.iter().all(|m| m.child_id != m.parent_id));
    assert!(matches.iter().all(|m| m.jaccard_similarity >= 0.99));
}

#[test]
fn test_incremental_phase2_compares_new_doc_to_lower_uuid_historical_candidate() {
    let temp_dir = create_temp_dir();

    let content = generate_long_document(
        "Incremental ordering regression document with exact duplicate content.",
        600,
    );
    let historical = TestDocument::with_id(Uuid::from_u128(1), &content);
    let new_doc = TestDocument::with_id(Uuid::from_u128(2), &content);
    let documents = vec![historical.clone(), new_doc.clone()];
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    let matches = run_phase2_for_docs(&temp_dir, &lsh_path, 0.8, 0.3, vec![new_doc.id]);

    assert_eq!(
        matches.len(),
        1,
        "Incremental Phase 2 must compare a new doc against historical candidates even when the new UUID is larger"
    );
    assert_eq!(matches[0].child_id, new_doc.id);
    assert_eq!(matches[0].parent_id, historical.id);
}

#[test]
fn test_incremental_phase2_maintains_adjacency_after_backfill() {
    let temp_dir = create_temp_dir();

    let content = generate_long_document(
        "Production adjacency maintenance regression document with exact duplicate content.",
        600,
    );
    let historical_a = TestDocument::with_id(Uuid::from_u128(1), &content);
    let historical_b = TestDocument::with_id(Uuid::from_u128(2), &content);
    let new_doc = TestDocument::with_id(Uuid::from_u128(3), &content);

    let lsh_path = build_test_index(&temp_dir, &[historical_a.clone(), historical_b.clone()], 42);

    run_disk_dedupe(&lsh_path, temp_dir.path(), 4, 0.8, 0.3, true, None)
        .expect("initial real disk Phase 2 should succeed");

    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Should open matches store");
    store
        .build_adjacency_index()
        .expect("Should backfill adjacency index");
    assert!(store.is_adjacency_built().unwrap());
    drop(store);

    {
        let lsh = DiskLSH::open(&lsh_path).expect("Should reopen LSH index");
        let sig = compute_signature(&new_doc.content, 42);
        lsh.insert(new_doc.id, sig, new_doc.content_len as usize)
            .expect("Should insert new doc into LSH");
    }

    run_disk_dedupe(
        &lsh_path,
        temp_dir.path(),
        4,
        0.8,
        0.3,
        false,
        Some(vec![new_doc.id]),
    )
    .expect("incremental real disk Phase 2 should succeed");

    let store = MatchStore::open(&matches_path).expect("Should reopen matches store");
    let (indexed_edges, used_index) = store
        .get_real_edges_connected_to_auto(&[new_doc.id])
        .expect("Auto lookup should succeed");
    assert!(used_index, "adjacency index should be used after backfill");

    let full_scan_edges = store
        .get_real_edges_connected_to(&[new_doc.id])
        .expect("Full scan lookup should succeed");

    let to_edge_set = |records: Vec<MatchRecord>| -> HashSet<(Uuid, Uuid)> {
        records
            .into_iter()
            .map(|m| (m.child_id, m.parent_id))
            .collect()
    };

    assert_eq!(to_edge_set(indexed_edges), to_edge_set(full_scan_edges));
}

#[test]
fn test_phase2_finds_similar_documents() {
    let temp_dir = create_temp_dir();

    // Make documents very similar (only small changes) to ensure LSH catches them
    let base = "The quick brown fox jumps over the lazy dog in the forest clearing. \
                This is a test document with enough content for meaningful testing. \
                We need sufficient text to generate proper MinHash signatures for deduplication. \
                The algorithm works by hashing shingles and finding similar documents.";

    // Only change a few words - documents should be ~90%+ similar
    let modified = "The quick brown fox jumps over the lazy dog in the forest clearing. \
                    This is a test document with enough content for meaningful testing. \
                    We need sufficient text to generate proper MinHash signatures for deduplication. \
                    The algorithm works by hashing shingles and finding duplicate documents.";

    let doc1 = TestDocument::new(&generate_long_document(base, 600));
    let doc2 = TestDocument::new(&generate_long_document(modified, 600));

    let documents = vec![doc1.clone(), doc2.clone()];
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Use a lower threshold to account for LSH probabilistic nature
    let matches = run_phase2(&temp_dir, &lsh_path, 0.6, 0.3);

    // Note: LSH is probabilistic, so very similar docs might occasionally not match
    // This test verifies the basic functionality works
    assert!(
        !matches.is_empty(),
        "Should find highly similar documents as duplicates (if LSH bands matched)"
    );
}

#[test]
fn test_phase2_ignores_different_documents() {
    let temp_dir = create_temp_dir();

    let content1 = generate_long_document(
        "The quick brown fox jumps over the lazy dog. \
         Foxes are cunning animals that live in forests.",
        600,
    );

    let content2 = generate_long_document(
        "Machine learning algorithms process data patterns. \
         Neural networks have revolutionized artificial intelligence.",
        600,
    );

    let doc1 = TestDocument::new(&content1);
    let doc2 = TestDocument::new(&content2);

    let documents = vec![doc1, doc2];
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    let matches = run_phase2(&temp_dir, &lsh_path, 0.8, 0.3);

    assert!(
        matches.is_empty(),
        "Should not find unrelated documents as duplicates"
    );
}

#[test]
fn test_phase2_size_filtering() {
    let temp_dir = create_temp_dir();

    // Same content but one is much longer (padded)
    let short_content = generate_long_document("The quick brown fox.", 600);
    let long_content = generate_long_document("The quick brown fox.", 2000);

    let doc1 = TestDocument::new(&short_content);
    let doc2 = TestDocument::new(&long_content);

    let documents = vec![doc1, doc2];
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // With strict size threshold, should not match
    let matches = run_phase2(&temp_dir, &lsh_path, 0.8, 0.2);

    assert!(
        matches.is_empty(),
        "Size filter should exclude documents with very different lengths"
    );
}

#[test]
fn test_phase2_multiple_duplicate_groups() {
    let temp_dir = create_temp_dir();

    // Group 1: Two duplicates
    let content1 = generate_long_document("Document about cats and dogs and pets.", 600);
    let group1_doc1 = TestDocument::new(&content1);
    let group1_doc2 = TestDocument::new(&content1);

    // Group 2: Two different duplicates
    let content2 = generate_long_document("Machine learning and neural networks.", 600);
    let group2_doc1 = TestDocument::new(&content2);
    let group2_doc2 = TestDocument::new(&content2);

    // Unique document
    let content3 = generate_long_document("Completely unique content about gardening.", 600);
    let unique_doc = TestDocument::new(&content3);

    let documents = vec![
        group1_doc1.clone(),
        group1_doc2.clone(),
        group2_doc1.clone(),
        group2_doc2.clone(),
        unique_doc.clone(),
    ];

    let lsh_path = build_test_index(&temp_dir, &documents, 42);
    let matches = run_phase2(&temp_dir, &lsh_path, 0.8, 0.3);

    assert_eq!(
        matches.len(),
        2,
        "Should find exactly two duplicate pairs (one per group)"
    );

    // Verify unique doc is not matched
    let matched_ids: HashSet<Uuid> = matches
        .iter()
        .flat_map(|m| vec![m.child_id, m.parent_id])
        .collect();

    assert!(
        !matched_ids.contains(&unique_doc.id),
        "Unique document should not be matched"
    );
}

#[test]
fn test_phase2_with_match_store() {
    let temp_dir = create_temp_dir();

    // Create duplicate documents
    let content = generate_long_document("The quick brown fox jumps over lazy dog.", 600);
    let doc1 = TestDocument::new(&content);
    let doc2 = TestDocument::new(&content);

    let documents = vec![doc1.clone(), doc2.clone()];
    let _lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Create MatchStore and write matches
    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Failed to create MatchStore");

    let (child_id, parent_id) = if doc1.id < doc2.id {
        (doc2.id, doc1.id)
    } else {
        (doc1.id, doc2.id)
    };

    let match_record = MatchRecord {
        child_id,
        parent_id,
        jaccard_similarity: 1.0,
        size_difference: 0,
        size_difference_pct: 0.0,
    };

    store.insert(&match_record).expect("Failed to insert match");

    // Verify match can be retrieved
    let retrieved = store
        .get(&child_id)
        .expect("Failed to get match")
        .expect("Match should exist");

    assert_eq!(retrieved.parent_id, parent_id);
    assert!((retrieved.jaccard_similarity - 1.0).abs() < f64::EPSILON);
}

#[test]
fn test_phase2_threshold_sensitivity() {
    let temp_dir = create_temp_dir();

    let base = "The quick brown fox jumps over the lazy dog in the forest. \
                This document is about animals and nature and wildlife.";

    let modified = "The quick brown fox jumps over the lazy dog in the woods. \
                    This document is about animals and nature and creatures.";

    let doc1 = TestDocument::new(&generate_long_document(base, 600));
    let doc2 = TestDocument::new(&generate_long_document(modified, 600));

    let documents = vec![doc1, doc2];
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // With high threshold, may not match
    let matches_high = run_phase2(&temp_dir, &lsh_path, 0.95, 0.3);

    // With low threshold, should match
    let matches_low = run_phase2(&temp_dir, &lsh_path, 0.5, 0.3);

    assert!(
        matches_low.len() >= matches_high.len(),
        "Lower threshold should find at least as many matches"
    );
}

#[test]
fn test_phase2_determinism() {
    let temp_dir = create_temp_dir();

    let content = generate_long_document("Test content for determinism check.", 600);
    let doc1 = TestDocument::new(&content);
    let doc2 = TestDocument::new(&content);

    let documents = vec![doc1, doc2];

    // Run multiple times
    let lsh_path1 = build_test_index(&temp_dir, &documents, 42);
    let matches1 = run_phase2(&temp_dir, &lsh_path1, 0.8, 0.3);

    // Create a new temp dir for second run
    let temp_dir2 = create_temp_dir();
    let lsh_path2 = build_test_index(&temp_dir2, &documents, 42);
    let matches2 = run_phase2(&temp_dir2, &lsh_path2, 0.8, 0.3);

    assert_eq!(
        matches1.len(),
        matches2.len(),
        "Results should be deterministic"
    );
}
