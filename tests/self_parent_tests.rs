//! Tests for Phase 2 match records and legacy self-parent handling.
//!
//! # Formal Specification: Phase 2 and Sync Invariants
//!
//! ## Invariant 1: Raw Edge Preservation
//! **After Phase 2 completes, matches.redb stores real duplicate edges only.**
//! Unique docs are acknowledged in Phase 2 state and are marked as parents by
//! Phase 3's `new_doc_ids - child_ids` safety net.
//!
//! ## Invariant 2: Sync State Derivation
//! **The sync correctly derives is_parent state from raw duplicate edges plus the current batch:**
//! - `is_parent = true`: doc is in the current batch and not in `child_ids`
//! - `is_parent = false`: doc is in `child_ids` (has a parent in dupes table)
//! - `is_parent = NULL`: doc was never processed
//!
//! ## Invariant 3: Legacy Self-Parent Handling
//! **resolve_transitivity correctly handles self-parents:**
//! - Self-parent `(A, A)` contributes A to `parent_ids`
//! - Self-parents are NOT added to `child_ids` or `assignments` (dupes table)

mod common;

use common::{create_temp_dir, generate_long_document, TestDocument};
use incrededup::{resolve_transitivity, run_disk_dedupe, DiskLSH, MatchStore, RMinHash, NUM_PERM};
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

/// Build a test LSH index with known documents
/// Returns the path to the LSH file (drops the DiskLSH to release the lock)
fn build_test_index(
    temp_dir: &tempfile::TempDir,
    documents: &[TestDocument],
    seed: u64,
) -> std::path::PathBuf {
    let lsh_path = temp_dir.path().join("lsh.redb");
    {
        let lsh = DiskLSH::open(&lsh_path).expect("Failed to create DiskLSH");

        for doc in documents {
            let sig = compute_signature(&doc.content, seed);
            lsh.insert(doc.id, sig, doc.content_len as usize)
                .expect("Failed to insert into LSH");
        }
        // lsh is dropped here, releasing the lock
    }
    lsh_path
}

// ============================================================
// Match Record Tests
// ============================================================

#[test]
fn test_unique_doc_does_not_bloat_matches_store() {
    // Unique documents should not create self-parent placeholder records.
    let temp_dir = create_temp_dir();

    // Create two completely different documents (no matches)
    let content1 = generate_long_document(
        "The quick brown fox jumps over the lazy dog in the forest clearing.",
        600,
    );
    let content2 = generate_long_document(
        "Machine learning algorithms process vast amounts of data for pattern recognition.",
        600,
    );

    let doc1 = TestDocument::new(&content1);
    let doc2 = TestDocument::new(&content2);
    let documents = vec![doc1.clone(), doc2.clone()];

    // Build index (returns path, lsh is dropped to release lock)
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Run disk-based Phase 2
    let stats = run_disk_dedupe(
        &lsh_path,
        temp_dir.path(),
        4,    // workers
        0.8,  // threshold
        0.3,  // size_diff
        true, // fresh
        None, // process all docs
    )
    .expect("Phase 2 should succeed");

    // Should find no duplicates
    assert_eq!(
        stats.duplicates_found, 0,
        "Should find no duplicates for different documents"
    );

    // matches.redb should contain only real duplicate edges.
    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Should open matches store");
    assert_eq!(store.count().expect("Should count matches"), 0);
    assert!(store.get(&doc1.id).expect("Should get").is_none());
    assert!(store.get(&doc2.id).expect("Should get").is_none());
}

#[test]
fn test_duplicate_docs_get_real_match_not_self_parent() {
    // Test that duplicate documents get a real match, not a self-parent
    let temp_dir = create_temp_dir();

    // Create two exact duplicate documents
    let content = generate_long_document(
        "The quick brown fox jumps over the lazy dog in the forest clearing. \
         This is a test document for deduplication testing purposes.",
        600,
    );

    let doc1 = TestDocument::new(&content);
    let doc2 = TestDocument::new(&content);
    let documents = vec![doc1.clone(), doc2.clone()];

    // Build index (returns path, lsh is dropped to release lock)
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Run disk-based Phase 2
    let stats = run_disk_dedupe(
        &lsh_path,
        temp_dir.path(),
        4,    // workers
        0.8,  // threshold
        0.3,  // size_diff
        true, // fresh
        None, // process all docs
    )
    .expect("Phase 2 should succeed");

    // Should find one duplicate
    assert_eq!(stats.duplicates_found, 1, "Should find one duplicate pair");

    // matches.redb should have the child pointing to a different parent
    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Should open matches store");

    // One doc should be the child (pointing to the other). The root has no
    // self-parent placeholder record.
    let record1 = store.get(&doc1.id).expect("Should be able to get record");
    let record2 = store.get(&doc2.id).expect("Should be able to get record");

    // At least one should have a real match (pointing to the other)
    let has_real_match = match (&record1, &record2) {
        (Some(r1), Some(r2)) => {
            (r1.parent_id != doc1.id && r1.jaccard_similarity > 0.8)
                || (r2.parent_id != doc2.id && r2.jaccard_similarity > 0.8)
        }
        (Some(r1), None) => r1.parent_id != doc1.id && r1.jaccard_similarity > 0.8,
        (None, Some(r2)) => r2.parent_id != doc2.id && r2.jaccard_similarity > 0.8,
        _ => false,
    };

    assert!(
        has_real_match,
        "At least one doc should have a real match (not self-parent)"
    );
}

#[test]
fn test_unique_single_doc_writes_no_match_record() {
    let temp_dir = create_temp_dir();

    // Create a unique document
    let content = generate_long_document(
        "Completely unique content about quantum physics and string theory.",
        600,
    );

    let doc = TestDocument::new(&content);
    let documents = vec![doc.clone()];

    // Build index (returns path, lsh is dropped to release lock)
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Run disk-based Phase 2
    run_disk_dedupe(&lsh_path, temp_dir.path(), 4, 0.8, 0.3, true, None)
        .expect("Phase 2 should succeed");

    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Should open matches store");
    assert!(store.get(&doc.id).expect("Should get record").is_none());
    assert_eq!(store.count().expect("Should count matches"), 0);
}

#[test]
fn test_real_matches_are_preserved_without_self_parent_race() {
    let temp_dir = create_temp_dir();

    // Create duplicate documents with specific UUIDs to control ordering
    let content = generate_long_document(
        "The quick brown fox jumps over the lazy dog in the forest clearing. \
         This is a test document for deduplication testing purposes.",
        600,
    );

    // Create many pairs to ensure we hit the race condition at least once
    let mut documents = Vec::new();
    for _ in 0..10 {
        let doc1 = TestDocument::new(&content);
        let doc2 = TestDocument::new(&content);
        documents.push(doc1);
        documents.push(doc2);
    }

    // Build index (returns path, lsh is dropped to release lock)
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Run disk-based Phase 2 with multiple workers to increase race likelihood
    let stats = run_disk_dedupe(
        &lsh_path,
        temp_dir.path(),
        8, // more workers for more concurrency
        0.8,
        0.3,
        true,
        None,
    )
    .expect("Phase 2 should succeed");

    // All 20 docs have same content, so they all match each other
    // This creates 20*19/2 = 190 potential pairs. Raw edges are preserved.
    assert!(
        stats.duplicates_found > 0,
        "Should find some duplicate pairs"
    );

    // Check matches.redb - only real edges should be present.
    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Should open matches store");

    let all_matches = store.iter().expect("Should iterate matches");

    for record in &all_matches {
        assert_ne!(
            record.child_id, record.parent_id,
            "No self-parent records should be written"
        );
        assert!(
            record.jaccard_similarity > 0.8,
            "Real match should have jaccard > 0.8, got {}",
            record.jaccard_similarity
        );
    }
}

#[test]
fn test_mixed_dataset_writes_only_duplicate_edges() {
    // Test a mixed dataset with some duplicates and some unique docs
    let temp_dir = create_temp_dir();

    // Group 1: duplicate pair
    let dup_content = generate_long_document(
        "Document about cats and dogs and various pet animals in the home.",
        600,
    );
    let dup1 = TestDocument::new(&dup_content);
    let dup2 = TestDocument::new(&dup_content);

    // Group 2: another duplicate pair
    let dup_content2 = generate_long_document(
        "Machine learning and neural networks for image classification tasks.",
        600,
    );
    let dup3 = TestDocument::new(&dup_content2);
    let dup4 = TestDocument::new(&dup_content2);

    // Unique documents
    let unique1 = TestDocument::new(&generate_long_document(
        "Gardening tips for growing tomatoes in small urban spaces and balconies.",
        600,
    ));
    let unique2 = TestDocument::new(&generate_long_document(
        "History of ancient Rome and the fall of the Roman Empire in 476 AD.",
        600,
    ));
    let unique3 = TestDocument::new(&generate_long_document(
        "Cooking recipes for Mediterranean cuisine including Greek and Italian dishes.",
        600,
    ));

    let documents = vec![
        dup1.clone(),
        dup2.clone(),
        dup3.clone(),
        dup4.clone(),
        unique1.clone(),
        unique2.clone(),
        unique3.clone(),
    ];

    // Build index (returns path, lsh is dropped to release lock)
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Run disk-based Phase 2
    let stats = run_disk_dedupe(&lsh_path, temp_dir.path(), 4, 0.8, 0.3, true, None)
        .expect("Phase 2 should succeed");

    // Should find 2 duplicate pairs
    assert_eq!(stats.duplicates_found, 2, "Should find 2 duplicate pairs");

    // Check matches.redb
    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Should open matches store");

    let count = store.count().expect("Should count matches");
    assert_eq!(count, 2, "Only the 2 duplicate edges should be stored");

    // Unique docs should not have placeholder records.
    for unique in &[&unique1, &unique2, &unique3] {
        assert!(
            store.get(&unique.id).expect("Should get").is_none(),
            "Unique doc {} should not have a match record",
            unique.id
        );
    }
}

// ============================================================
// Sync / Transitivity Tests for Self-Parents
// ============================================================

#[test]
fn test_resolve_transitivity_handles_self_parents() {
    // Test that resolve_transitivity correctly handles self-parent records
    use incrededup::storage::MatchRecord;

    // Create UUIDs with known ordering to avoid flakiness
    // Union-Find picks lexicographically smallest as root, so parent < child
    let parent = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let child = Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
    let unique1 = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let unique2 = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();

    let matches = vec![
        // Real match: child -> parent
        MatchRecord {
            child_id: child,
            parent_id: parent,
            jaccard_similarity: 0.85,
            size_difference: 100,
            size_difference_pct: 0.05,
        },
        // Self-parent for parent (root of cluster)
        MatchRecord {
            child_id: parent,
            parent_id: parent,
            jaccard_similarity: 0.0,
            size_difference: 0,
            size_difference_pct: 0.0,
        },
        // Self-parent for unique doc
        MatchRecord {
            child_id: unique1,
            parent_id: unique1,
            jaccard_similarity: 0.0,
            size_difference: 0,
            size_difference_pct: 0.0,
        },
        // Another self-parent
        MatchRecord {
            child_id: unique2,
            parent_id: unique2,
            jaccard_similarity: 0.0,
            size_difference: 0,
            size_difference_pct: 0.0,
        },
    ];

    let (resolved, parent_ids, child_ids) = resolve_transitivity(&matches);

    // Should have 1 resolved match (child -> parent)
    // Note: parent is the root because it's lexicographically smallest
    assert_eq!(resolved.len(), 1, "Should have 1 resolved match");
    assert_eq!(resolved[0].child_id, child, "child should be child_id");
    assert_eq!(resolved[0].parent_id, parent, "parent should be parent_id");

    // parent_ids should include: parent, unique1, unique2
    assert!(
        parent_ids.contains(&parent),
        "parent should be in parent_ids"
    );
    assert!(
        parent_ids.contains(&unique1),
        "unique1 should be in parent_ids"
    );
    assert!(
        parent_ids.contains(&unique2),
        "unique2 should be in parent_ids"
    );

    // child_ids should only include: child
    assert_eq!(child_ids.len(), 1, "Should have 1 child");
    assert!(child_ids.contains(&child), "child should be in child_ids");

    // Self-parents should NOT be in child_ids
    assert!(
        !child_ids.contains(&parent),
        "parent should NOT be in child_ids"
    );
    assert!(
        !child_ids.contains(&unique1),
        "unique1 should NOT be in child_ids"
    );
    assert!(
        !child_ids.contains(&unique2),
        "unique2 should NOT be in child_ids"
    );
}

#[test]
fn test_matches_store_contains_only_real_duplicate_edges() {
    let temp_dir = create_temp_dir();

    // Create a mix of documents
    let mut documents = Vec::new();

    // Add 5 unique documents
    for i in 0..5 {
        let content = generate_long_document(
            &format!(
                "Unique document number {} with completely distinct content.",
                i
            ),
            600,
        );
        documents.push(TestDocument::new(&content));
    }

    // Add 2 duplicate pairs
    let dup_content = generate_long_document("Duplicate content for testing.", 600);
    documents.push(TestDocument::new(&dup_content));
    documents.push(TestDocument::new(&dup_content));

    // Build index (returns path, lsh is dropped to release lock)
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // Run disk-based Phase 2
    run_disk_dedupe(&lsh_path, temp_dir.path(), 4, 0.8, 0.3, true, None)
        .expect("Phase 2 should succeed");

    // Check matches.redb - only real duplicate edges should be present.
    let matches_path = temp_dir.path().join("matches.redb");
    let store = MatchStore::open(&matches_path).expect("Should open matches store");

    let all_matches = store.iter().expect("Should iterate matches");
    assert!(
        !all_matches.is_empty(),
        "At least one duplicate edge should be stored"
    );
    for record in &all_matches {
        assert_ne!(record.child_id, record.parent_id);
        assert!(record.jaccard_similarity >= 0.8);
    }
}

#[test]
fn test_incremental_run_preserves_empty_unique_match_store() {
    let temp_dir = create_temp_dir();

    // Create unique documents
    let doc1 = TestDocument::new(&generate_long_document("First unique document.", 600));
    let doc2 = TestDocument::new(&generate_long_document("Second unique document.", 600));
    let doc3 = TestDocument::new(&generate_long_document("Third unique document.", 600));

    let documents = vec![doc1.clone(), doc2.clone(), doc3.clone()];

    // Build index (returns path, lsh is dropped to release lock)
    let lsh_path = build_test_index(&temp_dir, &documents, 42);

    // First run
    run_disk_dedupe(
        &lsh_path,
        temp_dir.path(),
        4,
        0.8,
        0.3,
        true, // fresh
        None,
    )
    .expect("First Phase 2 should succeed");

    // Verify unique docs did not create placeholder rows.
    let matches_path = temp_dir.path().join("matches.redb");
    {
        let store = MatchStore::open(&matches_path).expect("Should open matches store");
        assert_eq!(store.count().expect("Should count"), 0);
        // store is dropped here, releasing the lock
    }

    // Second run (resume mode - not fresh)
    // Should not corrupt existing records
    run_disk_dedupe(
        &lsh_path,
        temp_dir.path(),
        4,
        0.8,
        0.3,
        false, // resume
        None,
    )
    .expect("Second Phase 2 should succeed");

    // Verify resume did not add placeholder rows.
    let store = MatchStore::open(&matches_path).expect("Should open matches store");
    assert_eq!(store.count().expect("Should count"), 0);
}
