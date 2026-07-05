//! LSH (Locality-Sensitive Hashing) index tests.
//!
//! These tests verify:
//! - Insert and query operations
//! - Similar documents are found as candidates
//! - Different documents are not returned as candidates
//! - Disk-backed LSH persistence

mod common;

use common::{
    create_temp_dir, generate_different_pair, generate_duplicate_pair, generate_similar_pair,
    generate_unique_documents,
};
use incrededup::compute_signature;
use incrededup::lsh::{DiskLSH, InMemoryLSH};

// ============================================================================
// In-Memory LSH Tests
// ============================================================================

#[test]
fn test_inmemory_lsh_insert_and_query() {
    let mut lsh = InMemoryLSH::new();
    let (doc1, doc2) = generate_duplicate_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    // Insert first document (InMemoryLSH takes owned signature, no size)
    lsh.insert(doc1.id, sig1.clone());

    // Query with second document (duplicate) - should find first
    let candidates = lsh.query(&sig2);

    assert!(
        candidates.contains(&doc1.id),
        "LSH should return duplicate as candidate"
    );
}

#[test]
fn test_inmemory_lsh_similar_documents() {
    let mut lsh = InMemoryLSH::new();
    let (doc1, doc2) = generate_similar_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    lsh.insert(doc1.id, sig1);

    let candidates = lsh.query(&sig2);

    // Similar documents should usually be found (high probability with 16 bands)
    assert!(
        candidates.contains(&doc1.id),
        "LSH should usually return similar doc as candidate"
    );
}

#[test]
fn test_inmemory_lsh_different_documents() {
    let mut lsh = InMemoryLSH::new();
    let (doc1, doc2) = generate_different_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    lsh.insert(doc1.id, sig1);

    let candidates = lsh.query(&sig2);

    // Different documents should rarely be found as candidates
    assert!(
        !candidates.contains(&doc1.id),
        "LSH should not return very different doc as candidate"
    );
}

#[test]
fn test_inmemory_lsh_multiple_documents() {
    let mut lsh = InMemoryLSH::new();
    let docs = generate_unique_documents(10);

    // Insert all documents
    for doc in &docs {
        let sig = compute_signature(&doc.content, 42);
        lsh.insert(doc.id, sig);
    }

    // Query with signature of first document
    let sig0 = compute_signature(&docs[0].content, 42);
    let candidates = lsh.query(&sig0);

    assert!(
        candidates.contains(&docs[0].id),
        "LSH should find the original document"
    );
}

#[test]
fn test_inmemory_lsh_document_count() {
    let mut lsh = InMemoryLSH::new();
    let docs = generate_unique_documents(5);

    for doc in &docs {
        let sig = compute_signature(&doc.content, 42);
        lsh.insert(doc.id, sig);
    }

    assert_eq!(lsh.len(), 5, "LSH should track document count");
}

// ============================================================================
// Disk-Backed LSH Tests
// ============================================================================

#[test]
fn test_disk_lsh_insert_and_query() {
    let temp_dir = create_temp_dir();
    let lsh_path = temp_dir.path().join("test_lsh.redb");

    let lsh = DiskLSH::open(&lsh_path).expect("Failed to create DiskLSH");
    let (doc1, doc2) = generate_duplicate_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    let sig2 = compute_signature(&doc2.content, 42);

    // Insert first document (DiskLSH takes usize for content_len)
    lsh.insert(doc1.id, sig1.clone(), doc1.content_len as usize)
        .expect("Failed to insert");

    // Query with second document (duplicate)
    let candidates = lsh.query(&sig2).expect("Failed to query");

    assert!(
        candidates.contains(&doc1.id),
        "DiskLSH should return duplicate as candidate"
    );
}

#[test]
fn test_disk_lsh_persistence() {
    let temp_dir = create_temp_dir();
    let lsh_path = temp_dir.path().join("persist_lsh.redb");

    let (doc1, _) = generate_duplicate_pair();
    let sig1 = compute_signature(&doc1.content, 42);

    // Create, insert, and drop
    {
        let lsh = DiskLSH::open(&lsh_path).expect("Failed to create DiskLSH");
        lsh.insert(doc1.id, sig1.clone(), doc1.content_len as usize)
            .expect("Failed to insert");
    }

    // Reopen and verify data persisted
    {
        let lsh = DiskLSH::open(&lsh_path).expect("Failed to reopen DiskLSH");

        let candidates = lsh.query(&sig1).expect("Failed to query");

        assert!(
            candidates.contains(&doc1.id),
            "DiskLSH should persist data across reopens"
        );
    }
}

#[test]
fn test_disk_lsh_signature_retrieval() {
    let temp_dir = create_temp_dir();
    let lsh_path = temp_dir.path().join("sig_lsh.redb");

    let lsh = DiskLSH::open(&lsh_path).expect("Failed to create DiskLSH");
    let (doc1, _) = generate_duplicate_pair();

    let sig1 = compute_signature(&doc1.content, 42);
    lsh.insert(doc1.id, sig1.clone(), doc1.content_len as usize)
        .expect("Failed to insert");

    // Retrieve document entry
    let entry = lsh
        .get_document(&doc1.id)
        .expect("Failed to get document")
        .expect("Document should exist");

    assert_eq!(entry.signature, sig1, "Retrieved signature should match");
    assert_eq!(
        entry.content_len, doc1.content_len as usize,
        "Retrieved content_len should match"
    );
}

#[test]
fn test_disk_lsh_document_count() {
    let temp_dir = create_temp_dir();
    let lsh_path = temp_dir.path().join("count_lsh.redb");

    let lsh = DiskLSH::open(&lsh_path).expect("Failed to create DiskLSH");
    let docs = generate_unique_documents(5);

    for doc in &docs {
        let sig = compute_signature(&doc.content, 42);
        lsh.insert(doc.id, sig, doc.content_len as usize)
            .expect("Failed to insert");
    }

    assert_eq!(
        lsh.count().unwrap(),
        5,
        "DiskLSH should track document count"
    );
}

#[test]
fn test_disk_lsh_batch_insert() {
    let temp_dir = create_temp_dir();
    let lsh_path = temp_dir.path().join("batch_lsh.redb");

    let lsh = DiskLSH::open(&lsh_path).expect("Failed to create DiskLSH");
    let docs = generate_unique_documents(100);

    // Prepare batch data
    let batch: Vec<_> = docs
        .iter()
        .map(|doc| {
            let sig = compute_signature(&doc.content, 42);
            (doc.id, sig, doc.content_len as usize)
        })
        .collect();

    // Insert as batch
    lsh.insert_batch(&batch).expect("Failed to batch insert");

    assert_eq!(
        lsh.count().unwrap(),
        100,
        "All documents should be inserted"
    );

    // Verify we can find the first document
    let candidates = lsh.query(&batch[0].1).expect("Failed to query");
    assert!(
        candidates.contains(&batch[0].0),
        "Should find first document"
    );
}
