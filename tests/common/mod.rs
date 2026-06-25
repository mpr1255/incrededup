//! Common test utilities and synthetic data generators.
//!
//! This module provides helper functions for creating test documents
//! with known similarity relationships.

#![allow(dead_code)]

use uuid::Uuid;

/// A test document with known properties
#[derive(Debug, Clone)]
pub struct TestDocument {
    pub id: Uuid,
    pub content: String,
    pub content_len: i32,
}

impl TestDocument {
    pub fn new(content: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            content: content.to_string(),
            content_len: content.len() as i32,
        }
    }

    pub fn with_id(id: Uuid, content: &str) -> Self {
        Self {
            id,
            content: content.to_string(),
            content_len: content.len() as i32,
        }
    }
}

/// Generate a document with repeated content to ensure sufficient length
pub fn generate_long_document(base_content: &str, min_length: usize) -> String {
    let mut content = base_content.to_string();
    while content.len() < min_length {
        content.push(' ');
        content.push_str(base_content);
    }
    content
}

/// Generate two documents that are exact duplicates
pub fn generate_duplicate_pair() -> (TestDocument, TestDocument) {
    let content = generate_long_document(
        "The quick brown fox jumps over the lazy dog. This is a test document \
         with enough content to be meaningful for deduplication testing purposes. \
         We need sufficient text to generate proper MinHash signatures.",
        600,
    );
    (TestDocument::new(&content), TestDocument::new(&content))
}

/// Generate two documents that are similar but not identical (~80% similar)
pub fn generate_similar_pair() -> (TestDocument, TestDocument) {
    let base = "The quick brown fox jumps over the lazy dog. This is a test document \
                with enough content to be meaningful for deduplication testing purposes. \
                We need sufficient text to generate proper MinHash signatures. \
                Additional content here to pad out the document length for testing.";

    // Modify ~20% of the content
    let modified = "The quick brown fox jumps over the lazy dog. This is a test document \
                    with enough content to be meaningful for deduplication testing purposes. \
                    We need sufficient text to generate proper MinHash signatures. \
                    Different content here that changes the document somewhat for testing.";

    let content1 = generate_long_document(base, 600);
    let content2 = generate_long_document(modified, 600);

    (TestDocument::new(&content1), TestDocument::new(&content2))
}

/// Generate two documents that are clearly different (<50% similar)
pub fn generate_different_pair() -> (TestDocument, TestDocument) {
    let content1 = generate_long_document(
        "The quick brown fox jumps over the lazy dog. This document discusses \
         wildlife and animals in the forest. Foxes are cunning creatures that \
         hunt for food in the wilderness.",
        600,
    );

    let content2 = generate_long_document(
        "Machine learning algorithms process data to find patterns. Neural networks \
         consist of layers of interconnected nodes. Deep learning has revolutionized \
         computer vision and natural language processing.",
        600,
    );

    (TestDocument::new(&content1), TestDocument::new(&content2))
}

/// Generate a chain of similar documents: A ≈ B ≈ C
/// This tests transitivity resolution
pub fn generate_similarity_chain() -> (TestDocument, TestDocument, TestDocument) {
    let base = "The quick brown fox jumps over the lazy dog in the forest clearing. \
                This document contains text about animals and nature and wildlife. \
                The forest is home to many creatures including foxes and dogs.";

    let middle = "The quick brown fox jumps over the lazy dog in the forest clearing. \
                  This document contains text about animals and nature and wildlife. \
                  The woods are home to many creatures including foxes and wolves.";

    let end = "The quick brown fox jumps over the sleepy dog in the forest clearing. \
               This document contains text about animals and nature and wildlife. \
               The woods are home to many creatures including foxes and wolves.";

    let content1 = generate_long_document(base, 600);
    let content2 = generate_long_document(middle, 600);
    let content3 = generate_long_document(end, 600);

    (
        TestDocument::new(&content1),
        TestDocument::new(&content2),
        TestDocument::new(&content3),
    )
}

/// Generate a short document (below typical min_content_length threshold)
pub fn generate_short_document() -> TestDocument {
    TestDocument::new("Short text that is too brief for deduplication.")
}

/// Generate a batch of unique documents
pub fn generate_unique_documents(count: usize) -> Vec<TestDocument> {
    (0..count)
        .map(|i| {
            let content = generate_long_document(
                &format!(
                    "Unique document number {} with distinct content that should not match \
                     any other documents in the test set. Each document has its own \
                     identifier and unique text patterns. Document ID: {}",
                    i, i
                ),
                600,
            );
            TestDocument::new(&content)
        })
        .collect()
}

/// Generate a set of documents where some are duplicates
/// Returns (all_docs, expected_duplicate_pairs)
pub fn generate_mixed_dataset() -> (Vec<TestDocument>, Vec<(Uuid, Uuid)>) {
    let mut docs = Vec::new();
    let mut expected_pairs = Vec::new();

    // Add some unique documents
    docs.extend(generate_unique_documents(5));

    // Add a duplicate pair
    let (dup1, dup2) = generate_duplicate_pair();
    let pair1 = (dup1.id.min(dup2.id), dup1.id.max(dup2.id));
    expected_pairs.push(pair1);
    docs.push(dup1);
    docs.push(dup2);

    // Add another duplicate pair
    let (dup3, dup4) = generate_duplicate_pair();
    let pair2 = (dup3.id.min(dup4.id), dup3.id.max(dup4.id));
    expected_pairs.push(pair2);
    docs.push(dup3);
    docs.push(dup4);

    // Add more unique documents
    docs.extend(generate_unique_documents(3));

    (docs, expected_pairs)
}

/// Create a temporary directory for test data
pub fn create_temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp directory")
}
