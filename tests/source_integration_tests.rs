//! Integration tests for DocumentSource implementations.
//!
//! These tests create REAL files on disk and REAL SQLite databases to verify
//! the source implementations work correctly with the deduplication pipeline.

use incrededup::{
    run_dedupe_with_source, sources::filesystem::FileSystemConfig, DedupeConfig, DocumentSource,
    FileSystemSource, SourceDocument, SqliteSource,
};
use std::fs;
use tempfile::TempDir;
use uuid::Uuid;

/// Helper: Generate content that will be different (unique documents)
fn generate_unique_content(idx: usize) -> String {
    // Each document is completely different
    format!(
        "This is unique document number {} with completely different content. \
         It contains various words and phrases that make it distinct from \
         any other document in the collection. Document identifier: {}. \
         Random data: {}{}{}. End of document {}.",
        idx,
        idx,
        idx * 13,
        idx * 17,
        idx * 23,
        idx
    )
}

// ============================================================
// FileSystem Source Integration Tests
// ============================================================

#[tokio::test]
async fn test_filesystem_source_creates_real_files() {
    // Create a temporary directory
    let temp_dir = TempDir::new().unwrap();
    let dir_path = temp_dir.path();

    // Create real files on disk
    let files = vec![
        (
            "doc1.txt",
            "This is the first document content that is long enough to be indexed.",
        ),
        (
            "doc2.txt",
            "This is the second document content that is also long enough to be indexed.",
        ),
        (
            "doc3.txt",
            "This is the third document with completely different content for testing purposes.",
        ),
    ];

    for (name, content) in &files {
        let path = dir_path.join(name);
        fs::write(&path, content).unwrap();

        // Verify file exists and has correct content
        assert!(path.exists(), "File {} should exist", name);
        let read_content = fs::read_to_string(&path).unwrap();
        assert_eq!(read_content, *content);
    }

    // Create source and verify it reads the files
    let source = FileSystemSource::from_dir(dir_path);
    let count = source.count_total().await.unwrap();
    assert_eq!(count, 3, "Should find 3 files");

    // Fetch all documents and verify content
    let docs = source.fetch_all_after(None, 100).await.unwrap();
    assert_eq!(docs.len(), 3);

    for doc in &docs {
        assert!(!doc.content.is_empty());
        assert!(doc.filename.is_some());
    }
}

#[tokio::test]
async fn test_filesystem_source_with_duplicates_creates_output() {
    let temp_dir = TempDir::new().unwrap();
    let dir_path = temp_dir.path();
    let output_dir = temp_dir.path().join("output");

    // Create some similar documents (should be detected as duplicates)
    let base_content = "This is a base document content that will be used to create \
                       similar documents. It needs to be long enough to generate \
                       meaningful MinHash signatures for deduplication testing. \
                       Adding more words here to ensure the content is substantial.";

    // Create duplicate-like files (very similar content)
    fs::write(
        dir_path.join("original.txt"),
        format!("{} This is the original.", base_content),
    )
    .unwrap();
    fs::write(
        dir_path.join("duplicate1.txt"),
        format!("{} This is a slight variation.", base_content),
    )
    .unwrap();
    fs::write(
        dir_path.join("duplicate2.txt"),
        format!("{} Another slight variation here.", base_content),
    )
    .unwrap();

    // Create a unique document
    fs::write(
        dir_path.join("unique.txt"),
        "This is completely different content that has nothing to do with the base. \
         It discusses different topics and uses different vocabulary entirely. \
         There should be no similarity between this and the other documents.",
    )
    .unwrap();

    // Verify files exist
    assert!(dir_path.join("original.txt").exists());
    assert!(dir_path.join("duplicate1.txt").exists());
    assert!(dir_path.join("duplicate2.txt").exists());
    assert!(dir_path.join("unique.txt").exists());

    // Create source with output directory
    let config = FileSystemConfig::new(dir_path);
    let source = FileSystemSource::new(config).with_output_dir(&output_dir);

    let count = source.count_total().await.unwrap();
    assert_eq!(count, 4);

    // Fetch documents
    let docs = source.fetch_all_after(None, 100).await.unwrap();
    assert_eq!(docs.len(), 4);

    // Verify we can get documents by ID
    let first_id = docs[0].id;
    let fetched = source.fetch_by_ids(&[first_id]).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].id, first_id);
}

#[tokio::test]
async fn test_filesystem_full_dedupe_pipeline() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("input");
    let data_dir = temp_dir.path().join("data");
    let output_dir = temp_dir.path().join("output");

    fs::create_dir_all(&input_dir).unwrap();
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    // Create files with sufficient content for MinHash
    let base = "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do \
                eiusmod tempor incididunt ut labore et dolore magna aliqua. \
                Ut enim ad minim veniam quis nostrud exercitation ullamco laboris \
                nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor \
                in reprehenderit in voluptate velit esse cillum dolore eu fugiat \
                nulla pariatur. Excepteur sint occaecat cupidatat non proident \
                sunt in culpa qui officia deserunt mollit anim id est laborum.";

    // Create 5 documents: 3 similar, 2 unique
    for i in 0..3 {
        fs::write(
            input_dir.join(format!("similar_{}.txt", i)),
            format!("{} Document variation {}.", base, i),
        )
        .unwrap();
    }

    for i in 0..2 {
        fs::write(
            input_dir.join(format!("unique_{}.txt", i)),
            generate_unique_content(i),
        )
        .unwrap();
    }

    // Verify files were created
    let file_count = fs::read_dir(&input_dir).unwrap().count();
    assert_eq!(file_count, 5);

    // Create source and run dedupe
    let config = FileSystemConfig::new(&input_dir);
    let source = FileSystemSource::new(config).with_output_dir(&output_dir);

    let dedupe_config = DedupeConfig {
        threshold: 0.7, // Lower threshold for test documents
        batch_size: 100,
        data_dir: data_dir.clone(),
        process_all: true,
        skip_db_write: false,
        min_content_length: 100,
        ..Default::default()
    };

    let stats = run_dedupe_with_source(&source, dedupe_config, Some("test_fs"))
        .await
        .unwrap();

    // Verify stats
    assert_eq!(stats.total_documents, 5);

    // Verify data files were created
    let lsh_path = data_dir.join("test_fs").join("lsh.redb");
    assert!(
        lsh_path.exists(),
        "LSH index should be created at {:?}",
        lsh_path
    );
}

// ============================================================
// SQLite Source Integration Tests
// ============================================================

#[tokio::test]
async fn test_sqlite_source_creates_real_database() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create SQLite source (this creates the actual database file)
    let source = SqliteSource::open(&db_path).unwrap();

    // Verify database file exists
    assert!(db_path.exists(), "SQLite database file should exist");

    // Insert some documents
    let doc_ids: Vec<Uuid> = (0..5)
        .map(|i| {
            let id = Uuid::new_v4();
            let content = format!(
                "Document {} content that is long enough for indexing. \
                 It needs to have sufficient length to be meaningful.",
                i
            );
            let doc = SourceDocument {
                id,
                content: content.clone(),
                content_len: content.len() as i32,
                filename: Some(format!("doc{}.txt", i)),
            };
            source.insert_document(&doc).unwrap();
            id
        })
        .collect();

    // Verify count
    let count = source.count_total().await.unwrap();
    assert_eq!(count, 5);

    // Verify we can fetch documents
    let docs = source.fetch_all_after(None, 100).await.unwrap();
    assert_eq!(docs.len(), 5);

    // Verify we can fetch by specific IDs
    let fetched = source.fetch_by_ids(&doc_ids[0..2]).await.unwrap();
    assert_eq!(fetched.len(), 2);

    // Verify database file still exists and has data
    let metadata = fs::metadata(&db_path).unwrap();
    assert!(metadata.len() > 0, "Database file should have data");
}

#[tokio::test]
async fn test_sqlite_source_with_state_tracking() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_state.db");

    let source = SqliteSource::open(&db_path).unwrap();

    // Insert documents
    let mut ids = Vec::new();
    for i in 0..10 {
        let id = Uuid::new_v4();
        let content = format!("Content for document {}", i);
        let doc = SourceDocument {
            id,
            content: content.clone(),
            content_len: content.len() as i32,
            filename: None,
        };
        source.insert_document(&doc).unwrap();
        ids.push(id);
    }

    // Initially all should be unprocessed
    let unprocessed = source.count_unprocessed().await.unwrap();
    assert_eq!(unprocessed, 10);

    // Mark some as parents
    source.mark_as_parents(&ids[0..3]).await.unwrap();

    // Mark some as children
    source.mark_as_children(&ids[3..5]).await.unwrap();

    // Now unprocessed count should be reduced
    let unprocessed = source.count_unprocessed().await.unwrap();
    assert_eq!(unprocessed, 5); // 10 - 3 parents - 2 children = 5

    // Verify tracks_state returns true
    assert!(source.tracks_state());
}

#[tokio::test]
async fn test_sqlite_source_write_duplicates() {
    use incrededup::sources::SourceDupeMatch;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_dupes.db");

    let source = SqliteSource::open(&db_path).unwrap();

    // Insert documents
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let id3 = Uuid::new_v4();

    source
        .insert_document(&SourceDocument {
            id: id1,
            content: "Parent document content".to_string(),
            content_len: 24,
            filename: None,
        })
        .unwrap();
    source
        .insert_document(&SourceDocument {
            id: id2,
            content: "Child document 1 content".to_string(),
            content_len: 25,
            filename: None,
        })
        .unwrap();
    source
        .insert_document(&SourceDocument {
            id: id3,
            content: "Child document 2 content".to_string(),
            content_len: 25,
            filename: None,
        })
        .unwrap();

    // Write duplicate matches
    let matches = vec![
        SourceDupeMatch {
            child_id: id2,
            parent_id: id1,
            jaccard_similarity: 0.85,
            size_difference: 10,
            size_difference_pct: 0.05,
        },
        SourceDupeMatch {
            child_id: id3,
            parent_id: id1,
            jaccard_similarity: 0.90,
            size_difference: 5,
            size_difference_pct: 0.02,
        },
    ];

    let written = source.write_dupes(&matches).await.unwrap();
    assert_eq!(written, 2);

    // Verify supports_write returns true
    assert!(source.supports_write());
}

#[tokio::test]
async fn test_sqlite_full_dedupe_pipeline() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_pipeline.db");
    let data_dir = temp_dir.path().join("data");

    fs::create_dir_all(&data_dir).unwrap();

    // Create SQLite source
    let source = SqliteSource::open(&db_path).unwrap();

    // Insert documents with sufficient content
    let base = "This is a base content string that will be used to create \
                documents with similar content. It needs to be long enough \
                to generate meaningful MinHash signatures. Additional padding \
                text to ensure the documents are of sufficient length for \
                proper deduplication testing and verification.";

    // Insert similar documents (should be detected as duplicates)
    for i in 0..3 {
        let id = Uuid::new_v4();
        let content = format!("{} Slight variation number {}.", base, i);
        source
            .insert_document(&SourceDocument {
                id,
                content: content.clone(),
                content_len: content.len() as i32,
                filename: Some(format!("similar_{}.txt", i)),
            })
            .unwrap();
    }

    // Insert unique documents
    for i in 0..2 {
        let id = Uuid::new_v4();
        let content = generate_unique_content(i);
        source
            .insert_document(&SourceDocument {
                id,
                content: content.clone(),
                content_len: content.len() as i32,
                filename: Some(format!("unique_{}.txt", i)),
            })
            .unwrap();
    }

    // Verify documents were inserted
    let count = source.count_total().await.unwrap();
    assert_eq!(count, 5);

    // Run dedupe pipeline
    let dedupe_config = DedupeConfig {
        threshold: 0.7,
        batch_size: 100,
        data_dir: data_dir.clone(),
        process_all: true,
        skip_db_write: false,
        min_content_length: 50,
        ..Default::default()
    };

    let stats = run_dedupe_with_source(&source, dedupe_config, Some("test_sqlite"))
        .await
        .unwrap();

    // Verify stats
    assert_eq!(stats.total_documents, 5);

    // Verify LSH index was created
    let lsh_path = data_dir.join("test_sqlite").join("lsh.redb");
    assert!(
        lsh_path.exists(),
        "LSH index should be created at {:?}",
        lsh_path
    );

    // Verify database still exists and has the updated state
    assert!(db_path.exists());
    let final_count = source.count_total().await.unwrap();
    assert_eq!(final_count, 5);
}

#[tokio::test]
async fn test_sqlite_persistence_across_opens() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("persist.db");

    // First: create and populate database
    {
        let source = SqliteSource::open(&db_path).unwrap();
        let id = Uuid::new_v4();
        source
            .insert_document(&SourceDocument {
                id,
                content: "Persistent document content".to_string(),
                content_len: 28,
                filename: Some("persist.txt".to_string()),
            })
            .unwrap();
        let count = source.count_total().await.unwrap();
        assert_eq!(count, 1);
    } // Source dropped, connection closed

    // Second: reopen and verify data persists
    {
        let source = SqliteSource::open(&db_path).unwrap();
        let count = source.count_total().await.unwrap();
        assert_eq!(count, 1, "Data should persist after reopening");

        let docs = source.fetch_all_after(None, 10).await.unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].content, "Persistent document content");
    }
}

// ============================================================
// Cross-Source Compatibility Tests
// ============================================================

#[tokio::test]
async fn test_both_sources_produce_consistent_results() {
    // This test verifies that FileSystemSource and SqliteSource
    // produce consistent behavior when given the same documents

    let temp_dir = TempDir::new().unwrap();

    // Same documents for both sources
    let documents = vec![
        ("doc1.txt", "First document with some content for testing."),
        ("doc2.txt", "Second document with different content here."),
        ("doc3.txt", "Third document that is unique and different."),
    ];

    // Create filesystem source
    let fs_dir = temp_dir.path().join("filesystem");
    fs::create_dir_all(&fs_dir).unwrap();
    for (name, content) in &documents {
        fs::write(fs_dir.join(name), content).unwrap();
    }
    let fs_source = FileSystemSource::from_dir(&fs_dir);

    // Create SQLite source
    let db_path = temp_dir.path().join("sqlite.db");
    let sqlite_source = SqliteSource::open(&db_path).unwrap();
    for (name, content) in &documents {
        let id = Uuid::new_v4();
        sqlite_source
            .insert_document(&SourceDocument {
                id,
                content: content.to_string(),
                content_len: content.len() as i32,
                filename: Some(name.to_string()),
            })
            .unwrap();
    }

    // Both should report same count
    let fs_count = fs_source.count_total().await.unwrap();
    let sqlite_count = sqlite_source.count_total().await.unwrap();
    assert_eq!(fs_count, sqlite_count);
    assert_eq!(fs_count, 3);

    // Both should return documents
    let fs_docs = fs_source.fetch_all_after(None, 100).await.unwrap();
    let sqlite_docs = sqlite_source.fetch_all_after(None, 100).await.unwrap();
    assert_eq!(fs_docs.len(), sqlite_docs.len());

    // Verify both have proper content
    for doc in &fs_docs {
        assert!(!doc.content.is_empty());
    }
    for doc in &sqlite_docs {
        assert!(!doc.content.is_empty());
    }
}
