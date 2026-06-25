//! Demo: SQLite source with full deduplication pipeline
//!
//! This creates a real SQLite database with 200 documents:
//! - 150 documents in 50 groups of 3 similar docs each (should find duplicates)
//! - 50 unique documents (should not match)
//!
//! Run with: cargo run --example sqlite_demo

use incrededup::{
    run_dedupe_with_source, DedupeConfig, DocumentSource, SourceDocument, SqliteSource,
};
use std::fs;
use tempfile::TempDir;
use uuid::Uuid;

fn generate_base_content(group_id: usize) -> String {
    // Each group has distinct base content
    format!(
        "This is document group {} with substantial content for MinHash signature generation. \
         The content includes topic {} discussion about subject matter {} with various \
         keywords like alpha-{} beta-{} gamma-{} delta-{} epsilon-{}. \
         Additional padding text to ensure sufficient length for meaningful deduplication. \
         Group identifier: GROUP_{:04}. More filler content here to reach minimum length \
         requirements for the MinHash algorithm to work effectively on this document.",
        group_id, group_id, group_id, group_id, group_id, group_id, group_id, group_id, group_id
    )
}

fn generate_similar_content(group_id: usize, variation: usize) -> String {
    let base = generate_base_content(group_id);
    // Add small variation - keeps ~90%+ similarity
    format!("{} Variation {} of group {}.", base, variation, group_id)
}

fn generate_unique_content(idx: usize) -> String {
    // Completely different content for each unique document
    format!(
        "Unique document number {} contains entirely different subject matter. \
         This discusses topic UNIQUE_{} with keywords zeta-{} theta-{} iota-{} kappa-{}. \
         The vocabulary and structure are intentionally different from all group documents. \
         Unique identifier: UNIQ_{:04}. This content should NOT match any other documents \
         in the collection because it uses completely different terminology and topics. \
         Final padding for unique document {}.",
        idx,
        idx,
        idx * 7,
        idx * 11,
        idx * 13,
        idx * 17,
        idx,
        idx
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("incrededup=info")
        .with_target(false)
        .init();

    println!("=== SQLite Deduplication Demo ===\n");

    // Create temp directories
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("demo.db");
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir)?;

    println!("SQLite database: {:?}", db_path);
    println!("Data directory: {:?}", data_dir);
    println!();

    // Create SQLite source (this creates the actual .db file)
    let source = SqliteSource::open(&db_path)?;

    // Insert documents
    println!("=== Inserting Documents ===\n");

    let num_groups = 50;
    let docs_per_group = 3;
    let num_unique = 50;

    // Insert similar document groups
    println!(
        "Inserting {} groups of {} similar documents each ({} total)...",
        num_groups,
        docs_per_group,
        num_groups * docs_per_group
    );

    for group_id in 0..num_groups {
        for variation in 0..docs_per_group {
            let id = Uuid::new_v4();
            let content = generate_similar_content(group_id, variation);
            source.insert_document(&SourceDocument {
                id,
                content: content.clone(),
                content_len: content.len() as i32,
                filename: Some(format!("group_{:03}_var_{}.txt", group_id, variation)),
            })?;
        }
    }

    // Insert unique documents
    println!("Inserting {} unique documents...", num_unique);

    for i in 0..num_unique {
        let id = Uuid::new_v4();
        let content = generate_unique_content(i);
        source.insert_document(&SourceDocument {
            id,
            content: content.clone(),
            content_len: content.len() as i32,
            filename: Some(format!("unique_{:03}.txt", i)),
        })?;
    }

    let total_docs = source.count_total().await?;
    println!("\nTotal documents in SQLite: {}", total_docs);

    // Show database file size
    let db_size = fs::metadata(&db_path)?.len();
    println!("Database file size: {} KB", db_size / 1024);
    println!();

    // Run deduplication
    println!("=== Running Deduplication Pipeline ===\n");

    let dedupe_config = DedupeConfig {
        threshold: 0.7, // 70% similarity threshold
        batch_size: 100,
        data_dir: data_dir.clone(),
        process_all: true,
        skip_db_write: false,
        min_content_length: 50,
        ..Default::default()
    };

    let start = std::time::Instant::now();
    let stats = run_dedupe_with_source(&source, dedupe_config, Some("sqlite_demo")).await?;
    let duration = start.elapsed();

    println!("\n=== Results ===\n");
    println!("Total documents processed: {}", stats.total_documents);
    println!("Duplicates found: {}", stats.duplicates_found);
    println!("Unique parents: {}", stats.unique_parents);
    println!("Candidates checked: {}", stats.candidates_checked);
    println!("Duration: {:.2}s", duration.as_secs_f64());

    // Show what files were created
    println!("\n=== Created Files ===\n");
    let dataset_dir = data_dir.join("sqlite_demo");
    for entry in fs::read_dir(&dataset_dir)? {
        let entry = entry?;
        let size = entry.metadata()?.len();
        println!("  {:?}: {} KB", entry.file_name(), size / 1024);
    }

    // Query the SQLite database to show state
    println!("\n=== SQLite State After Dedupe ===\n");
    let unprocessed = source.count_unprocessed().await?;
    println!("Unprocessed documents: {}", unprocessed);
    println!("Processed documents: {}", total_docs - unprocessed);

    // Expected results
    println!("\n=== Expected vs Actual ===\n");
    let expected_dupes = num_groups * (docs_per_group - 1); // 2 dupes per group of 3
    println!(
        "Expected duplicates (approx): {} (50 groups x 2 dupes each)",
        expected_dupes
    );
    println!("Actual duplicates found: {}", stats.duplicates_found);

    if stats.duplicates_found >= expected_dupes / 2 {
        println!("\nDeduplication working correctly.");
    } else {
        println!("\nFewer duplicates than expected (threshold may be too high)");
    }

    Ok(())
}
