//! Inspect SQLite database after deduplication
//!
//! Run with: cargo run --example sqlite_inspect

use incrededup::{
    run_dedupe_with_source, DedupeConfig, DocumentSource, SourceDocument, SqliteSource,
};
use rusqlite::Connection;
use std::fs;
use uuid::Uuid;

fn generate_base_content(group_id: usize) -> String {
    format!(
        "This is document group {} with substantial content for MinHash signature generation. \
         The content includes topic {} discussion about subject matter {} with various \
         keywords like alpha-{} beta-{} gamma-{} delta-{} epsilon-{}. \
         Additional padding text to ensure sufficient length for meaningful deduplication. \
         Group identifier: GROUP_{:04}. More filler content here to reach minimum length.",
        group_id, group_id, group_id, group_id, group_id, group_id, group_id, group_id, group_id
    )
}

fn generate_similar_content(group_id: usize, variation: usize) -> String {
    let base = generate_base_content(group_id);
    format!("{} Variation {} of group {}.", base, variation, group_id)
}

fn generate_unique_content(idx: usize) -> String {
    format!(
        "Unique document number {} contains entirely different subject matter. \
         This discusses topic UNIQUE_{} with keywords zeta-{} theta-{} iota-{} kappa-{}. \
         The vocabulary and structure are intentionally different. Unique identifier: UNIQ_{:04}.",
        idx,
        idx,
        idx * 7,
        idx * 11,
        idx * 13,
        idx * 17,
        idx
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path = "/tmp/sqlite_inspect_demo.db";
    let data_dir = "/tmp/sqlite_inspect_data";

    // Clean up from previous runs
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(data_dir);
    fs::create_dir_all(data_dir)?;

    println!("=== Creating SQLite Database ===\n");
    println!("Database: {}", db_path);

    let source = SqliteSource::open(db_path)?;

    // Insert 30 documents: 20 in groups, 10 unique
    println!("Inserting documents...\n");

    // 5 groups of 4 similar docs
    for group_id in 0..5 {
        for variation in 0..4 {
            let id = Uuid::new_v4();
            let content = generate_similar_content(group_id, variation);
            source.insert_document(&SourceDocument {
                id,
                content: content.clone(),
                content_len: content.len() as i32,
                filename: Some(format!("group_{}_var_{}.txt", group_id, variation)),
            })?;
        }
    }

    // 10 unique docs
    for i in 0..10 {
        let id = Uuid::new_v4();
        let content = generate_unique_content(i);
        source.insert_document(&SourceDocument {
            id,
            content: content.clone(),
            content_len: content.len() as i32,
            filename: Some(format!("unique_{}.txt", i)),
        })?;
    }

    println!("Total documents: {}\n", source.count_total().await?);

    // Run dedupe
    println!("=== Running Deduplication ===\n");

    let dedupe_config = DedupeConfig {
        threshold: 0.7,
        batch_size: 100,
        data_dir: data_dir.into(),
        process_all: true,
        skip_db_write: false,
        min_content_length: 50,
        ..Default::default()
    };

    let stats = run_dedupe_with_source(&source, dedupe_config, Some("inspect_demo")).await?;

    println!("\nDuplicates found: {}", stats.duplicates_found);

    // Now query the SQLite database directly to show results
    println!("\n=== Querying SQLite Database ===\n");

    let conn = Connection::open(db_path)?;

    // Show document state summary
    println!("--- Document State Summary ---");
    let mut stmt = conn.prepare(
        "SELECT
            CASE
                WHEN is_parent IS NULL THEN 'unprocessed'
                WHEN is_parent = 1 THEN 'parent'
                ELSE 'child'
            END as state,
            COUNT(*) as count
        FROM documents
        GROUP BY is_parent",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;

    for row in rows {
        let (state, count) = row?;
        println!("  {}: {}", state, count);
    }

    // Show sample documents
    println!("\n--- Sample Documents (first 10) ---");
    let mut stmt = conn.prepare(
        "SELECT id, filename, content_len,
            CASE WHEN is_parent = 1 THEN 'PARENT' WHEN is_parent = 0 THEN 'CHILD' ELSE 'UNPROC' END
        FROM documents LIMIT 10",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, i32>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;

    println!("{:<38} {:<25} {:>6} State", "ID", "Filename", "Len");
    println!("{}", "-".repeat(80));
    for row in rows {
        let (id, filename, len, state) = row?;
        println!(
            "{:<38} {:<25} {:>6} {}",
            &id[..36],
            filename.unwrap_or_default(),
            len,
            state
        );
    }

    // Show duplicate matches
    println!("\n--- Duplicate Matches (dupes table) ---");
    let mut stmt = conn.prepare(
        "SELECT d.child_id, docs_child.filename, d.parent_id, docs_parent.filename,
                d.jaccard_similarity
        FROM dupes d
        JOIN documents docs_child ON d.child_id = docs_child.id
        JOIN documents docs_parent ON d.parent_id = docs_parent.id
        ORDER BY d.jaccard_similarity DESC
        LIMIT 15",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, f64>(4)?,
        ))
    })?;

    println!("{:<25} -> {:<25} Jaccard", "Child", "Parent");
    println!("{}", "-".repeat(70));
    for row in rows {
        let (child_file, parent_file, jaccard) = row?;
        println!(
            "{:<25} -> {:<25} {:.3}",
            child_file.unwrap_or_default(),
            parent_file.unwrap_or_default(),
            jaccard
        );
    }

    // Count dupes by group
    println!("\n--- Dupes per Group ---");
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM dupes", [], |row| row.get(0))?;
    println!("Total duplicate pairs in dupes table: {}", count);

    // Show database file size
    let size = fs::metadata(db_path)?.len();
    println!("\n--- Database Info ---");
    println!("Database file size: {} KB", size / 1024);

    println!("\n=== Done! ===");
    println!("Database saved at: {}", db_path);
    println!("You can inspect it with: sqlite3 {}", db_path);

    Ok(())
}
