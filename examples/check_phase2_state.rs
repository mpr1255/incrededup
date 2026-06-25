//! Check Phase 2 state in state.redb (using the phase2_processed table)

use anyhow::Result;
use redb::{Database, ReadableTableMetadata, TableDefinition};
use std::env;
use uuid::Uuid;

// Table definitions from disk_dedupe.rs
const PHASE2_STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("phase2_processed");
const PHASE2_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("phase2_meta");

#[derive(Debug, serde::Deserialize)]
struct Phase2Metadata {
    duplicates_found: usize,
    candidates_checked: usize,
    last_saved: String,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: check_phase2_state <state.redb path> [uuid1] [uuid2] ...");
        std::process::exit(1);
    }

    let state_path = &args[1];
    println!("Opening state.redb: {}", state_path);

    let db = Database::open(state_path)?;
    let read_txn = db.begin_read()?;

    // Check metadata
    println!("\n=== Phase 2 Metadata ===");
    if let Ok(meta_table) = read_txn.open_table(PHASE2_META_TABLE) {
        if let Some(data) = meta_table.get("metadata")? {
            let meta: Phase2Metadata = bincode::deserialize(data.value())?;
            println!("  Duplicates found: {}", meta.duplicates_found);
            println!("  Candidates checked: {}", meta.candidates_checked);
            println!("  Last saved: {}", meta.last_saved);
        } else {
            println!("  No metadata found");
        }
    } else {
        println!("  phase2_meta table not found");
    }

    // Check processed count
    println!("\n=== Phase 2 Processed Docs ===");
    if let Ok(state_table) = read_txn.open_table(PHASE2_STATE_TABLE) {
        let count = state_table.len()?;
        println!("  Total processed: {}", count);

        // Check specific UUIDs if provided
        if args.len() > 2 {
            println!("\n=== Checking specific docs ===");
            for uuid_str in &args[2..] {
                let uuid = Uuid::parse_str(uuid_str)?;
                let is_processed = state_table.get(uuid.as_bytes().as_slice())?.is_some();
                println!(
                    "  {} -> {}",
                    uuid,
                    if is_processed {
                        "PROCESSED"
                    } else {
                        "NOT PROCESSED"
                    }
                );
            }
        }
    } else {
        println!("  phase2_processed table not found");
    }

    // List all tables in the database
    println!("\n=== All Tables in DB ===");
    // This is tricky with redb - we can try opening known tables
    let tables = [
        "phase2_processed",
        "phase2_meta",
        "state",
        "meta",
        "matches",
    ];
    for table_name in tables {
        // Try as &[u8] key table
        if table_name == "phase2_processed" || table_name == "state" {
            let def: TableDefinition<&[u8], &[u8]> = TableDefinition::new(table_name);
            if let Ok(t) = read_txn.open_table(def) {
                println!("  {} - {} entries", table_name, t.len()?);
            }
        } else {
            let def: TableDefinition<&str, &[u8]> = TableDefinition::new(table_name);
            if let Ok(t) = read_txn.open_table(def) {
                println!("  {} - {} entries", table_name, t.len()?);
            }
        }
    }

    Ok(())
}
