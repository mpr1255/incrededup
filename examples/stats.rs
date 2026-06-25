//! Quick stats on index and state

use anyhow::Result;
use incrededup::lsh::DiskLSH;
use incrededup::storage::{MatchStore, StateStore};
use std::env;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: stats <dataset_path>");
        std::process::exit(1);
    }

    let base_path = std::path::Path::new(&args[1]);

    println!("=== LSH Index ===");
    let lsh_path = base_path.join("lsh.redb");
    if lsh_path.exists() {
        let lsh = DiskLSH::open(&lsh_path)?;
        println!("  Documents: {}", lsh.count()?);
    } else {
        println!("  NOT FOUND");
    }

    println!("\n=== Sync State ===");
    let state_path = base_path.join("state.redb");
    if state_path.exists() {
        let state = StateStore::open(&state_path)?;
        let progress = state.get_sync_progress()?;
        println!("  Step: {:?}", progress.step);
        println!(
            "  Dupes: {}/{}",
            progress.dupes_written, progress.dupes_total
        );
        println!(
            "  Parents: {}/{}",
            progress.parents_marked, progress.parents_total
        );
        println!(
            "  Children: {}/{}",
            progress.children_marked, progress.children_total
        );
    } else {
        println!("  NOT FOUND");
    }

    println!("\n=== Match Store ===");
    let match_path = base_path.join("matches.redb");
    if match_path.exists() {
        let matches = MatchStore::open(&match_path)?;
        println!("  Total matches: {}", matches.count()?);
    } else {
        println!("  NOT FOUND");
    }

    Ok(())
}
