//! incrededup: Performant, disk-based, incremental deduplication using MinHash LSH
//!
//! This library provides:
//! - MinHash signature computation (extracted from Rensa)
//! - LSH (Locality-Sensitive Hashing) indexing (in-memory and disk-backed)
//! - PostgreSQL integration for document fetching and result storage
//! - Parallel processing with rayon
//!
//! ## Example
//!
//! ```ignore
//! use incrededup::{DbConfig, DedupeConfig, run_dedupe};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let db_config = DbConfig::from_env()?.with_table("documents");
//!
//!     let dedupe_config = DedupeConfig {
//!         threshold: 0.8,
//!         ..Default::default()
//!     };
//!
//!     let stats = run_dedupe(db_config, dedupe_config).await?;
//!     println!("Found {} duplicates", stats.duplicates_found);
//!     Ok(())
//! }
//! ```

pub mod cleanup;
pub mod cli;
pub mod db;
pub mod dedupe;
pub mod disk_dedupe;
pub mod lsh;
pub mod minhash;
pub mod sources;
pub mod storage;
pub mod union_find;

// Re-export main types
pub use db::{DbConfig, DbPool, Document, DupeMatch};
pub use dedupe::{
    is_boilerplate, load_junk_patterns, resolve_transitivity, run_dedupe, run_dedupe_with_source,
    DedupeConfig, DedupeStats, EdgeLookupMode, InMemoryDeduplicator, IndexBuilder,
};
/// Deprecated compatibility alias for the Phase 1 index builder.
#[deprecated(since = "0.2.4", note = "Use IndexBuilder for Phase 1 index building")]
pub type DiskIndexBuilder = IndexBuilder;
pub use cleanup::{
    find_pathological_clusters, run_cleanup, run_phase_1_5, CleanupAction, CleanupStats,
    PathologicalCluster,
};
pub use cli::{Args, EdgeLookupArg};
pub use disk_dedupe::{run_disk_dedupe, DiskDedupeStats, DiskDeduplicator};
pub use lsh::{DiskLSH, InMemoryLSH};
pub use minhash::{
    calculate_band_hash, compute_band_hashes, jaccard_from_signatures, try_compute_band_hashes,
    try_jaccard_from_signatures, RMinHash, NUM_BANDS, NUM_PERM, ROWS_PER_BAND,
};
pub use sources::{
    DocumentSource, FileSystemSource, PostgresSource, SourceDocument, SourceDupeMatch, SqliteSource,
};
pub use storage::{
    DatasetStorage, FilteredParentStore, MatchRecord, MatchStore, StateStore, SyncProgress,
    SyncStep,
};
pub use union_find::UnionFind;
