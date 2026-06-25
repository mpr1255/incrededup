//! Document source abstractions for pluggable data connectors.
//!
//! This module provides the `DocumentSource` trait that abstracts over different
//! data sources (PostgreSQL, SQLite, filesystem, etc.) allowing the deduplication
//! pipeline to work with any data source.
//!
//! # Implementing a Custom Source
//!
//! To create a custom data source, implement the `DocumentSource` trait:
//!
//! ```ignore
//! use incrededup::sources::{DocumentSource, SourceDocument};
//! use async_trait::async_trait;
//!
//! struct MyCustomSource { /* ... */ }
//!
//! #[async_trait]
//! impl DocumentSource for MyCustomSource {
//!     async fn count_total(&self) -> anyhow::Result<i64> { /* ... */ }
//!     // ... implement other methods
//! }
//! ```

pub mod filesystem;
pub mod postgres;
pub mod sql_dialect;
pub mod sqlite;

use anyhow::Result;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// A document from the data source
#[derive(Debug, Clone)]
pub struct SourceDocument {
    pub id: Uuid,
    pub content: String,
    pub content_len: i32,
    pub filename: Option<String>,
}

/// A duplicate match to write back to the data source
#[derive(Debug, Clone)]
pub struct SourceDupeMatch {
    pub child_id: Uuid,
    pub parent_id: Uuid,
    pub jaccard_similarity: f64,
    pub size_difference: i32,
    pub size_difference_pct: f64,
}

/// Trait for document data sources.
///
/// Implementations provide access to documents for deduplication and
/// optionally support writing results back to the source.
///
/// # Required Methods
///
/// Core read methods fail by default so incomplete sources do not look like
/// empty datasets. Optional write/state methods default to no-op behavior.
#[async_trait]
pub trait DocumentSource: Send + Sync {
    /// Get a human-readable name for the data source (e.g., dataset name)
    async fn source_name(&self) -> Result<String> {
        Ok("unknown".to_string())
    }

    /// Count total documents in the source
    async fn count_total(&self) -> Result<i64> {
        anyhow::bail!("count_total is not implemented for this DocumentSource")
    }

    /// Count unprocessed documents (not yet deduplicated)
    /// For sources without state tracking, this returns count_total()
    async fn count_unprocessed(&self) -> Result<i64> {
        self.count_total().await
    }

    /// Fetch a batch of ALL documents using keyset pagination.
    /// Returns documents with id > last_id, up to `limit` documents.
    /// Pass None for last_id on first call.
    async fn fetch_all_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<SourceDocument>> {
        let _ = (last_id, limit);
        anyhow::bail!("fetch_all_after is not implemented for this DocumentSource")
    }

    /// Fetch IDs of unprocessed documents only.
    /// For sources without state tracking, this behaves like fetch_all_after but returns only IDs.
    async fn fetch_unprocessed_ids_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Uuid>> {
        // Default: fetch all and extract IDs
        let docs = self.fetch_all_after(last_id, limit).await?;
        Ok(docs.into_iter().map(|d| d.id).collect())
    }

    /// Fetch documents by specific IDs
    async fn fetch_by_ids(&self, ids: &[Uuid]) -> Result<Vec<SourceDocument>> {
        let _ = ids;
        anyhow::bail!("fetch_by_ids is not implemented for this DocumentSource")
    }

    /// Fetch IDs currently marked as canonical parents.
    ///
    /// Implementations that track state should override this. The default is
    /// empty so read-only sources can still use the dedupe pipeline.
    async fn fetch_existing_parent_ids(&self, _ids: &[Uuid]) -> Result<HashSet<Uuid>> {
        Ok(HashSet::new())
    }

    /// Fetch existing canonical assignments keyed by child ID.
    ///
    /// Incremental sync uses this to avoid rewriting unchanged historical rows,
    /// while still repairing rows when a new document bridges existing clusters.
    async fn fetch_existing_dupe_parents(
        &self,
        _child_ids: &[Uuid],
    ) -> Result<HashMap<Uuid, Uuid>> {
        Ok(HashMap::new())
    }

    /// Mark documents as parents (unique documents or duplicate group leaders)
    /// This is optional - sources without state tracking can no-op
    async fn mark_as_parents(&self, _ids: &[Uuid]) -> Result<u64> {
        Ok(0)
    }

    /// Mark documents as children (duplicates pointing to a parent)
    /// This is optional - sources without state tracking can no-op
    async fn mark_as_children(&self, _ids: &[Uuid]) -> Result<u64> {
        Ok(0)
    }

    /// Write duplicate matches to the source
    /// This is optional - sources that don't store results can no-op
    async fn write_dupes(&self, _matches: &[SourceDupeMatch]) -> Result<u64> {
        Ok(0)
    }

    /// Check if this source supports writing results back
    fn supports_write(&self) -> bool {
        false
    }

    /// Check if this source tracks processing state (is_parent field)
    fn tracks_state(&self) -> bool {
        false
    }
}

// Re-export implementations
pub use filesystem::FileSystemSource;
pub use postgres::PostgresSource;
pub use sql_dialect::SqlDialect;
pub use sqlite::SqliteSource;
