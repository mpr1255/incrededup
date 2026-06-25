//! PostgreSQL document source implementation.
//!
//! This wraps the existing `DbPool` to implement the `DocumentSource` trait,
//! maintaining full backwards compatibility.

use super::{DocumentSource, SourceDocument, SourceDupeMatch};
use crate::db::{DbConfig, DbPool, DupeMatch};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// PostgreSQL document source.
///
/// Wraps the existing `DbPool` implementation to provide the `DocumentSource` trait.
/// This maintains full backwards compatibility with existing code.
pub struct PostgresSource {
    pool: DbPool,
}

impl PostgresSource {
    /// Create a new PostgreSQL source from config
    pub async fn new(config: DbConfig) -> Result<Self> {
        let pool = DbPool::new(config).await?;
        Ok(Self { pool })
    }

    /// Create from an existing DbPool
    pub fn from_pool(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Get the underlying pool for advanced operations
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Get dataset ID from config
    pub fn dataset_id(&self) -> Option<Uuid> {
        self.pool.config().dataset_id
    }
}

#[async_trait]
impl DocumentSource for PostgresSource {
    async fn source_name(&self) -> Result<String> {
        Ok(self.pool.config().source_name())
    }

    async fn count_total(&self) -> Result<i64> {
        self.pool.count_total().await
    }

    async fn count_unprocessed(&self) -> Result<i64> {
        self.pool.count_unprocessed().await
    }

    async fn fetch_all_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<SourceDocument>> {
        let docs = self.pool.fetch_chunk_after_all(last_id, limit).await?;
        Ok(docs
            .into_iter()
            .map(|d| SourceDocument {
                id: d.id,
                content: d.content,
                content_len: d.content_len,
                filename: d.filename,
            })
            .collect())
    }

    async fn fetch_unprocessed_ids_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Uuid>> {
        self.pool.fetch_unprocessed_ids_after(last_id, limit).await
    }

    async fn fetch_by_ids(&self, ids: &[Uuid]) -> Result<Vec<SourceDocument>> {
        let docs = self.pool.fetch_documents_by_ids(ids).await?;
        Ok(docs
            .into_iter()
            .map(|d| SourceDocument {
                id: d.id,
                content: d.content,
                content_len: d.content_len,
                filename: d.filename,
            })
            .collect())
    }

    async fn fetch_existing_parent_ids(&self, ids: &[Uuid]) -> Result<HashSet<Uuid>> {
        self.pool.fetch_parent_ids_by_doc_ids(ids).await
    }

    async fn fetch_existing_dupe_parents(&self, child_ids: &[Uuid]) -> Result<HashMap<Uuid, Uuid>> {
        self.pool.fetch_dupe_parents_by_child_ids(child_ids).await
    }

    async fn mark_as_parents(&self, ids: &[Uuid]) -> Result<u64> {
        self.pool.mark_as_parents(ids).await
    }

    async fn mark_as_children(&self, ids: &[Uuid]) -> Result<u64> {
        self.pool.mark_as_children(ids).await
    }

    async fn write_dupes(&self, matches: &[SourceDupeMatch]) -> Result<u64> {
        let dupe_matches: Vec<DupeMatch> = matches
            .iter()
            .map(|m| DupeMatch {
                child_id: m.child_id,
                parent_id: m.parent_id,
                jaccard_similarity: m.jaccard_similarity,
                size_difference: m.size_difference,
                size_difference_pct: m.size_difference_pct,
            })
            .collect();
        self.pool.write_dupes(&dupe_matches).await
    }

    fn supports_write(&self) -> bool {
        true
    }

    fn tracks_state(&self) -> bool {
        true
    }
}
