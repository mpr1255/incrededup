//! Canonical local storage for deduplication results.
//!
//! This module provides disk-backed storage for:
//! - matches.redb: Duplicate pairs (child → parent with similarity metrics)
//! - state.redb: Sync progress for resumable database sync
//!
//! The PostgreSQL database becomes write-only - we sync our local
//! canonical data to it, but never depend on it for correctness.

use anyhow::{Context, Result};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Match record stored in matches.redb
/// Mirrors the dupes table schema
#[derive(Debug, Clone)]
pub struct MatchRecord {
    pub child_id: Uuid,
    pub parent_id: Uuid,
    pub jaccard_similarity: f64,
    pub size_difference: i32,
    pub size_difference_pct: f64,
}

fn match_key(child_id: Uuid, parent_id: Uuid) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(child_id.as_bytes());
    key[16..].copy_from_slice(parent_id.as_bytes());
    key
}

fn child_id_from_match_key(key: &[u8]) -> Result<Uuid> {
    match key.len() {
        16 | 32 => Ok(Uuid::from_slice(&key[..16])?),
        len => anyhow::bail!("Invalid match key length: {}", len),
    }
}

// Table definitions for matches.redb
// Key: child_id + parent_id (32 bytes UUID pair) for new records. Readers also
// support legacy child_id-only 16 byte keys.
const MATCHES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("matches");

// Adjacency side-index for matches.redb.
//
// A derived cache that lets Phase 3 find the connected component touched by a
// set of seed docs with prefix range scans instead of a full table scan. It is
// rebuildable from MATCHES_TABLE and is written in the same transaction as the
// edge it indexes, so it never diverges from the canonical matches on crash.
//
// Each real edge (child, parent) with child != parent contributes two entries,
// one per endpoint:
//   key   = node_id (16) ++ child_id (16) ++ parent_id (16)   [48 bytes]
//   value = jaccard (8) ++ size_diff (4) ++ size_diff_pct (8)  [20 bytes]
// Keying by the full directed edge keeps entries unique even if both (a,b) and
// (b,a) ever exist, and embedding the metrics keeps the reader self-contained
// (no MATCHES_TABLE lookup, so legacy 16-byte keys are irrelevant to it).
const ADJACENCY_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("adjacency");

// META key set to [1] once a full adjacency backfill has completed. Readers use
// it to decide whether the index is complete enough to trust.
const ADJACENCY_BUILT_KEY: &str = "adjacency_built";

fn adjacency_key(node: &Uuid, child: &Uuid, parent: &Uuid) -> [u8; 48] {
    let mut key = [0u8; 48];
    key[..16].copy_from_slice(node.as_bytes());
    key[16..32].copy_from_slice(child.as_bytes());
    key[32..].copy_from_slice(parent.as_bytes());
    key
}

fn adjacency_value(record: &MatchRecord) -> [u8; 20] {
    let mut buf = [0u8; 20];
    buf[..8].copy_from_slice(&record.jaccard_similarity.to_le_bytes());
    buf[8..12].copy_from_slice(&record.size_difference.to_le_bytes());
    buf[12..].copy_from_slice(&record.size_difference_pct.to_le_bytes());
    buf
}

/// The two adjacency entries (one per endpoint) for a real edge, or `None` for a
/// self-edge (which is never indexed). Returns `(from_child_key, from_parent_key,
/// value)`.
fn adjacency_entries(record: &MatchRecord) -> Option<([u8; 48], [u8; 48], [u8; 20])> {
    if record.child_id == record.parent_id {
        return None;
    }
    let value = adjacency_value(record);
    let from_child = adjacency_key(&record.child_id, &record.child_id, &record.parent_id);
    let from_parent = adjacency_key(&record.parent_id, &record.child_id, &record.parent_id);
    Some((from_child, from_parent, value))
}

// Filtered parent docs are documents intentionally skipped before indexing
// (short/boilerplate). They are only persisted when DB writes are skipped, so a
// later manual sync can still mark them as parents without self-edge bloat.
const FILTERED_PARENTS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("filtered_parents");

// Metadata table (shared in both DBs)
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

// Sync progress table (in state.redb)
// Key: field name, Value: serialized value
const SYNC_STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sync_state");

fn begin_quick_repair_write(db: &Database) -> Result<redb::WriteTransaction> {
    let mut write_txn = db.begin_write()?;
    write_txn.set_quick_repair(true);
    Ok(write_txn)
}

/// Sync progress tracking - which step of sync we're on
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStep {
    /// Sync has not started
    NotStarted = 0,
    /// Writing dupes to database
    WritingDupes = 1,
    /// Marking parents (is_parent = true)
    MarkingParents = 2,
    /// Marking children (is_parent = false)
    MarkingChildren = 3,
    /// Sync completed successfully
    Completed = 4,
}

impl From<u8> for SyncStep {
    fn from(v: u8) -> Self {
        match v {
            1 => SyncStep::WritingDupes,
            2 => SyncStep::MarkingParents,
            3 => SyncStep::MarkingChildren,
            4 => SyncStep::Completed,
            _ => SyncStep::NotStarted,
        }
    }
}

/// Sync progress state - tracks where we are in the sync process
#[derive(Debug, Clone)]
pub struct SyncProgress {
    /// Current step in the sync process
    pub step: SyncStep,
    /// Number of dupes already written
    pub dupes_written: u64,
    /// Total dupes to write
    pub dupes_total: u64,
    /// Number of parents already marked
    pub parents_marked: u64,
    /// Total parents to mark
    pub parents_total: u64,
    /// Number of children already marked
    pub children_marked: u64,
    /// Total children to mark
    pub children_total: u64,
    /// Timestamp when sync started (Unix timestamp)
    pub started_at: u64,
    /// Timestamp when sync completed (Unix timestamp, 0 if not completed)
    pub completed_at: u64,
}

/// Result of building the adjacency side-index from the canonical matches.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdjacencyBuildStats {
    /// Real (non-self) edges indexed.
    pub edges_indexed: u64,
    /// Adjacency entries written (two per real edge).
    pub entries_written: u64,
}

/// Statistics from a connected-edge streaming pass.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConnectedEdgeStats {
    /// Real duplicate edges yielded to the caller.
    pub edges_streamed: u64,
    /// Nodes reached while walking from the seed document ids.
    pub nodes_seen: usize,
    /// Whether the adjacency side-index served the pass.
    pub used_adjacency_index: bool,
}

/// Canonical storage for duplicate matches
pub struct MatchStore {
    db: Database,
    /// Counts full `matches` table scans performed by this handle. Used by tests
    /// to assert the adjacency-backed reader never falls back to a full scan.
    full_scans: AtomicU64,
}

impl MatchStore {
    /// Open or create the matches store
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path.as_ref())
            .with_context(|| format!("Failed to open matches DB at {:?}", path.as_ref()))?;

        // Initialize tables
        let write_txn = begin_quick_repair_write(&db)?;
        {
            let _ = write_txn.open_table(MATCHES_TABLE)?;
            let _ = write_txn.open_table(ADJACENCY_TABLE)?;
            let _ = write_txn.open_table(META_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self {
            db,
            full_scans: AtomicU64::new(0),
        })
    }

    /// Number of full `matches` table scans this handle has performed.
    pub fn full_scan_count(&self) -> u64 {
        self.full_scans.load(Ordering::Relaxed)
    }

    /// Insert a raw match edge.
    ///
    /// New records are keyed by `(child_id, parent_id)` so Union-Find sees every
    /// raw edge. Older stores used `child_id` as the key; readers still support
    /// that format for compatibility.
    /// Returns true if the record was inserted/updated.
    pub fn insert(&self, record: &MatchRecord) -> Result<bool> {
        Ok(self.insert_batch(std::slice::from_ref(record))? > 0)
    }

    /// Insert multiple raw match edges in a batch.
    ///
    /// If an exact `(child_id, parent_id)` edge already exists, only replace it
    /// if the new Jaccard score is higher. The adjacency side-index is kept in
    /// sync within the same transaction so it cannot diverge from the canonical
    /// matches on crash.
    /// Returns the number of records actually inserted/updated.
    pub fn insert_batch(&self, records: &[MatchRecord]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut inserted = 0;
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(MATCHES_TABLE)?;
            let mut adjacency = write_txn.open_table(ADJACENCY_TABLE)?;
            for record in records {
                let key = match_key(record.child_id, record.parent_id);

                let should_insert = if let Some(existing) = table.get(key.as_slice())? {
                    let existing_data = existing.value();
                    if existing_data.len() >= 24 {
                        let existing_jaccard = f64::from_le_bytes(
                            existing_data[16..24].try_into().unwrap_or([0u8; 8]),
                        );
                        record.jaccard_similarity > existing_jaccard
                    } else {
                        true // Corrupted data, replace it
                    }
                } else {
                    true // No existing match
                };

                if should_insert {
                    let value = self.serialize_record(record);
                    table.insert(key.as_slice(), value.as_slice())?;
                    if let Some((from_child, from_parent, av)) = adjacency_entries(record) {
                        adjacency.insert(from_child.as_slice(), av.as_slice())?;
                        adjacency.insert(from_parent.as_slice(), av.as_slice())?;
                    }
                    inserted += 1;
                }
            }
        }
        write_txn.commit()?;

        Ok(inserted)
    }

    /// Get a match record by child_id
    pub fn get(&self, child_id: &Uuid) -> Result<Option<MatchRecord>> {
        let mut best: Option<MatchRecord> = None;
        for record in self.iter()? {
            if record.child_id == *child_id {
                let replace = best
                    .as_ref()
                    .map(|existing| {
                        let existing_self = existing.child_id == existing.parent_id;
                        let record_self = record.child_id == record.parent_id;
                        (existing_self || record.jaccard_similarity > existing.jaccard_similarity)
                            && !record_self
                    })
                    .unwrap_or(true);
                if replace {
                    best = Some(record);
                }
            }
        }
        Ok(best)
    }

    /// Check if a child_id exists
    pub fn contains(&self, child_id: &Uuid) -> Result<bool> {
        Ok(self.get(child_id)?.is_some())
    }

    /// Count total matches
    pub fn count(&self) -> Result<u64> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MATCHES_TABLE)?;
        Ok(table.len()?)
    }

    /// Iterate over all matches
    pub fn iter(&self) -> Result<Vec<MatchRecord>> {
        self.full_scans.fetch_add(1, Ordering::Relaxed);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MATCHES_TABLE)?;

        let mut records = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let child_id = child_id_from_match_key(key.value())?;
            records.push(self.deserialize_record(&child_id, value.value())?);
        }

        Ok(records)
    }

    /// Iterate over only real duplicate edges, excluding self-parent records.
    pub fn iter_real_matches(&self) -> Result<Vec<MatchRecord>> {
        self.full_scans.fetch_add(1, Ordering::Relaxed);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MATCHES_TABLE)?;

        let mut records = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let child_id = child_id_from_match_key(key.value())?;
            let record = self.deserialize_record(&child_id, value.value())?;
            if record.child_id != record.parent_id {
                records.push(record);
            }
        }

        Ok(records)
    }

    /// Load the real duplicate edges in the connected components touched by
    /// `seed_doc_ids`.
    ///
    /// This avoids materializing the entire historical match graph during
    /// daemon incremental sync. The table is scanned until no new endpoints are
    /// discovered; memory remains bounded by the affected components.
    pub fn get_real_edges_connected_to(&self, seed_doc_ids: &[Uuid]) -> Result<Vec<MatchRecord>> {
        let mut records = Vec::new();
        self.visit_real_edges_connected_to(seed_doc_ids, |record| {
            records.push(record.clone());
            Ok(())
        })?;
        Ok(records)
    }

    /// Visit real duplicate edges in the connected components touched by
    /// `seed_doc_ids` without materializing the edge set in memory.
    pub fn visit_real_edges_connected_to<F>(
        &self,
        seed_doc_ids: &[Uuid],
        mut visitor: F,
    ) -> Result<ConnectedEdgeStats>
    where
        F: FnMut(&MatchRecord) -> Result<()>,
    {
        if seed_doc_ids.is_empty() {
            return Ok(ConnectedEdgeStats::default());
        }

        let mut known_docs: HashSet<Uuid> = seed_doc_ids.iter().copied().collect();

        loop {
            let before = known_docs.len();
            self.full_scans.fetch_add(1, Ordering::Relaxed);
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(MATCHES_TABLE)?;

            for entry in table.iter()? {
                let (key, value) = entry?;
                let child_id = child_id_from_match_key(key.value())?;
                let record = self.deserialize_record(&child_id, value.value())?;
                if record.child_id == record.parent_id {
                    continue;
                }

                if known_docs.contains(&record.child_id) || known_docs.contains(&record.parent_id) {
                    known_docs.insert(record.child_id);
                    known_docs.insert(record.parent_id);
                }
            }

            if known_docs.len() == before {
                break;
            }
        }

        let mut edges_streamed = 0;
        self.full_scans.fetch_add(1, Ordering::Relaxed);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MATCHES_TABLE)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let child_id = child_id_from_match_key(key.value())?;
            let record = self.deserialize_record(&child_id, value.value())?;
            if record.child_id == record.parent_id {
                continue;
            }
            if known_docs.contains(&record.child_id) || known_docs.contains(&record.parent_id) {
                visitor(&record)?;
                edges_streamed += 1;
            }
        }

        Ok(ConnectedEdgeStats {
            edges_streamed,
            nodes_seen: known_docs.len(),
            used_adjacency_index: false,
        })
    }

    /// Whether a full adjacency backfill has been recorded as complete. Until it
    /// is, the indexed reader must not be trusted (the index may cover only
    /// edges written since the maintain-on-write path was deployed).
    pub fn is_adjacency_built(&self) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(META_TABLE)?;
        Ok(table
            .get(ADJACENCY_BUILT_KEY)?
            .map(|v| v.value().first().copied() == Some(1))
            .unwrap_or(false))
    }

    fn set_adjacency_built(&self) -> Result<()> {
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(META_TABLE)?;
            let flag = [1u8];
            table.insert(ADJACENCY_BUILT_KEY, flag.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Number of entries in the adjacency index (two per real edge once built).
    pub fn adjacency_entry_count(&self) -> Result<u64> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(ADJACENCY_TABLE)?;
        Ok(table.len()?)
    }

    /// Backfill the adjacency side-index from the canonical matches table.
    ///
    /// Streams the matches table with bounded memory (entries are flushed to the
    /// index in fixed-size transactions, never accumulated in full) and records
    /// completion so the indexed reader becomes trusted. Idempotent and
    /// resumable: existing adjacency entries are left untouched, so retrying an
    /// interrupted backfill only writes missing entries.
    pub fn build_adjacency_index(&self) -> Result<AdjacencyBuildStats> {
        // Adjacency entries buffered before each write transaction. Bounds the
        // builder's memory regardless of how large matches.redb is.
        const FLUSH_EVERY: usize = 100_000;

        let mut buffer: Vec<([u8; 48], [u8; 20])> = Vec::with_capacity(FLUSH_EVERY + 2);
        let mut edges_indexed: u64 = 0;
        let mut entries_written: u64 = 0;

        let read_txn = self.db.begin_read()?;
        {
            let table = read_txn.open_table(MATCHES_TABLE)?;
            for entry in table.iter()? {
                let (key, value) = entry?;
                let child_id = child_id_from_match_key(key.value())?;
                let record = self.deserialize_record(&child_id, value.value())?;
                if let Some((from_child, from_parent, av)) = adjacency_entries(&record) {
                    buffer.push((from_child, av));
                    buffer.push((from_parent, av));
                    edges_indexed += 1;
                    if buffer.len() >= FLUSH_EVERY {
                        entries_written += self.flush_adjacency(&buffer)? as u64;
                        buffer.clear();
                    }
                }
            }
        }
        if !buffer.is_empty() {
            entries_written += self.flush_adjacency(&buffer)? as u64;
        }

        self.set_adjacency_built()?;
        Ok(AdjacencyBuildStats {
            edges_indexed,
            entries_written,
        })
    }

    fn flush_adjacency(&self, entries: &[([u8; 48], [u8; 20])]) -> Result<usize> {
        let mut inserted = 0;
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut adjacency = write_txn.open_table(ADJACENCY_TABLE)?;
            for (k, v) in entries {
                if adjacency.get(k.as_slice())?.is_none() {
                    adjacency.insert(k.as_slice(), v.as_slice())?;
                    inserted += 1;
                }
            }
        }
        write_txn.commit()?;
        Ok(inserted)
    }

    /// Load the real duplicate edges in the connected components touched by
    /// `seed_doc_ids` using the adjacency side-index.
    ///
    /// Walks the component with prefix range scans and point reads instead of
    /// repeatedly scanning the whole matches table. Work is proportional to the
    /// touched component, not the size of matches.redb. The caller must ensure
    /// the index is built (`is_adjacency_built`); `get_real_edges_connected_to_auto`
    /// handles that decision and falls back to the full scan otherwise.
    pub fn get_real_edges_connected_to_indexed(
        &self,
        seed_doc_ids: &[Uuid],
    ) -> Result<Vec<MatchRecord>> {
        let mut records = Vec::new();
        self.visit_real_edges_connected_to_indexed(seed_doc_ids, |record| {
            records.push(record.clone());
            Ok(())
        })?;
        Ok(records)
    }

    /// Visit connected real duplicate edges using the adjacency side-index.
    ///
    /// Each stored edge has two adjacency entries, one per endpoint. The walk
    /// emits the edge only when visiting its lower UUID endpoint, so memory is
    /// bounded by reached nodes rather than reached edges.
    pub fn visit_real_edges_connected_to_indexed<F>(
        &self,
        seed_doc_ids: &[Uuid],
        mut visitor: F,
    ) -> Result<ConnectedEdgeStats>
    where
        F: FnMut(&MatchRecord) -> Result<()>,
    {
        if seed_doc_ids.is_empty() {
            return Ok(ConnectedEdgeStats {
                used_adjacency_index: true,
                ..ConnectedEdgeStats::default()
            });
        }

        let read_txn = self.db.begin_read()?;
        let adjacency = read_txn.open_table(ADJACENCY_TABLE)?;

        let mut visited_nodes: HashSet<Uuid> = seed_doc_ids.iter().copied().collect();
        let mut stack: Vec<Uuid> = visited_nodes.iter().copied().collect();
        let mut edges_streamed = 0;

        while let Some(node) = stack.pop() {
            let node_bytes = node.as_bytes();
            let mut lo = [0u8; 48];
            lo[..16].copy_from_slice(node_bytes);
            let mut hi = [0xffu8; 48];
            hi[..16].copy_from_slice(node_bytes);

            for entry in adjacency.range(lo.as_slice()..=hi.as_slice())? {
                let (key, value) = entry?;
                let key = key.value();
                if key.len() != 48 {
                    continue;
                }
                let child_id = Uuid::from_slice(&key[16..32])?;
                let parent_id = Uuid::from_slice(&key[32..48])?;

                if node == child_id.min(parent_id) {
                    let v = value.value();
                    if v.len() >= 20 {
                        let record = MatchRecord {
                            child_id,
                            parent_id,
                            jaccard_similarity: f64::from_le_bytes(v[0..8].try_into()?),
                            size_difference: i32::from_le_bytes(v[8..12].try_into()?),
                            size_difference_pct: f64::from_le_bytes(v[12..20].try_into()?),
                        };
                        visitor(&record)?;
                        edges_streamed += 1;
                    }
                }

                let other = if child_id == node {
                    parent_id
                } else {
                    child_id
                };
                if visited_nodes.insert(other) {
                    stack.push(other);
                }
            }
        }

        Ok(ConnectedEdgeStats {
            edges_streamed,
            nodes_seen: visited_nodes.len(),
            used_adjacency_index: true,
        })
    }

    /// Load connected edges via the adjacency index when it has been built,
    /// otherwise fall back to the full-scan implementation. Returns the edges
    /// and whether the index path was used.
    pub fn get_real_edges_connected_to_auto(
        &self,
        seed_doc_ids: &[Uuid],
    ) -> Result<(Vec<MatchRecord>, bool)> {
        let mut records = Vec::new();
        let stats = self.visit_real_edges_connected_to_auto(seed_doc_ids, |record| {
            records.push(record.clone());
            Ok(())
        })?;
        Ok((records, stats.used_adjacency_index))
    }

    /// Visit connected edges via the adjacency index when it has been built,
    /// otherwise fall back to the full-scan implementation.
    pub fn visit_real_edges_connected_to_auto<F>(
        &self,
        seed_doc_ids: &[Uuid],
        visitor: F,
    ) -> Result<ConnectedEdgeStats>
    where
        F: FnMut(&MatchRecord) -> Result<()>,
    {
        if self.is_adjacency_built()? {
            self.visit_real_edges_connected_to_indexed(seed_doc_ids, visitor)
        } else {
            self.visit_real_edges_connected_to(seed_doc_ids, visitor)
        }
    }

    /// Get all matches that haven't been synced to PostgreSQL yet
    /// (For future use with sync tracking)
    pub fn get_unsynced(&self) -> Result<Vec<MatchRecord>> {
        // For now, return all - we can add sync tracking later
        self.iter()
    }

    /// Get all matches (alias for iter)
    pub fn get_all(&self) -> Result<Vec<MatchRecord>> {
        self.iter()
    }

    /// Get all real duplicate edges, excluding self-parent records.
    pub fn get_all_real(&self) -> Result<Vec<MatchRecord>> {
        self.iter_real_matches()
    }

    fn serialize_record(&self, record: &MatchRecord) -> Vec<u8> {
        // Format: parent_id (16) + jaccard (8) + size_diff (4) + size_diff_pct (8) = 36 bytes
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(record.parent_id.as_bytes());
        buf.extend_from_slice(&record.jaccard_similarity.to_le_bytes());
        buf.extend_from_slice(&record.size_difference.to_le_bytes());
        buf.extend_from_slice(&record.size_difference_pct.to_le_bytes());
        buf
    }

    fn deserialize_record(&self, child_id: &Uuid, data: &[u8]) -> Result<MatchRecord> {
        if data.len() < 36 {
            anyhow::bail!("Invalid match record data length: {}", data.len());
        }

        let parent_id = Uuid::from_slice(&data[0..16])?;
        let jaccard = f64::from_le_bytes(data[16..24].try_into()?);
        let size_diff = i32::from_le_bytes(data[24..28].try_into()?);
        let size_diff_pct = f64::from_le_bytes(data[28..36].try_into()?);

        Ok(MatchRecord {
            child_id: *child_id,
            parent_id,
            jaccard_similarity: jaccard,
            size_difference: size_diff,
            size_difference_pct: size_diff_pct,
        })
    }
}

/// Sidecar store for filtered documents that should be marked as parents during
/// a later manual sync.
pub struct FilteredParentStore {
    db: Database,
}

impl FilteredParentStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path.as_ref())
            .with_context(|| format!("Failed to open filtered parent DB at {:?}", path.as_ref()))?;

        let write_txn = begin_quick_repair_write(&db)?;
        {
            let _ = write_txn.open_table(FILTERED_PARENTS_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    pub fn insert_batch(&self, doc_ids: &[Uuid]) -> Result<usize> {
        if doc_ids.is_empty() {
            return Ok(0);
        }

        let write_txn = begin_quick_repair_write(&self.db)?;
        let mut inserted = 0usize;
        {
            let mut table = write_txn.open_table(FILTERED_PARENTS_TABLE)?;
            for doc_id in doc_ids {
                let empty: &[u8] = &[];
                if table.insert(doc_id.as_bytes().as_slice(), empty)?.is_none() {
                    inserted += 1;
                }
            }
        }
        write_txn.commit()?;

        Ok(inserted)
    }

    pub fn iter(&self) -> Result<Vec<Uuid>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILTERED_PARENTS_TABLE)?;

        let mut ids = Vec::with_capacity(table.len()? as usize);
        for entry in table.iter()? {
            let (key, _) = entry?;
            ids.push(Uuid::from_slice(key.value())?);
        }
        Ok(ids)
    }

    pub fn count(&self) -> Result<u64> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(FILTERED_PARENTS_TABLE)?;
        Ok(table.len()?)
    }
}

/// State store for resumable sync progress.
pub struct StateStore {
    db: Database,
}

impl StateStore {
    /// Open or create the sync state store.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path.as_ref())
            .with_context(|| format!("Failed to open state DB at {:?}", path.as_ref()))?;

        // Initialize tables
        let write_txn = begin_quick_repair_write(&db)?;
        {
            let _ = write_txn.open_table(META_TABLE)?;
            let _ = write_txn.open_table(SYNC_STATE_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    // =========================================================================
    // Sync Progress Tracking
    // =========================================================================

    /// Get current sync progress
    pub fn get_sync_progress(&self) -> Result<SyncProgress> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(SYNC_STATE_TABLE)?;

        let get_u64 = |key: &str| -> u64 {
            table
                .get(key)
                .ok()
                .flatten()
                .and_then(|v| {
                    let bytes: [u8; 8] = v.value().try_into().ok()?;
                    Some(u64::from_le_bytes(bytes))
                })
                .unwrap_or(0)
        };

        let step = SyncStep::from(get_u64("step") as u8);

        Ok(SyncProgress {
            step,
            dupes_written: get_u64("dupes_written"),
            dupes_total: get_u64("dupes_total"),
            parents_marked: get_u64("parents_marked"),
            parents_total: get_u64("parents_total"),
            children_marked: get_u64("children_marked"),
            children_total: get_u64("children_total"),
            started_at: get_u64("started_at"),
            completed_at: get_u64("completed_at"),
        })
    }

    /// Set sync progress atomically
    pub fn set_sync_progress(&self, progress: &SyncProgress) -> Result<()> {
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(SYNC_STATE_TABLE)?;

            let set_u64 = |t: &mut redb::Table<&str, &[u8]>, key: &str, val: u64| -> Result<()> {
                t.insert(key, val.to_le_bytes().as_slice())?;
                Ok(())
            };

            set_u64(&mut table, "step", progress.step as u64)?;
            set_u64(&mut table, "dupes_written", progress.dupes_written)?;
            set_u64(&mut table, "dupes_total", progress.dupes_total)?;
            set_u64(&mut table, "parents_marked", progress.parents_marked)?;
            set_u64(&mut table, "parents_total", progress.parents_total)?;
            set_u64(&mut table, "children_marked", progress.children_marked)?;
            set_u64(&mut table, "children_total", progress.children_total)?;
            set_u64(&mut table, "started_at", progress.started_at)?;
            set_u64(&mut table, "completed_at", progress.completed_at)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Update a single sync progress field (for incremental updates)
    pub fn update_sync_field(&self, field: &str, value: u64) -> Result<()> {
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(SYNC_STATE_TABLE)?;
            table.insert(field, value.to_le_bytes().as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Reset sync progress (for fresh start)
    pub fn reset_sync_progress(&self) -> Result<()> {
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(SYNC_STATE_TABLE)?;
            // Delete all keys by iterating and removing
            let keys: Vec<String> = table
                .iter()?
                .map(|r| r.map(|(k, _)| k.value().to_string()))
                .collect::<Result<_, _>>()?;
            for key in keys {
                table.remove(key.as_str())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Check if sync is in progress (started but not completed)
    pub fn is_sync_in_progress(&self) -> Result<bool> {
        let progress = self.get_sync_progress()?;
        Ok(progress.step != SyncStep::NotStarted && progress.step != SyncStep::Completed)
    }

    /// Check if sync is completed
    pub fn is_sync_completed(&self) -> Result<bool> {
        let progress = self.get_sync_progress()?;
        Ok(progress.step == SyncStep::Completed)
    }
}

/// Combined storage manager for a dataset
pub struct DatasetStorage {
    pub matches: MatchStore,
    pub base_path: std::path::PathBuf,
}

impl DatasetStorage {
    /// Open or create storage for a dataset
    pub fn open<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base = base_path.as_ref();
        std::fs::create_dir_all(base)?;

        let matches_path = base.join("matches.redb");
        Ok(Self {
            matches: MatchStore::open(&matches_path)?,
            base_path: base.to_path_buf(),
        })
    }

    /// Open storage for a dataset by UUID
    pub fn open_for_dataset(data_dir: &Path, dataset_id: &Uuid) -> Result<Self> {
        let path = data_dir.join(format!("dataset_{}", dataset_id));
        Self::open(path)
    }

    /// Get summary statistics
    pub fn match_count(&self) -> Result<u64> {
        self.matches.count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_match_store() {
        let dir = tempdir().unwrap();
        let store = MatchStore::open(dir.path().join("matches.redb")).unwrap();

        let child_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();

        let record = MatchRecord {
            child_id,
            parent_id,
            jaccard_similarity: 0.85,
            size_difference: 100,
            size_difference_pct: 0.1,
        };

        assert!(store.insert(&record).unwrap()); // First insert succeeds

        let retrieved = store.get(&child_id).unwrap().unwrap();
        assert_eq!(retrieved.parent_id, parent_id);
        assert!((retrieved.jaccard_similarity - 0.85).abs() < 0.001);
        assert_eq!(retrieved.size_difference, 100);
    }

    #[test]
    fn test_match_store_preserves_raw_edges_and_get_returns_best() {
        let dir = tempdir().unwrap();
        let store = MatchStore::open(dir.path().join("matches.redb")).unwrap();

        let child_id = Uuid::new_v4();
        let parent_a = Uuid::new_v4();
        let parent_b = Uuid::new_v4();
        let parent_c = Uuid::new_v4();

        // Insert first match with jaccard 0.80
        let record_a = MatchRecord {
            child_id,
            parent_id: parent_a,
            jaccard_similarity: 0.80,
            size_difference: 100,
            size_difference_pct: 0.1,
        };
        assert!(store.insert(&record_a).unwrap());

        // Insert another raw edge for the same child. It must be preserved so
        // Union-Find sees the full graph.
        let record_b = MatchRecord {
            child_id,
            parent_id: parent_b,
            jaccard_similarity: 0.70,
            size_difference: 50,
            size_difference_pct: 0.05,
        };
        assert!(store.insert(&record_b).unwrap());
        assert_eq!(store.count().unwrap(), 2);

        // get(child) returns the best edge for inspection/back-compat only.
        let retrieved = store.get(&child_id).unwrap().unwrap();
        assert_eq!(retrieved.parent_id, parent_a);
        assert!((retrieved.jaccard_similarity - 0.80).abs() < 0.001);

        // Insert better match with jaccard 0.95.
        let record_c = MatchRecord {
            child_id,
            parent_id: parent_c,
            jaccard_similarity: 0.95,
            size_difference: 20,
            size_difference_pct: 0.02,
        };
        assert!(store.insert(&record_c).unwrap());
        assert_eq!(store.count().unwrap(), 3);

        // Verify get() now reports parent_c with better jaccard.
        let retrieved = store.get(&child_id).unwrap().unwrap();
        assert_eq!(retrieved.parent_id, parent_c);
        assert!((retrieved.jaccard_similarity - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_match_store_real_match_replaces_legacy_self_parent() {
        let dir = tempdir().unwrap();
        let store = MatchStore::open(dir.path().join("matches.redb")).unwrap();

        let child_id = Uuid::new_v4();
        let real_parent = Uuid::new_v4();

        let legacy_self_parent = MatchRecord {
            child_id,
            parent_id: child_id,
            jaccard_similarity: 1.0,
            size_difference: 0,
            size_difference_pct: 0.0,
        };
        assert!(store.insert(&legacy_self_parent).unwrap());

        let real_match = MatchRecord {
            child_id,
            parent_id: real_parent,
            jaccard_similarity: 0.85,
            size_difference: 100,
            size_difference_pct: 0.1,
        };
        assert!(store.insert(&real_match).unwrap());

        let retrieved = store.get(&child_id).unwrap().unwrap();
        assert_eq!(retrieved.parent_id, real_parent);
        assert!((retrieved.jaccard_similarity - 0.85).abs() < 0.001);
    }

    #[test]
    fn test_match_store_loads_only_connected_edges() {
        let dir = tempdir().unwrap();
        let store = MatchStore::open(dir.path().join("matches.redb")).unwrap();

        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);
        let x = Uuid::from_u128(10);
        let y = Uuid::from_u128(11);

        let mk = |child_id, parent_id| MatchRecord {
            child_id,
            parent_id,
            jaccard_similarity: 0.9,
            size_difference: 1,
            size_difference_pct: 0.01,
        };

        store.insert_batch(&[mk(a, b), mk(b, c), mk(x, y)]).unwrap();

        let connected = store.get_real_edges_connected_to(&[a]).unwrap();
        let edges: std::collections::HashSet<_> = connected
            .iter()
            .map(|m| (m.child_id, m.parent_id))
            .collect();

        assert_eq!(edges.len(), 2);
        assert!(edges.contains(&(a, b)));
        assert!(edges.contains(&(b, c)));
        assert!(!edges.contains(&(x, y)));
    }

    #[test]
    fn test_filtered_parent_store_round_trip() {
        let dir = tempdir().unwrap();
        let store = FilteredParentStore::open(dir.path().join("filtered_parents.redb")).unwrap();
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);

        assert_eq!(store.insert_batch(&[a, b, a]).unwrap(), 2);
        assert_eq!(store.count().unwrap(), 2);

        let ids: std::collections::HashSet<_> = store.iter().unwrap().into_iter().collect();
        assert_eq!(ids, [a, b].into_iter().collect());
    }

    #[test]
    fn test_dataset_storage() {
        let dir = tempdir().unwrap();
        let storage = DatasetStorage::open(dir.path()).unwrap();

        let child_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();

        let record = MatchRecord {
            child_id,
            parent_id,
            jaccard_similarity: 0.9,
            size_difference: 50,
            size_difference_pct: 0.05,
        };

        assert!(storage.matches.insert(&record).unwrap());
        assert_eq!(storage.match_count().unwrap(), 1);
        assert!(storage.matches.contains(&child_id).unwrap());
    }

    /// CRITICAL TEST: After reset, reading sync progress must return NotStarted.
    ///
    /// This catches the bug where we call reset_sync_progress() but the caller
    /// still holds a stale copy of the old progress with step=Completed.
    /// The fix requires re-reading after reset.
    #[test]
    fn test_sync_progress_reset_must_be_readable_as_not_started() {
        let dir = tempdir().unwrap();
        let store = StateStore::open(dir.path().join("state.redb")).unwrap();

        // Set progress to Completed
        let progress = SyncProgress {
            step: SyncStep::Completed,
            dupes_written: 1000,
            dupes_total: 1000,
            parents_marked: 500,
            parents_total: 500,
            children_marked: 500,
            children_total: 500,
            started_at: 12345,
            completed_at: 12400,
        };
        store.set_sync_progress(&progress).unwrap();

        // Verify it's Completed
        let read1 = store.get_sync_progress().unwrap();
        assert_eq!(read1.step, SyncStep::Completed);

        // Reset
        store.reset_sync_progress().unwrap();

        // CRITICAL: Reading AFTER reset must return NotStarted
        let read2 = store.get_sync_progress().unwrap();
        assert_eq!(
            read2.step,
            SyncStep::NotStarted,
            "After reset, sync progress must read as NotStarted"
        );
        assert_eq!(read2.dupes_written, 0);
        assert_eq!(read2.parents_marked, 0);
        assert_eq!(read2.children_marked, 0);
    }

    /// Test that sync progress survives re-open (for resume functionality)
    #[test]
    fn test_sync_progress_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.redb");

        // Write progress
        {
            let store = StateStore::open(&path).unwrap();
            let progress = SyncProgress {
                step: SyncStep::MarkingChildren,
                dupes_written: 1000,
                dupes_total: 1000,
                parents_marked: 500,
                parents_total: 500,
                children_marked: 250,
                children_total: 500,
                started_at: 12345,
                completed_at: 0,
            };
            store.set_sync_progress(&progress).unwrap();
        }

        // Re-open and verify
        {
            let store = StateStore::open(&path).unwrap();
            let read = store.get_sync_progress().unwrap();
            assert_eq!(read.step, SyncStep::MarkingChildren);
            assert_eq!(read.children_marked, 250);
            assert_eq!(read.children_total, 500);
        }
    }
}
