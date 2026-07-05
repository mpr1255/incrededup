//! Disk-based deduplication with resumable state.
//!
//! This module provides parallel deduplication that reads from an existing LSH index
//! (lsh.redb) and outputs matches to a separate file. No database connection required.
//!
//! State is stored in state.redb (consistent with the rest of the codebase).

use anyhow::{Context, Result};

/// Log current RSS memory usage (Linux only)
#[cfg(target_os = "linux")]
fn log_memory(label: &str) {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(line) = status.lines().find(|l| l.starts_with("VmRSS:")) {
            if let Some(kb_str) = line.split_whitespace().nth(1) {
                if let Ok(kb) = kb_str.parse::<f64>() {
                    let mb = kb / 1024.0;
                    tracing::info!("[MEMORY] {}: {:.1} MB ({:.2} GB)", label, mb, mb / 1024.0);
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn log_memory(_label: &str) {}
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use redb::{Database, ReadOnlyTable, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

use crate::minhash::{calculate_band_hash, NUM_BANDS, ROWS_PER_BAND};

// Table definitions (must match lsh/mod.rs)
const BAND_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("bands");
const SIGNATURE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("signatures");

// Match storage table
const MATCHES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("matches");
const ADJACENCY_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("adjacency");

// State storage table (for tracking processed docs during Phase 2)
const PHASE2_STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("phase2_processed");
// Metadata table for stats
const PHASE2_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("phase2_meta");

fn begin_quick_repair_write(db: &Database) -> Result<redb::WriteTransaction> {
    let mut write_txn = db.begin_write()?;
    write_txn.set_quick_repair(true);
    Ok(write_txn)
}

fn lock_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!("Recovering poisoned Phase 2 mutex: {}", name);
            poisoned.into_inner()
        }
    }
}

/// Document entry from LSH index
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DocumentEntry {
    signature: Vec<u32>,
    content_len: usize,
}

/// Match record to store
#[derive(Debug, Clone)]
pub struct MatchRecord {
    pub child_id: Uuid,
    pub parent_id: Uuid,
    pub jaccard_similarity: f64,
    #[allow(dead_code)]
    pub size_difference: i32,
    #[allow(dead_code)]
    pub size_difference_pct: f64,
}

impl MatchRecord {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(self.parent_id.as_bytes());
        buf.extend_from_slice(&self.jaccard_similarity.to_le_bytes());
        buf.extend_from_slice(&self.size_difference.to_le_bytes());
        buf.extend_from_slice(&self.size_difference_pct.to_le_bytes());
        buf
    }

    /// Deserialize a match record from bytes (only extracts jaccard for comparison)
    fn deserialize_jaccard(data: &[u8]) -> Option<f64> {
        if data.len() >= 24 {
            // Skip parent_id (16 bytes), read jaccard (8 bytes)
            let jaccard_bytes: [u8; 8] = data[16..24].try_into().ok()?;
            Some(f64::from_le_bytes(jaccard_bytes))
        } else {
            None
        }
    }
}

fn match_key(child_id: Uuid, parent_id: Uuid) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(child_id.as_bytes());
    key[16..].copy_from_slice(parent_id.as_bytes());
    key
}

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

fn adjacency_entries(record: &MatchRecord) -> Option<([u8; 48], [u8; 48], [u8; 20])> {
    if record.child_id == record.parent_id {
        return None;
    }
    let value = adjacency_value(record);
    let from_child = adjacency_key(&record.child_id, &record.child_id, &record.parent_id);
    let from_parent = adjacency_key(&record.parent_id, &record.child_id, &record.parent_id);
    Some((from_child, from_parent, value))
}

#[derive(Default)]
struct PendingPhase2Writes {
    matches: Vec<MatchRecord>,
    processed_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Copy)]
struct Phase2FlushSnapshot {
    duplicates_found: usize,
    candidates_checked: usize,
    processed_this_run: usize,
    remaining_docs: usize,
}

/// Metadata for Phase 2 state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase2Metadata {
    duplicates_found: usize,
    candidates_checked: usize,
    last_saved: String,
}

/// State manager for resumable Phase 2 deduplication using redb (state.redb)
pub struct Phase2StateStore {
    db: Database,
}

impl Phase2StateStore {
    /// Open or create state.redb for Phase 2 state tracking
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path.as_ref())
            .with_context(|| format!("Failed to create state DB at {:?}", path.as_ref()))?;

        // Initialize tables
        let write_txn = begin_quick_repair_write(&db)?;
        {
            let _ = write_txn.open_table(PHASE2_STATE_TABLE)?;
            let _ = write_txn.open_table(PHASE2_META_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    /// Check if a document has been processed
    pub fn is_processed(&self, doc_id: &Uuid) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PHASE2_STATE_TABLE)?;
        Ok(table.get(doc_id.as_bytes().as_slice())?.is_some())
    }

    /// Get set of all processed document IDs
    pub fn get_processed_set(&self) -> Result<HashSet<Uuid>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PHASE2_STATE_TABLE)?;

        let mut set = HashSet::new();
        for item in table.iter()? {
            let (key, _) = item?;
            if let Ok(uuid) = Uuid::from_slice(key.value()) {
                set.insert(uuid);
            }
        }
        Ok(set)
    }

    /// Get count of processed documents
    pub fn processed_count(&self) -> Result<u64> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PHASE2_STATE_TABLE)?;
        Ok(table.len()?)
    }

    /// Mark documents as processed (batch)
    pub fn mark_processed_batch(&self, doc_ids: &[Uuid]) -> Result<()> {
        if doc_ids.is_empty() {
            return Ok(());
        }

        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(PHASE2_STATE_TABLE)?;
            let marker: &[u8] = &[1]; // Simple marker to indicate processed
            for doc_id in doc_ids {
                table.insert(doc_id.as_bytes().as_slice(), marker)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Load metadata
    pub fn load_metadata(&self) -> Result<Option<Phase2Metadata>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PHASE2_META_TABLE)?;

        if let Some(data) = table.get("metadata")? {
            let meta: Phase2Metadata = bincode::deserialize(data.value())?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    /// Save metadata
    pub fn save_metadata(&self, duplicates_found: usize, candidates_checked: usize) -> Result<()> {
        let meta = Phase2Metadata {
            duplicates_found,
            candidates_checked,
            last_saved: chrono::Utc::now().to_rfc3339(),
        };

        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(PHASE2_META_TABLE)?;
            let data = bincode::serialize(&meta)?;
            table.insert("metadata", data.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Remove specific documents from the processed set.
    /// This is needed when documents are being reprocessed (e.g., after is_parent was reset).
    pub fn remove_from_processed(&self, doc_ids: &[Uuid]) -> Result<usize> {
        if doc_ids.is_empty() {
            return Ok(0);
        }

        let mut removed = 0;
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            let mut table = write_txn.open_table(PHASE2_STATE_TABLE)?;
            for doc_id in doc_ids {
                if table.remove(doc_id.as_bytes().as_slice())?.is_some() {
                    removed += 1;
                }
            }
        }
        write_txn.commit()?;
        Ok(removed)
    }

    /// Clear all state (for fresh start)
    pub fn clear(&self) -> Result<()> {
        let write_txn = begin_quick_repair_write(&self.db)?;
        {
            // Drop and recreate tables
            let mut table = write_txn.open_table(PHASE2_STATE_TABLE)?;
            // Clear by iterating (redb doesn't have truncate)
            let keys: Result<Vec<Vec<u8>>> = table
                .iter()?
                .map(|item| {
                    let (k, _) = item?;
                    Ok(k.value().to_vec())
                })
                .collect();
            let keys = keys?;
            for key in keys {
                table.remove(key.as_slice())?;
            }

            let mut meta_table = write_txn.open_table(PHASE2_META_TABLE)?;
            let _ = meta_table.remove("metadata");
        }
        write_txn.commit()?;
        Ok(())
    }
}

/// Calculate Jaccard similarity from two MinHash signatures
fn jaccard_from_signatures(sig1: &[u32], sig2: &[u32]) -> f64 {
    crate::minhash::jaccard_from_signatures(sig1, sig2)
}

/// Check if two sizes are within threshold
fn size_within_threshold(size1: i32, size2: i32, threshold: f64) -> bool {
    if size1 == 0 || size2 == 0 {
        return true;
    }
    let (smaller, larger) = if size1 < size2 {
        (size1, size2)
    } else {
        (size2, size1)
    };
    let diff = (larger - smaller) as f64 / larger as f64;
    diff <= threshold
}

/// Statistics from disk-based deduplication
#[derive(Debug)]
pub struct DiskDedupeStats {
    pub total_documents: usize,
    pub duplicates_found: usize,
    pub candidates_checked: usize,
    pub duration_secs: f64,
}

/// Options for `run_disk_dedupe_with_options`.
#[derive(Debug)]
pub struct DiskDedupeRunOptions {
    pub num_workers: usize,
    pub threshold: f64,
    pub size_diff_threshold: f64,
    pub fresh: bool,
    pub new_doc_ids: Option<Vec<Uuid>>,
    pub max_matches_per_doc: Option<usize>,
}

/// Phase 2: Parallel disk-based deduplicator for finding duplicate pairs.
///
/// This struct handles Phase 2 of the deduplication pipeline: reading from an existing
/// LSH index (built by Phase 1) and performing parallel comparisons to find duplicate
/// document pairs. Results are written to `matches.redb`.
///
/// # Note
/// This is distinct from `dedupe::IndexBuilder` (Phase 1) which handles
/// the streaming index building phase. `IndexBuilder` was previously named
/// `DiskDeduplicator` but was renamed to clarify its role.
pub struct DiskDeduplicator {
    lsh_db: Database,
    output_db: Database,
    num_bands: usize,
    rows_per_band: usize,
    threshold: f64,
    size_diff_threshold: f64,
    max_matches_per_doc: Option<usize>,
}

impl DiskDeduplicator {
    pub fn open<P: AsRef<Path>>(lsh_path: P, output_path: P) -> Result<Self> {
        let lsh_db = Database::open(lsh_path.as_ref())
            .with_context(|| format!("Failed to open LSH DB at {:?}", lsh_path.as_ref()))?;

        let output_db = Database::create(output_path.as_ref())
            .with_context(|| format!("Failed to create output DB at {:?}", output_path.as_ref()))?;

        // Initialize output table
        let write_txn = begin_quick_repair_write(&output_db)?;
        {
            let _ = write_txn.open_table(MATCHES_TABLE)?;
            let _ = write_txn.open_table(ADJACENCY_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self {
            lsh_db,
            output_db,
            num_bands: NUM_BANDS,
            rows_per_band: ROWS_PER_BAND,
            threshold: 0.8,
            size_diff_threshold: 0.3,
            max_matches_per_doc: None,
        })
    }

    pub fn with_threshold(mut self, threshold: f64) -> Self {
        self.threshold = threshold;
        self
    }

    pub fn with_size_diff_threshold(mut self, threshold: f64) -> Self {
        self.size_diff_threshold = threshold;
        self
    }

    pub fn with_max_matches_per_doc(mut self, max_matches_per_doc: Option<usize>) -> Self {
        self.max_matches_per_doc = max_matches_per_doc;
        self
    }

    /// Get all document IDs from the signatures table
    fn get_all_doc_ids(&self) -> Result<Vec<Uuid>> {
        let read_txn = self.lsh_db.begin_read()?;
        let sig_table = read_txn.open_table(SIGNATURE_TABLE)?;

        let mut doc_ids = Vec::new();
        for item in sig_table.iter()? {
            let (key, _) = item?;
            if let Ok(uuid) = Uuid::parse_str(key.value()) {
                doc_ids.push(uuid);
            }
        }
        Ok(doc_ids)
    }

    fn get_document_from_table(
        sig_table: &ReadOnlyTable<&str, &[u8]>,
        doc_id: &Uuid,
    ) -> Result<Option<DocumentEntry>> {
        if let Some(entry_bytes) = sig_table.get(doc_id.to_string().as_str())? {
            let entry: DocumentEntry = bincode::deserialize(entry_bytes.value())?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    fn query_from_table(
        &self,
        band_table: &ReadOnlyTable<&str, &[u8]>,
        signature: &[u32],
    ) -> Result<Vec<Uuid>> {
        let mut candidates = Vec::new();

        for band_idx in 0..self.num_bands {
            let start = band_idx * self.rows_per_band;
            let end = start + self.rows_per_band;
            let band_hash = calculate_band_hash(&signature[start..end]);
            let key = format!("{}:{}", band_idx, band_hash);

            if let Some(doc_ids_bytes) = band_table.get(key.as_str())? {
                let doc_ids: Vec<Uuid> = bincode::deserialize(doc_ids_bytes.value())?;
                candidates.extend(doc_ids);
            }
        }

        // Deduplicate
        candidates.sort();
        candidates.dedup();
        Ok(candidates)
    }

    /// Write a batch of raw match edges to disk.
    ///
    /// Records are keyed by `(child_id, parent_id)` so transitivity resolution
    /// sees every raw edge. If the exact edge already exists, keep the higher
    /// Jaccard score.
    fn write_matches_batch(&self, matches: &[MatchRecord]) -> Result<usize> {
        if matches.is_empty() {
            return Ok(0);
        }

        let mut written = 0;
        let write_txn = begin_quick_repair_write(&self.output_db)?;
        {
            let mut table = write_txn.open_table(MATCHES_TABLE)?;
            let mut adjacency = write_txn.open_table(ADJACENCY_TABLE)?;
            for record in matches {
                let key = match_key(record.child_id, record.parent_id);

                // Check if this exact edge already exists.
                let should_insert = if let Some(existing) = table.get(key.as_slice())? {
                    let existing_data = existing.value();
                    let existing_jaccard =
                        MatchRecord::deserialize_jaccard(existing_data).unwrap_or(0.0);

                    record.jaccard_similarity > existing_jaccard
                } else {
                    true
                };

                if should_insert {
                    let value = record.serialize();
                    table.insert(key.as_slice(), value.as_slice())?;
                    if let Some((from_child, from_parent, av)) = adjacency_entries(record) {
                        adjacency.insert(from_child.as_slice(), av.as_slice())?;
                        adjacency.insert(from_parent.as_slice(), av.as_slice())?;
                    }
                    written += 1;
                }
            }
        }
        write_txn.commit()?;

        Ok(written)
    }

    fn flush_pending_writes(
        &self,
        pending_writes: &Arc<Mutex<PendingPhase2Writes>>,
        state_store: &Phase2StateStore,
        total_written: &AtomicUsize,
        snapshot: Phase2FlushSnapshot,
    ) -> Result<bool> {
        let (matches_to_write, ids_to_save) = {
            let mut pending = lock_recover(pending_writes, "pending_writes");
            if pending.matches.is_empty() && pending.processed_ids.is_empty() {
                return Ok(false);
            }
            (
                std::mem::take(&mut pending.matches),
                std::mem::take(&mut pending.processed_ids),
            )
        };

        let written = self.write_matches_batch(&matches_to_write)?;
        total_written.fetch_add(written, Ordering::Relaxed);

        state_store.mark_processed_batch(&ids_to_save)?;
        state_store.save_metadata(snapshot.duplicates_found, snapshot.candidates_checked)?;

        tracing::info!(
            "Phase 2 checkpoint: {}/{} docs processed ({:.1}%), {} duplicates found, {} candidates checked",
            snapshot.processed_this_run,
            snapshot.remaining_docs,
            (snapshot.processed_this_run as f64 / snapshot.remaining_docs as f64) * 100.0,
            snapshot.duplicates_found,
            snapshot.candidates_checked
        );

        Ok(true)
    }

    /// Count current matches in output
    pub fn count_matches(&self) -> Result<u64> {
        let read_txn = self.output_db.begin_read()?;
        let table = read_txn.open_table(MATCHES_TABLE)?;
        Ok(table.len()?)
    }

    /// Run parallel deduplication with resumable state.
    ///
    /// If `doc_ids_to_process` is provided (incremental mode), only those documents
    /// will be processed. Otherwise, all documents in the index are processed.
    ///
    /// Matches are written directly to disk (matches.redb) and NOT kept in memory.
    /// This is critical for large datasets to avoid OOM.
    pub fn run(
        &self,
        num_workers: usize,
        state_path: &Path,
        resume: bool,
        doc_ids_to_process: Option<Vec<Uuid>>,
    ) -> Result<DiskDedupeStats> {
        let start = std::time::Instant::now();

        // Get document IDs to process (either incremental or all)
        let (all_doc_ids, total_docs, is_incremental) = if let Some(ids) = doc_ids_to_process {
            let len = ids.len();
            tracing::info!("Using incremental doc list: {} new docs", len);
            (ids, len, true)
        } else {
            tracing::info!("Loading ALL document IDs from index...");
            let ids = self.get_all_doc_ids()?;
            let len = ids.len();
            tracing::info!("Found {} documents", len);
            (ids, len, false)
        };

        // Open or create state store (state.redb)
        let state_store = Phase2StateStore::open(state_path)?;

        // In incremental mode, remove new docs from processed set first.
        // This handles the case where a doc was previously processed but is now
        // being requested again (e.g., after is_parent was reset in the database).
        if is_incremental && resume {
            let removed = state_store.remove_from_processed(&all_doc_ids)?;
            if removed > 0 {
                tracing::info!(
                    "Removed {} docs from processed set for reprocessing",
                    removed
                );
            }
        }

        // Load existing state if resuming
        let (initial_dupes, initial_cands) = if resume {
            if let Some(meta) = state_store.load_metadata()? {
                let processed_count = state_store.processed_count()?;
                tracing::info!(
                    "Resuming from checkpoint: {} docs already processed",
                    processed_count
                );
                tracing::info!("  Duplicates found so far: {}", meta.duplicates_found);
                tracing::info!("  Last saved: {}", meta.last_saved);
                (meta.duplicates_found, meta.candidates_checked)
            } else {
                tracing::info!("No checkpoint found, starting fresh");
                (0, 0)
            }
        } else {
            // Fresh start - clear existing state
            state_store.clear()?;
            (0, 0)
        };

        // Filter out already processed docs
        let processed_set = state_store.get_processed_set()?;
        let doc_ids: Vec<Uuid> = all_doc_ids
            .into_iter()
            .filter(|id| !processed_set.contains(id))
            .collect();
        let doc_id_set: Arc<HashSet<Uuid>> = Arc::new(doc_ids.iter().copied().collect());

        let remaining_docs = doc_ids.len();
        let already_done = processed_set.len();
        tracing::info!(
            "Documents to process: {} (skipping {} already done)",
            remaining_docs,
            already_done
        );

        if remaining_docs == 0 {
            tracing::info!("All documents already processed!");
            return Ok(DiskDedupeStats {
                total_documents: total_docs,
                duplicates_found: initial_dupes,
                candidates_checked: initial_cands,
                duration_secs: start.elapsed().as_secs_f64(),
            });
        }

        // Progress tracking
        let pb = ProgressBar::new(remaining_docs as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.yellow/blue} {pos}/{len} ({per_sec}) Deduplicating... ETA: {eta}")
                .unwrap(),
        );

        let candidates_checked = AtomicUsize::new(initial_cands);
        let duplicates_found = AtomicUsize::new(initial_dupes);
        let processed = AtomicUsize::new(0);

        // NOTE: We do NOT accumulate matches in memory - they go directly to disk
        // This is critical for large datasets to avoid OOM

        // Batch buffer for match writes and the exact doc IDs they acknowledge.
        let pending_writes: Arc<Mutex<PendingPhase2Writes>> =
            Arc::new(Mutex::new(PendingPhase2Writes::default()));
        let total_written = AtomicUsize::new(0);
        let batch_size = 10000;
        let state_save_interval = 10000; // Log progress every 10k docs

        // Use a per-run thread pool so daemon runs can honor changed worker counts.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_workers)
            .build()
            .context("Failed to build Phase 2 thread pool")?;

        // State store wrapper (shared via Arc for saving)
        let state_store = Arc::new(state_store);
        let state_store_clone = Arc::clone(&state_store);

        // Error counters for visibility into failures
        let doc_fetch_errors = AtomicUsize::new(0);
        let query_errors = AtomicUsize::new(0);
        let candidate_fetch_errors = AtomicUsize::new(0);
        let match_write_failed = AtomicBool::new(false);
        let state_save_failed = AtomicBool::new(false);
        let phase2_failed = AtomicBool::new(false);

        // Log that we're starting parallel processing
        tracing::info!(
            "Starting Phase 2 parallel processing: {} docs with {} workers",
            remaining_docs,
            num_workers
        );

        // Process documents in parallel
        pool.install(|| {
            doc_ids.par_iter().for_each(|&doc_id| {
                if phase2_failed.load(Ordering::Relaxed) {
                    pb.inc(1);
                    return;
                }

                let read_txn = match self.lsh_db.begin_read() {
                    Ok(txn) => txn,
                    Err(_) => {
                        doc_fetch_errors.fetch_add(1, Ordering::Relaxed);
                        phase2_failed.store(true, Ordering::Relaxed);
                        pb.inc(1);
                        return;
                    }
                };
                let sig_table = match read_txn.open_table(SIGNATURE_TABLE) {
                    Ok(table) => table,
                    Err(_) => {
                        doc_fetch_errors.fetch_add(1, Ordering::Relaxed);
                        phase2_failed.store(true, Ordering::Relaxed);
                        pb.inc(1);
                        return;
                    }
                };
                let band_table = match read_txn.open_table(BAND_TABLE) {
                    Ok(table) => table,
                    Err(_) => {
                        query_errors.fetch_add(1, Ordering::Relaxed);
                        phase2_failed.store(true, Ordering::Relaxed);
                        pb.inc(1);
                        return;
                    }
                };

                // Get document from disk
                let doc = match Self::get_document_from_table(&sig_table, &doc_id) {
                    Ok(Some(d)) => d,
                    Ok(None) | Err(_) => {
                        doc_fetch_errors.fetch_add(1, Ordering::Relaxed);
                        phase2_failed.store(true, Ordering::Relaxed);
                        pb.inc(1);
                        return;
                    }
                };

                // Query for candidates from disk
                let candidates = match self.query_from_table(&band_table, &doc.signature) {
                    Ok(c) => c,
                    Err(_) => {
                        query_errors.fetch_add(1, Ordering::Relaxed);
                        phase2_failed.store(true, Ordering::Relaxed);
                        pb.inc(1);
                        return;
                    }
                };

                candidates_checked.fetch_add(candidates.len(), Ordering::Relaxed);

                let mut local_matches = Vec::new();

                for cand_id in candidates {
                    // Skip self
                    if cand_id == doc_id {
                        continue;
                    }

                    // In full mode both sides are in doc_id_set, so process
                    // each pair once. In incremental mode, historical
                    // candidates are not in doc_id_set and must still be
                    // compared regardless of UUID ordering.
                    if doc_id >= cand_id && doc_id_set.contains(&cand_id) {
                        continue;
                    }

                    // Get candidate from disk
                    let cand = match Self::get_document_from_table(&sig_table, &cand_id) {
                        Ok(Some(c)) => c,
                        Ok(None) | Err(_) => {
                            candidate_fetch_errors.fetch_add(1, Ordering::Relaxed);
                            phase2_failed.store(true, Ordering::Relaxed);
                            pb.inc(1);
                            return;
                        }
                    };

                    // Size filter
                    if !size_within_threshold(
                        doc.content_len as i32,
                        cand.content_len as i32,
                        self.size_diff_threshold,
                    ) {
                        continue;
                    }

                    // Jaccard similarity check
                    let jaccard = jaccard_from_signatures(&doc.signature, &cand.signature);

                    if jaccard >= self.threshold {
                        // Larger document is the child
                        let (child_id, child_size, parent_id, parent_size) =
                            if doc.content_len >= cand.content_len {
                                (
                                    doc_id,
                                    doc.content_len as i32,
                                    cand_id,
                                    cand.content_len as i32,
                                )
                            } else {
                                (
                                    cand_id,
                                    cand.content_len as i32,
                                    doc_id,
                                    doc.content_len as i32,
                                )
                            };

                        let size_diff = (child_size - parent_size).abs();
                        let larger_size = child_size.max(parent_size);
                        let size_diff_pct = if larger_size > 0 {
                            size_diff as f64 / larger_size as f64
                        } else {
                            0.0
                        };

                        local_matches.push(MatchRecord {
                            child_id,
                            parent_id,
                            jaccard_similarity: jaccard,
                            size_difference: size_diff,
                            size_difference_pct: size_diff_pct,
                        });
                    }
                }

                if let Some(max_matches) = self.max_matches_per_doc {
                    if local_matches.len() > max_matches {
                        local_matches.sort_by(|a, b| {
                            b.jaccard_similarity
                                .total_cmp(&a.jaccard_similarity)
                                .then_with(|| a.child_id.cmp(&b.child_id))
                                .then_with(|| a.parent_id.cmp(&b.parent_id))
                        });
                        local_matches.truncate(max_matches);
                    }
                }
                duplicates_found.fetch_add(local_matches.len(), Ordering::Relaxed);

                let current_processed = processed.fetch_add(1, Ordering::Relaxed) + 1;
                let should_flush = {
                    let mut pending = lock_recover(&pending_writes, "pending_writes");
                    pending.matches.extend(local_matches);
                    pending.processed_ids.push(doc_id);
                    pending.matches.len() >= batch_size
                        || pending.processed_ids.len() >= state_save_interval
                };

                if should_flush {
                    let dupes = duplicates_found.load(Ordering::Relaxed);
                    let cands = candidates_checked.load(Ordering::Relaxed);
                    if let Err(e) = self.flush_pending_writes(
                        &pending_writes,
                        &state_store_clone,
                        &total_written,
                        Phase2FlushSnapshot {
                            duplicates_found: dupes,
                            candidates_checked: cands,
                            processed_this_run: processed.load(Ordering::Relaxed),
                            remaining_docs,
                        },
                    ) {
                        match_write_failed.store(true, Ordering::Relaxed);
                        state_save_failed.store(true, Ordering::Relaxed);
                        phase2_failed.store(true, Ordering::Relaxed);
                        tracing::warn!("Failed to flush Phase 2 checkpoint: {}", e);
                    }
                }

                let _ = current_processed;
                pb.inc(1);
            });
        });

        // Flush remaining matches before acknowledging those docs as processed.
        if let Err(e) = self.flush_pending_writes(
            &pending_writes,
            &state_store,
            &total_written,
            Phase2FlushSnapshot {
                duplicates_found: duplicates_found.load(Ordering::Relaxed),
                candidates_checked: candidates_checked.load(Ordering::Relaxed),
                processed_this_run: processed.load(Ordering::Relaxed),
                remaining_docs,
            },
        ) {
            match_write_failed.store(true, Ordering::Relaxed);
            state_save_failed.store(true, Ordering::Relaxed);
            phase2_failed.store(true, Ordering::Relaxed);
            tracing::warn!("Failed to flush final Phase 2 checkpoint: {}", e);
        }

        let total_dupes = duplicates_found.load(Ordering::Relaxed);
        let total_cands = candidates_checked.load(Ordering::Relaxed);
        let total_proc = processed.load(Ordering::Relaxed);
        let fetch_errs = doc_fetch_errors.load(Ordering::Relaxed);
        let qry_errs = query_errors.load(Ordering::Relaxed);
        let cand_fetch_errs = candidate_fetch_errors.load(Ordering::Relaxed);

        // Log any errors encountered during processing
        if fetch_errs > 0 || qry_errs > 0 || cand_fetch_errs > 0 {
            tracing::warn!(
                "Encountered {} document fetch errors, {} candidate fetch errors, and {} query errors during Phase 2",
                fetch_errs,
                cand_fetch_errs,
                qry_errs
            );
        }

        if let Err(e) = state_store.save_metadata(total_dupes, total_cands) {
            state_save_failed.store(true, Ordering::Relaxed);
            tracing::warn!("Failed to save final metadata: {}", e);
        } else {
            let total_processed = state_store.processed_count().unwrap_or(0);
            tracing::info!(
                "Phase 2 final state saved: {} docs processed",
                total_processed
            );
        }

        if fetch_errs > 0
            || qry_errs > 0
            || cand_fetch_errs > 0
            || match_write_failed.load(Ordering::Relaxed)
            || state_save_failed.load(Ordering::Relaxed)
            || phase2_failed.load(Ordering::Relaxed)
        {
            anyhow::bail!(
                "Phase 2 failed (doc fetch errors: {}, candidate fetch errors: {}, query errors: {}, match/state write failed: {})",
                fetch_errs,
                cand_fetch_errs,
                qry_errs,
                match_write_failed.load(Ordering::Relaxed) || state_save_failed.load(Ordering::Relaxed)
            );
        }

        pb.finish_with_message(format!(
            "Complete: {} duplicates from {} candidates ({} docs processed)",
            total_dupes, total_cands, total_proc
        ));

        Ok(DiskDedupeStats {
            total_documents: total_docs,
            duplicates_found: total_dupes,
            candidates_checked: total_cands,
            duration_secs: start.elapsed().as_secs_f64(),
        })
    }
}

/// Run disk-based deduplication from an existing LSH index.
///
/// If `new_doc_ids` is provided (incremental mode), only those documents will be
/// processed for duplicate finding. Otherwise, all documents in the index are processed.
pub fn run_disk_dedupe(
    lsh_path: &Path,
    output_dir: &Path,
    num_workers: usize,
    threshold: f64,
    size_diff_threshold: f64,
    fresh: bool,
    new_doc_ids: Option<Vec<Uuid>>,
) -> Result<DiskDedupeStats> {
    run_disk_dedupe_with_options(
        lsh_path,
        output_dir,
        DiskDedupeRunOptions {
            num_workers,
            threshold,
            size_diff_threshold,
            fresh,
            new_doc_ids,
            max_matches_per_doc: None,
        },
    )
}

/// Run disk-based deduplication with optional Phase 2 edge-graph controls.
pub fn run_disk_dedupe_with_options(
    lsh_path: &Path,
    output_dir: &Path,
    options: DiskDedupeRunOptions,
) -> Result<DiskDedupeStats> {
    if options.num_workers == 0 {
        anyhow::bail!("num_workers must be positive");
    }
    if !(0.0..=1.0).contains(&options.threshold) {
        anyhow::bail!("threshold must be between 0.0 and 1.0");
    }
    if options.size_diff_threshold < 0.0 {
        anyhow::bail!("size_diff_threshold must be non-negative");
    }
    if options.max_matches_per_doc == Some(0) {
        anyhow::bail!("max_matches_per_doc must be positive when set");
    }

    log_memory("run_disk_dedupe start");
    tracing::info!("=== Disk-Based Parallel Deduplication ===");
    tracing::info!("LSH index: {:?}", lsh_path);
    tracing::info!("Output dir: {:?}", output_dir);
    tracing::info!("Workers: {}", options.num_workers);
    tracing::info!("Threshold: {:.2}", options.threshold);
    tracing::info!("Size diff threshold: {:.2}", options.size_diff_threshold);
    if let Some(max_matches) = options.max_matches_per_doc {
        tracing::info!("Max matches per doc: {}", max_matches);
    } else {
        tracing::info!("Max matches per doc: unlimited");
    }
    if let Some(ref ids) = options.new_doc_ids {
        tracing::info!("Incremental mode: {} new docs to process", ids.len());
    } else {
        tracing::info!("Full mode: processing all docs in index");
    }
    tracing::info!(
        "Resume mode: {}",
        if !options.fresh {
            "enabled"
        } else {
            "disabled (--fresh)"
        }
    );

    // Create output paths
    std::fs::create_dir_all(output_dir)?;
    let output_path = output_dir.join("matches.redb");
    let state_path = output_dir.join("state.redb");

    // If fresh mode, remove existing files
    if options.fresh {
        if output_path.exists() {
            std::fs::remove_file(&output_path)?;
            tracing::info!("Removed existing matches file");
        }
        if state_path.exists() {
            std::fs::remove_file(&state_path)?;
            tracing::info!("Removed existing state file");
        }
    }

    // Open deduplicator
    let deduper = DiskDeduplicator::open(lsh_path, &output_path)?
        .with_threshold(options.threshold)
        .with_size_diff_threshold(options.size_diff_threshold)
        .with_max_matches_per_doc(options.max_matches_per_doc);

    // Run parallel deduplication (writes matches to disk, doesn't keep in memory)
    let stats = deduper.run(
        options.num_workers,
        &state_path,
        !options.fresh,
        options.new_doc_ids,
    )?;

    tracing::info!("=== Results ===");
    tracing::info!("Documents processed: {}", stats.total_documents);
    tracing::info!("Duplicates found: {}", stats.duplicates_found);
    tracing::info!("Candidates checked: {}", stats.candidates_checked);
    tracing::info!("Duration: {:.2}s", stats.duration_secs);
    if stats.duration_secs > 0.0 {
        tracing::info!(
            "Speed: {:.0} docs/sec",
            stats.total_documents as f64 / stats.duration_secs
        );
    }

    // Verify output
    let final_count = deduper.count_matches()?;
    tracing::info!("Matches in output file: {}", final_count);
    log_memory("run_disk_dedupe end");

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsh::DiskLSH;
    use tempfile::tempdir;

    #[test]
    fn test_flush_pending_writes_persists_match_before_processed_state() {
        let dir = tempdir().unwrap();
        let lsh_path = dir.path().join("lsh.redb");
        let matches_path = dir.path().join("matches.redb");
        let state_path = dir.path().join("state.redb");

        {
            let _lsh = DiskLSH::open(&lsh_path).unwrap();
        }

        let deduper = DiskDeduplicator::open(&lsh_path, &matches_path).unwrap();
        let state_store = Phase2StateStore::open(&state_path).unwrap();
        let total_written = AtomicUsize::new(0);

        let child_id = Uuid::from_u128(2);
        let parent_id = Uuid::from_u128(1);
        let pending = Arc::new(Mutex::new(PendingPhase2Writes {
            matches: vec![MatchRecord {
                child_id,
                parent_id,
                jaccard_similarity: 0.95,
                size_difference: 10,
                size_difference_pct: 0.1,
            }],
            processed_ids: vec![child_id],
        }));

        assert!(deduper
            .flush_pending_writes(
                &pending,
                &state_store,
                &total_written,
                Phase2FlushSnapshot {
                    duplicates_found: 1,
                    candidates_checked: 1,
                    processed_this_run: 1,
                    remaining_docs: 1,
                },
            )
            .unwrap());

        assert_eq!(deduper.count_matches().unwrap(), 1);
        assert_eq!(total_written.load(Ordering::Relaxed), 1);
        assert!(state_store.is_processed(&child_id).unwrap());
    }
}
