//! Document deduplication orchestrator.
//!
//! Provides the main deduplication pipeline that:
//! 1. Fetches documents from PostgreSQL in streaming batches
//! 2. Computes MinHash signatures (in parallel)
//! 3. Writes signatures to disk-backed LSH index (per batch)
//! 4. Finds candidate duplicates by iterating disk LSH
//! 5. Verifies with Jaccard similarity
//! 6. Stores results in local redb storage
//! 7. Optionally syncs to PostgreSQL at the end
//!
//! Key design: Memory usage is fixed regardless of dataset size by:
//! - Processing documents in batches (fetch → compute → write → free)
//! - Using disk-backed LSH (redb) for the index
//! - Using disk-backed storage for results

use crate::db::{DbConfig, DbPool, Document, DupeMatch};
use crate::lsh::{DiskLSH, InMemoryLSH};
use crate::minhash::{jaccard_from_signatures, RMinHash, NUM_PERM};
use crate::sources::{DocumentSource, SourceDupeMatch};
use crate::storage::{FilteredParentStore, MatchRecord, MatchStore};
use crate::union_find::UnionFind;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{info, warn};
use uuid::Uuid;

/// Log current RSS memory usage (Linux only)
#[cfg(target_os = "linux")]
fn log_memory(label: &str) {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(line) = status.lines().find(|l| l.starts_with("VmRSS:")) {
            if let Some(kb_str) = line.split_whitespace().nth(1) {
                if let Ok(kb) = kb_str.parse::<f64>() {
                    let mb = kb / 1024.0;
                    info!("[MEMORY] {}: {:.1} MB ({:.2} GB)", label, mb, mb / 1024.0);
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn log_memory(_label: &str) {}

/// How Phase 3 loads historical match edges connected to the current batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeLookupMode {
    /// Existing implementation: repeatedly scan matches.redb.
    Scan,
    /// Use the adjacency side-index when a completed backfill is recorded.
    Auto,
    /// Return scan results, but compare them against the adjacency index.
    Shadow,
}

/// Configuration for the deduplication process
#[derive(Debug, Clone)]
pub struct DedupeConfig {
    /// Jaccard similarity threshold for considering documents as duplicates
    pub threshold: f64,
    /// Maximum size difference ratio to consider pairs (e.g., 0.3 = 30%)
    pub size_diff_threshold: f64,
    /// Batch size for database fetches
    pub batch_size: i64,
    /// Number of parallel workers for MinHash computation
    pub num_workers: usize,
    /// Whether to use disk-backed LSH index (recommended for large datasets)
    pub use_disk_lsh: bool,
    /// Path for disk LSH index (deprecated, use data_dir instead)
    pub disk_lsh_path: Option<String>,
    /// MinHash seed
    pub seed: u64,
    /// Fetch all source documents instead of only unprocessed rows.
    ///
    /// For a true full rebuild, set `fresh = true` as well so local sidecar
    /// state is cleared before indexing.
    pub process_all: bool,
    /// Clear local sidecar state before processing.
    ///
    /// Intended for explicit full rebuilds. Daemon mode should leave this false.
    pub fresh: bool,
    /// Base directory for all data files (LSH index, matches, state)
    /// Default: ./data
    pub data_dir: PathBuf,
    /// Skip writing results to PostgreSQL (for testing/validation)
    pub skip_db_write: bool,
    /// Use disk-based Phase 2 instead of in-memory (lower RAM, slower)
    pub disk_phase2: bool,
    /// Minimum UTF-8 byte length to index; shorter documents are skipped.
    /// Documents <= this length are too short for meaningful deduplication.
    /// Default: 500 (matches Python version)
    pub min_content_length: i32,
    /// Lookup strategy for connected historical match edges in Phase 3.
    pub edge_lookup_mode: EdgeLookupMode,
}

impl Default for DedupeConfig {
    fn default() -> Self {
        Self {
            threshold: 0.8,
            size_diff_threshold: 0.3,
            batch_size: 10000,
            num_workers: num_cpus::get(),
            use_disk_lsh: true, // Default to disk-backed for safety
            disk_lsh_path: None,
            seed: 42,
            process_all: false,
            fresh: false,
            data_dir: PathBuf::from("./data"),
            skip_db_write: false,
            disk_phase2: true,
            min_content_length: 500, // Match Python version - skip docs <= 500 UTF-8 bytes
            edge_lookup_mode: EdgeLookupMode::Scan,
        }
    }
}

fn normalized_match_records(records: &[MatchRecord]) -> Vec<(Uuid, Uuid, u64, i32, u64)> {
    let mut normalized: Vec<_> = records
        .iter()
        .map(|record| {
            (
                record.child_id,
                record.parent_id,
                record.jaccard_similarity.to_bits(),
                record.size_difference,
                record.size_difference_pct.to_bits(),
            )
        })
        .collect();
    normalized.sort_unstable();
    normalized
}

fn load_connected_matches(
    matches_store: &MatchStore,
    seed_doc_ids: &[Uuid],
    mode: EdgeLookupMode,
    label: &str,
) -> Result<Vec<MatchRecord>> {
    match mode {
        EdgeLookupMode::Scan => matches_store.get_real_edges_connected_to(seed_doc_ids),
        EdgeLookupMode::Auto => {
            let (records, used_index) =
                matches_store.get_real_edges_connected_to_auto(seed_doc_ids)?;
            if used_index {
                info!(
                    "Loaded {} connected match edges for {} using adjacency index",
                    records.len(),
                    label
                );
            } else {
                info!(
                    "Loaded {} connected match edges for {} using full scan; adjacency index is not built",
                    records.len(),
                    label
                );
            }
            Ok(records)
        }
        EdgeLookupMode::Shadow => {
            let scan_start = std::time::Instant::now();
            let scan_records = matches_store.get_real_edges_connected_to(seed_doc_ids)?;
            let scan_elapsed = scan_start.elapsed();

            if !matches_store.is_adjacency_built()? {
                info!(
                    "Adjacency shadow skipped for {}; index is not built. Full scan returned {} edges in {:.2}s",
                    label,
                    scan_records.len(),
                    scan_elapsed.as_secs_f64()
                );
                return Ok(scan_records);
            }

            let indexed_start = std::time::Instant::now();
            let indexed_records =
                matches_store.get_real_edges_connected_to_indexed(seed_doc_ids)?;
            let indexed_elapsed = indexed_start.elapsed();

            let scan_normalized = normalized_match_records(&scan_records);
            let indexed_normalized = normalized_match_records(&indexed_records);
            if scan_normalized == indexed_normalized {
                info!(
                    "Adjacency shadow check passed for {}: {} edges, scan {:.2}s, index {:.2}s",
                    label,
                    scan_records.len(),
                    scan_elapsed.as_secs_f64(),
                    indexed_elapsed.as_secs_f64()
                );
            } else {
                warn!(
                    "ADJACENCY_SHADOW_MISMATCH for {}: scan_edges={}, indexed_edges={}, scan_time={:.2}s, indexed_time={:.2}s",
                    label,
                    scan_records.len(),
                    indexed_records.len(),
                    scan_elapsed.as_secs_f64(),
                    indexed_elapsed.as_secs_f64()
                );
            }

            Ok(scan_records)
        }
    }
}

/// Statistics from deduplication run
#[derive(Debug, Default)]
pub struct DedupeStats {
    pub total_documents: usize,
    pub duplicates_found: usize,
    pub unique_parents: usize,
    pub candidates_checked: usize,
    pub duration_secs: f64,
    pub skipped_short: usize,
    pub skipped_boilerplate: usize,
}

/// Tokenize document content into shingles
fn tokenize(content: &str) -> Vec<String> {
    // Simple word-based tokenization with 3-word shingles
    let words: Vec<&str> = content.split_whitespace().filter(|w| w.len() > 1).collect();

    if words.len() < 3 {
        return words.iter().map(|s| s.to_string()).collect();
    }

    words.windows(3).map(|w| w.join(" ")).collect()
}

/// Load junk patterns from a file. Each line is treated as a pattern.
/// Empty lines and lines starting with # are ignored.
/// Patterns can contain literal \n for newlines.
pub fn load_junk_patterns<P: AsRef<Path>>(path: P) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("Failed to read junk patterns from {:?}", path.as_ref()))?;

    let patterns: Vec<String> = content
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        // Convert literal \n in the file to actual newlines
        .map(|line| line.replace("\\n", "\n"))
        .collect();

    info!(
        "Loaded {} junk patterns from {:?}",
        patterns.len(),
        path.as_ref()
    );
    Ok(patterns)
}

/// Check if content is likely boilerplate/junk that shouldn't be indexed.
///
/// This function applies:
/// 1. Generic heuristics (empty content, navigation menu detection)
/// 2. User-provided patterns loaded from junk_patterns.txt
///
/// Returns true if the content matches any junk pattern or heuristic.
pub fn is_boilerplate(content: &str, custom_patterns: &[String]) -> bool {
    // Quick checks first
    let content_trimmed = content.trim();

    // Empty or whitespace-only
    if content_trimmed.is_empty() {
        return true;
    }

    // Check for user-provided patterns
    for pattern in custom_patterns {
        if content.contains(pattern.as_str()) {
            return true;
        }
    }

    // Heuristic: if content is mostly navigation (high ratio of newlines to content)
    // Typical navigation has many short lines
    let lines: Vec<&str> = content_trimmed.lines().collect();
    if lines.len() > 10 {
        let avg_line_len: f64 = content_trimmed.len() as f64 / lines.len() as f64;
        // If average line is very short (< 20 chars) and we have many lines, it's likely nav
        if avg_line_len < 20.0 && lines.len() > 20 {
            return true;
        }
    }

    false
}

/// Compute MinHash signature for a document
fn compute_signature(content: &str, seed: u64) -> Vec<u32> {
    let mut tokens = tokenize(content);
    if tokens.is_empty() {
        tokens.push(content.to_string());
    }
    let mut minhash = RMinHash::new(NUM_PERM, seed);
    minhash.update(&tokens);
    minhash.digest_owned()
}

/// Remove pathological documents from the current Phase 2 batch and return only
/// the pathological IDs that were actually part of this run.
fn take_pathological_batch_ids(
    new_doc_ids: &mut Vec<Uuid>,
    pathological_doc_ids: &HashSet<Uuid>,
) -> Vec<Uuid> {
    let mut pathological_batch_ids = Vec::new();

    new_doc_ids.retain(|id| {
        if pathological_doc_ids.contains(id) {
            pathological_batch_ids.push(*id);
            false
        } else {
            true
        }
    });

    pathological_batch_ids
}

fn clear_sidecar_files(dataset_dir: &Path) -> Result<()> {
    for file_name in [
        "lsh.redb",
        "matches.redb",
        "state.redb",
        "filtered_parents.redb",
    ] {
        let path = dataset_dir.join(file_name);
        if path.exists() {
            std::fs::remove_file(&path)
                .with_context(|| format!("Failed to remove stale sidecar file {:?}", path))?;
            info!("Removed stale sidecar file {:?}", path);
        }
    }
    Ok(())
}

fn validate_dedupe_config(config: &DedupeConfig) -> Result<()> {
    if !(0.0..=1.0).contains(&config.threshold) {
        anyhow::bail!("threshold must be between 0.0 and 1.0");
    }
    if config.size_diff_threshold < 0.0 {
        anyhow::bail!("size_diff_threshold must be non-negative");
    }
    if config.batch_size <= 0 {
        anyhow::bail!("batch_size must be positive");
    }
    if config.num_workers == 0 {
        anyhow::bail!("num_workers must be positive");
    }
    if config.min_content_length < 0 {
        anyhow::bail!("min_content_length must be non-negative");
    }
    Ok(())
}

fn batch_size_usize(batch_size: i64) -> Result<usize> {
    if batch_size <= 0 {
        anyhow::bail!("batch_size must be positive");
    }
    usize::try_from(batch_size).context("batch_size does not fit in usize")
}

async fn flush_filtered_parents_pool(pool: &DbPool, buffer: &mut Vec<Uuid>) -> Result<()> {
    if !buffer.is_empty() {
        pool.mark_as_parents(buffer).await?;
        buffer.clear();
    }
    Ok(())
}

async fn flush_filtered_parents_source<S: DocumentSource>(
    source: &S,
    buffer: &mut Vec<Uuid>,
) -> Result<()> {
    if !buffer.is_empty() {
        source.mark_as_parents(buffer).await?;
        buffer.clear();
    }
    Ok(())
}

fn flush_filtered_parents_sidecar(dataset_dir: &Path, buffer: &mut Vec<Uuid>) -> Result<()> {
    if !buffer.is_empty() {
        let store = FilteredParentStore::open(dataset_dir.join("filtered_parents.redb"))?;
        store.insert_batch(buffer)?;
        buffer.clear();
    }
    Ok(())
}

fn match_doc_ids(matches: &[MatchRecord]) -> Vec<Uuid> {
    let mut ids: Vec<Uuid> = matches
        .iter()
        .flat_map(|m| [m.child_id, m.parent_id])
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn assignments_requiring_write(
    resolved_matches: &[DupeMatch],
    new_doc_set: &HashSet<Uuid>,
    existing_dupe_parents: &HashMap<Uuid, Uuid>,
) -> Vec<DupeMatch> {
    resolved_matches
        .iter()
        .filter(|m| {
            new_doc_set.contains(&m.child_id)
                || existing_dupe_parents.get(&m.child_id) != Some(&m.parent_id)
        })
        .cloned()
        .collect()
}

fn child_marks_to_apply(
    child_ids: &HashSet<Uuid>,
    new_doc_set: &HashSet<Uuid>,
    existing_parent_ids: &HashSet<Uuid>,
) -> Vec<Uuid> {
    child_ids
        .iter()
        .filter(|id| new_doc_set.contains(id) || existing_parent_ids.contains(id))
        .copied()
        .collect()
}

fn parent_marks_to_apply(
    parent_ids: &HashSet<Uuid>,
    new_doc_ids: &[Uuid],
    child_ids: &HashSet<Uuid>,
    existing_dupe_parents: &HashMap<Uuid, Uuid>,
) -> Vec<Uuid> {
    let mut ids: HashSet<Uuid> = new_doc_ids
        .iter()
        .filter(|id| !child_ids.contains(id))
        .copied()
        .collect();

    ids.extend(
        parent_ids
            .iter()
            .filter(|id| existing_dupe_parents.contains_key(id))
            .copied(),
    );

    let mut ids: Vec<Uuid> = ids.into_iter().collect();
    ids.sort();
    ids
}

async fn flush_filtered_parents_db_run(
    pool: &DbPool,
    dataset_dir: &Path,
    skip_db_write: bool,
    buffer: &mut Vec<Uuid>,
) -> Result<()> {
    if skip_db_write {
        flush_filtered_parents_sidecar(dataset_dir, buffer)
    } else {
        flush_filtered_parents_pool(pool, buffer).await
    }
}

async fn flush_filtered_parents_source_run<S: DocumentSource>(
    source: &S,
    dataset_dir: &Path,
    write_to_source: bool,
    buffer: &mut Vec<Uuid>,
) -> Result<()> {
    if write_to_source {
        flush_filtered_parents_source(source, buffer).await
    } else {
        flush_filtered_parents_sidecar(dataset_dir, buffer)
    }
}

/// Main deduplicator using in-memory LSH
pub struct InMemoryDeduplicator {
    config: DedupeConfig,
    lsh: InMemoryLSH,
    doc_sizes: HashMap<Uuid, i32>,
}

impl InMemoryDeduplicator {
    /// Create a new in-memory deduplicator
    pub fn new(config: DedupeConfig) -> Self {
        Self {
            config,
            lsh: InMemoryLSH::new(),
            doc_sizes: HashMap::new(),
        }
    }

    /// Process documents and build the LSH index
    pub fn index_documents(&mut self, documents: &[Document]) {
        let seed = self.config.seed;
        let pb = ProgressBar::new(documents.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({per_sec}) {msg}")
                .unwrap(),
        );
        pb.set_message("Computing signatures...");

        // Compute signatures in parallel
        let signatures: Vec<(Uuid, Vec<u32>, i32)> = documents
            .par_iter()
            .map(|doc| {
                let sig = compute_signature(&doc.content, seed);
                pb.inc(1);
                (doc.id, sig, doc.content_len)
            })
            .collect();

        pb.finish_with_message("Signatures computed");

        // Insert into LSH index
        let pb = ProgressBar::new(signatures.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} Indexing...")
                .unwrap(),
        );

        for (doc_id, signature, content_len) in signatures {
            self.doc_sizes.insert(doc_id, content_len);
            self.lsh.insert(doc_id, signature);
            pb.inc(1);
        }

        pb.finish_with_message("Indexing complete");
    }

    /// Find all duplicate pairs and return parent assignments
    pub fn find_duplicates(&self) -> HashMap<Uuid, Uuid> {
        let matches = self.find_duplicates_with_scores();
        let pairs: Vec<(Uuid, Uuid)> = matches.iter().map(|m| (m.child_id, m.parent_id)).collect();
        build_parent_assignments(&pairs)
    }

    /// Find all duplicate pairs with full match information (including Jaccard scores)
    pub fn find_duplicates_with_scores(&self) -> Vec<DuplicateMatch> {
        let doc_ids: Vec<Uuid> = self.lsh.doc_ids().copied().collect();
        let total = doc_ids.len();

        let pb = ProgressBar::new(total as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.yellow/blue} {pos}/{len} ({per_sec}) Finding duplicates...")
                .unwrap(),
        );

        let candidates_checked = AtomicUsize::new(0);
        let duplicates_found = AtomicUsize::new(0);

        // Find duplicates in parallel
        let matches: Vec<DuplicateMatch> = doc_ids
            .par_iter()
            .flat_map(|&doc_id| {
                let Some(sig) = self.lsh.get_signature(&doc_id) else {
                    warn!("Missing signature for indexed document {}", doc_id);
                    pb.inc(1);
                    return Vec::new();
                };
                let doc_size = *self.doc_sizes.get(&doc_id).unwrap_or(&0);

                let candidates = self.lsh.query(sig);
                candidates_checked.fetch_add(candidates.len(), Ordering::Relaxed);

                let mut pairs = Vec::new();

                for &candidate_id in &candidates {
                    // Skip self
                    if candidate_id == doc_id {
                        continue;
                    }

                    // Only process each pair once (smaller ID first)
                    if doc_id >= candidate_id {
                        continue;
                    }

                    // Size filter
                    let cand_size = *self.doc_sizes.get(&candidate_id).unwrap_or(&0);
                    if !size_within_threshold(doc_size, cand_size, self.config.size_diff_threshold)
                    {
                        continue;
                    }

                    // Jaccard similarity check
                    let Some(cand_sig) = self.lsh.get_signature(&candidate_id) else {
                        warn!("Missing signature for candidate document {}", candidate_id);
                        continue;
                    };
                    let jaccard = jaccard_from_signatures(sig, cand_sig);

                    if jaccard >= self.config.threshold {
                        duplicates_found.fetch_add(1, Ordering::Relaxed);

                        // Larger document is the child (points to smaller parent)
                        let (child_id, child_size, parent_id, parent_size) =
                            if doc_size >= cand_size {
                                (doc_id, doc_size, candidate_id, cand_size)
                            } else {
                                (candidate_id, cand_size, doc_id, doc_size)
                            };

                        let size_diff = (child_size - parent_size).abs();
                        let larger_size = child_size.max(parent_size);
                        let size_diff_pct = if larger_size > 0 {
                            size_diff as f64 / larger_size as f64
                        } else {
                            0.0
                        };

                        pairs.push(DuplicateMatch {
                            child_id,
                            parent_id,
                            jaccard_similarity: jaccard,
                            size_difference: size_diff,
                            size_difference_pct: size_diff_pct,
                        });
                    }
                }

                pb.inc(1);
                pairs
            })
            .collect();

        pb.finish_with_message(format!(
            "Found {} duplicates from {} candidates",
            duplicates_found.load(Ordering::Relaxed),
            candidates_checked.load(Ordering::Relaxed)
        ));

        matches
    }
}

/// A duplicate match with full similarity information
#[derive(Debug, Clone)]
pub struct DuplicateMatch {
    pub child_id: Uuid,
    pub parent_id: Uuid,
    pub jaccard_similarity: f64,
    pub size_difference: i32,
    pub size_difference_pct: f64,
}

/// Check if two sizes are within the threshold
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

/// Build parent assignments from duplicate pairs using union-find.
///
/// Uses the shared `UnionFind` implementation from `crate::union_find`.
fn build_parent_assignments(pairs: &[(Uuid, Uuid)]) -> HashMap<Uuid, Uuid> {
    UnionFind::from_pairs(pairs)
}

/// Phase 1: Disk-backed LSH index builder for streaming document indexing.
///
/// This struct handles Phase 1 of the deduplication pipeline: building the LSH index
/// by streaming documents from the database and writing signatures to disk. It uses
/// `DiskLSH` for storage, avoiding a full in-memory LSH index.
///
/// # Note
/// This is distinct from `disk_dedupe::DiskDeduplicator` (Phase 2) which handles
/// the parallel comparison phase after indexing is complete.
///
/// # Naming
/// Previously named `DiskDeduplicator`, renamed to `IndexBuilder` to clarify its role
/// as an index-building component rather than a deduplication component.
pub struct IndexBuilder {
    config: DedupeConfig,
    lsh: DiskLSH,
}

impl IndexBuilder {
    /// Create a new disk-backed index builder
    pub fn new<P: AsRef<Path>>(config: DedupeConfig, db_path: P) -> Result<Self> {
        validate_dedupe_config(&config)?;
        let lsh = DiskLSH::open(db_path)?;
        lsh.validate_or_initialize_metadata(config.seed)?;
        Ok(Self { config, lsh })
    }

    /// Process and index documents in batches
    pub fn index_documents(&self, documents: &[Document]) -> Result<()> {
        let seed = self.config.seed;
        let batch_size = 1000;

        let pb = ProgressBar::new(documents.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({per_sec}) Indexing...",
                )
                .unwrap(),
        );

        for chunk in documents.chunks(batch_size) {
            // Compute signatures in parallel
            let entries: Vec<(Uuid, Vec<u32>, usize)> = chunk
                .par_iter()
                .map(|doc| {
                    let sig = compute_signature(&doc.content, seed);
                    (doc.id, sig, doc.content_len as usize)
                })
                .collect();

            // Batch insert
            self.lsh.insert_batch(&entries)?;
            pb.inc(chunk.len() as u64);
        }

        pb.finish_with_message("Indexing complete");
        Ok(())
    }

    /// Deprecated: run Phase 2 through `disk_dedupe::run_disk_dedupe`.
    ///
    /// This type only builds the Phase 1 index. The old shortcut attempted to
    /// resolve duplicates directly here and bypassed the production pipeline
    /// invariants around raw edge preservation, checkpointing, and sync.
    #[deprecated(
        since = "0.2.4",
        note = "Use disk_dedupe::run_disk_dedupe after building the index"
    )]
    pub fn find_duplicates(&self) -> Result<HashMap<Uuid, Uuid>> {
        anyhow::bail!(
            "IndexBuilder::find_duplicates has been disabled; use disk_dedupe::run_disk_dedupe after index_documents"
        )
    }
}

/// Run full deduplication pipeline with streaming architecture.
///
/// This is the main entry point for deduplication. It processes documents in
/// a streaming fashion to avoid OOM on large datasets:
///
/// Phase 1 (Build Index): Stream from PostgreSQL → compute signatures → write to disk LSH
/// Phase 2 (Find Duplicates): Iterate disk LSH → find candidates → verify Jaccard → store matches
/// Phase 3 (Sync to DB): Optionally write results to PostgreSQL
///
/// All intermediate data is stored on disk in the data_dir using source names:
/// - {dataset_name}/lsh.redb: LSH index with signatures and band buckets
/// - {dataset_name}/matches.redb: Duplicate match records
/// - {dataset_name}/state.redb: Document processing state
pub async fn run_dedupe(db_config: DbConfig, dedupe_config: DedupeConfig) -> Result<DedupeStats> {
    validate_dedupe_config(&dedupe_config)?;
    let start = std::time::Instant::now();
    log_memory("run_dedupe start");

    let source_name = db_config.source_name();
    let table_name = db_config.table_name.clone();

    // Connect to database
    info!("Connecting to database...");
    let pool = DbPool::new(db_config).await?;

    info!("PostgreSQL table: {}", table_name);
    if source_name != table_name {
        info!("PostgreSQL scope: {}", source_name);
    }

    // Setup data directory with source name
    let dataset_dir = dedupe_config.data_dir.join(&source_name);
    std::fs::create_dir_all(&dataset_dir)?;
    let lsh_path = dataset_dir.join("lsh.redb");
    info!("Data directory: {:?}", dataset_dir);
    info!("LSH index: {:?}", lsh_path);
    if dedupe_config.fresh {
        info!("Fresh mode enabled: clearing local sidecar files before rebuild");
        clear_sidecar_files(&dataset_dir)?;
    }

    // Load junk patterns from file (if exists)
    let junk_patterns_path = dataset_dir.join("junk_patterns.txt");
    let junk_patterns: Vec<String> = if junk_patterns_path.exists() {
        load_junk_patterns(&junk_patterns_path)?
    } else {
        info!(
            "No junk_patterns.txt found in {:?}, skipping boilerplate filtering",
            dataset_dir
        );
        Vec::new()
    };

    // Count documents to process
    let total = if dedupe_config.process_all {
        let total = pool.count_total().await?;
        info!("Found {} total documents (processing ALL)", total);
        total
    } else {
        let total = pool.count_unprocessed().await?;
        info!("Found {} unprocessed documents", total);
        total
    };

    if total == 0 {
        return Ok(DedupeStats::default());
    }

    // ============================================================
    // PHASE 1: INCREMENTAL Index Build
    // ============================================================
    log_memory("Before Phase 1");
    info!("");
    info!("=== Phase 1: Incremental LSH Index Build ===");

    let open_pb = ProgressBar::new_spinner();
    open_pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] Opening LSH index: {msg}")
            .unwrap(),
    );
    open_pb.set_message(lsh_path.display().to_string());
    open_pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let open_start = std::time::Instant::now();
    let lsh = DiskLSH::open(&lsh_path)?;
    lsh.validate_or_initialize_metadata(dedupe_config.seed)?;
    let existing_count = lsh.count()?;
    let has_bands = lsh.has_bands()?;
    open_pb.finish_with_message(format!(
        "Opened in {:.1}s: {} signatures, has_bands: {}",
        open_start.elapsed().as_secs_f64(),
        existing_count,
        has_bands
    ));
    info!(
        "Existing index: {} signatures, has_bands: {}",
        existing_count, has_bands
    );

    if existing_count > 0 && !has_bands {
        info!("Legacy index detected: building bands from existing signatures...");
        let band_start = std::time::Instant::now();
        let band_count = lsh.build_bands_from_signatures()?;
        info!(
            "Built bands for {} docs in {:.2}s",
            band_count,
            band_start.elapsed().as_secs_f64()
        );
    }

    info!("");
    info!("=== Phase 1a: Finding documents not yet indexed ===");
    let seed = dedupe_config.seed;
    let min_content_len = dedupe_config.min_content_length;
    let mut new_doc_ids: Vec<Uuid> = Vec::new();
    let mut short_doc_count = 0usize;
    let mut boilerplate_doc_count = 0usize;
    let filtered_parent_batch_size = (dedupe_config.batch_size as usize).max(1);
    let mut filtered_parent_buffer: Vec<Uuid> =
        Vec::with_capacity(filtered_parent_batch_size.min(50_000));

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({per_sec}) Phase 1a: Checking & indexing new docs... ETA: {eta}")
            .unwrap(),
    );

    let mut last_id: Option<Uuid> = None;
    let index_batch_size = 1000;
    let mut pending_entries: Vec<(Uuid, Vec<u32>, usize)> = Vec::with_capacity(index_batch_size);
    let mut processed_count = 0u64;

    loop {
        // Step 1: Fetch only UNPROCESSED document IDs (is_parent IS NULL)
        // This is the key fix - we only look at unprocessed docs, not ALL docs
        let unprocessed_ids = if dedupe_config.process_all {
            // --all mode: fetch all docs. The CLI pairs this with fresh=true
            // so one-off full reprocessing rebuilds local sidecars.
            let docs = pool
                .fetch_chunk_after_all(last_id, dedupe_config.batch_size)
                .await?;
            if docs.is_empty() {
                break;
            }
            last_id = docs.last().map(|d| d.id);

            // Process these docs directly since we fetched full content
            for doc in docs {
                // Check for boilerplate/junk first
                if is_boilerplate(&doc.content, &junk_patterns) {
                    boilerplate_doc_count += 1;
                    filtered_parent_buffer.push(doc.id);
                    if filtered_parent_buffer.len() >= filtered_parent_batch_size {
                        flush_filtered_parents_db_run(
                            &pool,
                            &dataset_dir,
                            dedupe_config.skip_db_write,
                            &mut filtered_parent_buffer,
                        )
                        .await?;
                    }
                    continue;
                }
                if doc.content.len() as i32 <= min_content_len {
                    short_doc_count += 1;
                    filtered_parent_buffer.push(doc.id);
                    if filtered_parent_buffer.len() >= filtered_parent_batch_size {
                        flush_filtered_parents_db_run(
                            &pool,
                            &dataset_dir,
                            dedupe_config.skip_db_write,
                            &mut filtered_parent_buffer,
                        )
                        .await?;
                    }
                    continue;
                }
                if existing_count > 0 && lsh.has_document(&doc.id)? {
                    continue;
                }
                let sig = compute_signature(&doc.content, seed);
                pending_entries.push((doc.id, sig, doc.content.len()));
                new_doc_ids.push(doc.id);
                if pending_entries.len() >= index_batch_size {
                    lsh.insert_batch(&pending_entries)?;
                    pending_entries.clear();
                }
            }
            processed_count += dedupe_config.batch_size as u64;
            pb.set_position(processed_count.min(total as u64));
            continue;
        } else {
            // Incremental mode: fetch only unprocessed IDs first
            pool.fetch_unprocessed_ids_after(last_id, dedupe_config.batch_size)
                .await?
        };

        if unprocessed_ids.is_empty() {
            break;
        }
        last_id = unprocessed_ids.last().copied();
        processed_count += unprocessed_ids.len() as u64;
        pb.set_position(processed_count.min(total as u64));

        // Step 2: Filter out IDs already in the index
        let mut ids_to_fetch: Vec<Uuid> = Vec::new();
        for id in &unprocessed_ids {
            if existing_count > 0 && lsh.has_document(id)? {
                // Already indexed - add to new_doc_ids for Phase 2 but don't re-index
                new_doc_ids.push(*id);
                continue;
            }
            ids_to_fetch.push(*id);
        }

        if ids_to_fetch.is_empty() {
            continue;
        }

        // Step 3: Fetch full content only for docs we need to index
        let docs = pool.fetch_documents_by_ids(&ids_to_fetch).await?;

        for doc in docs {
            // Check for boilerplate/junk first
            if is_boilerplate(&doc.content, &junk_patterns) {
                boilerplate_doc_count += 1;
                filtered_parent_buffer.push(doc.id);
                if filtered_parent_buffer.len() >= filtered_parent_batch_size {
                    flush_filtered_parents_db_run(
                        &pool,
                        &dataset_dir,
                        dedupe_config.skip_db_write,
                        &mut filtered_parent_buffer,
                    )
                    .await?;
                }
                continue;
            }
            if doc.content.len() as i32 <= min_content_len {
                short_doc_count += 1;
                filtered_parent_buffer.push(doc.id);
                if filtered_parent_buffer.len() >= filtered_parent_batch_size {
                    flush_filtered_parents_db_run(
                        &pool,
                        &dataset_dir,
                        dedupe_config.skip_db_write,
                        &mut filtered_parent_buffer,
                    )
                    .await?;
                }
                continue;
            }
            let sig = compute_signature(&doc.content, seed);
            pending_entries.push((doc.id, sig, doc.content.len()));
            new_doc_ids.push(doc.id);
            if pending_entries.len() >= index_batch_size {
                lsh.insert_batch(&pending_entries)?;
                pending_entries.clear();
            }
        }
    }
    if !pending_entries.is_empty() {
        lsh.insert_batch(&pending_entries)?;
    }
    flush_filtered_parents_db_run(
        &pool,
        &dataset_dir,
        dedupe_config.skip_db_write,
        &mut filtered_parent_buffer,
    )
    .await?;
    pb.finish_with_message("Phase 1a complete");

    // Short and boilerplate docs are not indexed. They have no meaningful
    // dedupe candidates, so they are flushed as parents during Phase 1 in
    // bounded batches instead of being accumulated or written as self-edges.
    if short_doc_count > 0 || boilerplate_doc_count > 0 {
        if short_doc_count > 0 {
            info!(
                "Skipped {} documents with content <= {} UTF-8 bytes",
                short_doc_count, min_content_len
            );
        }
        if boilerplate_doc_count > 0 {
            info!(
                "Skipped {} boilerplate/junk documents",
                boilerplate_doc_count
            );
        }
    }
    info!("Phase 1 complete: indexed {} new docs.", new_doc_ids.len());
    log_memory("After Phase 1");

    // ============================================================
    // PHASE 1.5: Detect and Skip Pathological Clusters
    // ============================================================
    // Pathological clusters are 1K+ docs that hash identically across 14+ bands.
    // Processing these causes O(n²) comparisons and can hang for hours.
    // We detect them BEFORE Phase 2 and mark them as canonical parents to skip.
    let min_bucket_size = 1000; // Lower than standalone cleanup (1K vs 10K)
    let min_bands = 14; // Require 14/16 bands overlap

    let pathological_doc_ids =
        match crate::cleanup::run_phase_1_5(&lsh, &dataset_dir, min_bucket_size, min_bands) {
            Ok(ids) => ids,
            Err(e) => {
                warn!(
                    "Failed to detect pathological clusters: {}. Continuing anyway.",
                    e
                );
                HashSet::new()
            }
        };

    // Filter out pathological docs from the processing list for Phase 2, but
    // only sync the pathological docs that are part of this incremental batch.
    // run_phase_1_5 scans the full index, so pathological_doc_ids can include
    // tens of thousands of already-synced historical docs.
    let original_count = new_doc_ids.len();
    let pathological_batch_doc_ids =
        take_pathological_batch_ids(&mut new_doc_ids, &pathological_doc_ids);
    if !pathological_doc_ids.is_empty() {
        info!(
            "Detected {} pathological docs in index; {} are in the current batch",
            pathological_doc_ids.len(),
            pathological_batch_doc_ids.len()
        );
    }
    if original_count != new_doc_ids.len() {
        info!(
            "Filtered {} pathological docs, {} remaining for Phase 2",
            original_count - new_doc_ids.len(),
            new_doc_ids.len()
        );
    }

    // ============================================================
    // PHASE 2: Find Duplicates
    // ============================================================
    info!("");

    // If we have neither regular docs nor pathological docs, nothing else to do.
    // Short/boilerplate docs were already flushed as parents during Phase 1.
    if new_doc_ids.is_empty() && pathological_batch_doc_ids.is_empty() {
        info!("No new documents to dedupe - skipping Phase 2 and 3.");
        return Ok(DedupeStats {
            total_documents: total as usize,
            unique_parents: short_doc_count + boilerplate_doc_count,
            skipped_short: short_doc_count,
            skipped_boilerplate: boilerplate_doc_count,
            ..DedupeStats::default()
        });
    }

    // If only pathological docs (no regular docs to compare), skip Phase 2 but still run Phase 3
    let skip_phase2 = new_doc_ids.is_empty();

    log_memory("Before Phase 2");
    let (new_matches, candidates_checked) = if skip_phase2 {
        // All docs are pathological - no Phase 2 needed, matches already in redb
        info!("=== Phase 2: Skipped (only pathological docs) ===");
        let matches_path = dataset_dir.join("matches.redb");
        let matches_store = crate::storage::MatchStore::open(&matches_path)?;
        let matches = load_connected_matches(
            &matches_store,
            &pathological_batch_doc_ids,
            dedupe_config.edge_lookup_mode,
            "database pathological batch",
        )?;
        (matches, 0)
    } else {
        if !dedupe_config.disk_phase2 {
            warn!("In-memory Phase 2 has been removed; using disk-based Phase 2");
        }
        info!("=== Phase 2: Finding Duplicates (DISK-BASED parallel) ===");
        drop(lsh); // Release file lock
        info!("Released LSH index lock for Phase 2");

        let disk_stats = crate::disk_dedupe::run_disk_dedupe(
            &lsh_path,
            &dataset_dir,
            dedupe_config.num_workers,
            dedupe_config.threshold,
            dedupe_config.size_diff_threshold,
            false,
            Some(new_doc_ids.clone()),
        )?;

        // For disk-based mode, read matches from matches.redb instead of keeping in memory
        // This is critical for large datasets to avoid OOM
        info!("Reading matches from disk for Phase 3 sync...");
        let matches_path = dataset_dir.join("matches.redb");
        let matches_store = crate::storage::MatchStore::open(&matches_path)?;
        let new_matches_disk = load_connected_matches(
            &matches_store,
            &new_doc_ids,
            dedupe_config.edge_lookup_mode,
            "database incremental batch",
        )?;

        let new_matches: Vec<MatchRecord> = new_matches_disk
            .iter()
            .map(|m| MatchRecord {
                child_id: m.child_id,
                parent_id: m.parent_id,
                jaccard_similarity: m.jaccard_similarity,
                size_difference: m.size_difference,
                size_difference_pct: m.size_difference_pct,
            })
            .collect();

        (new_matches, disk_stats.candidates_checked)
    };

    info!(
        "Phase 2 complete: found {} new duplicates from {} candidates.",
        new_matches.len(),
        candidates_checked
    );
    log_memory("After Phase 2");

    if !pathological_batch_doc_ids.is_empty() {
        new_doc_ids.extend(pathological_batch_doc_ids.iter().copied());
        info!(
            "Added {} pathological docs from current batch for Phase 3 sync",
            pathological_batch_doc_ids.len()
        );
    }

    // ============================================================
    // PHASE 3: Sync to PostgreSQL
    // ============================================================
    if !dedupe_config.skip_db_write {
        log_memory("Before Phase 3");
        info!("");
        info!("=== Phase 3: Syncing to PostgreSQL ===");
        perform_incremental_sync(&pool, &new_matches, &new_doc_ids, dedupe_config.batch_size)
            .await?;
        log_memory("After Phase 3");
    } else {
        info!("");
        info!("=== Skipping PostgreSQL sync (--skip-db-write) ===");
    }

    let duration = start.elapsed();
    let final_parents = new_doc_ids.len().saturating_sub(new_matches.len())
        + short_doc_count
        + boilerplate_doc_count;
    log_memory("run_dedupe end");

    Ok(DedupeStats {
        total_documents: total as usize,
        duplicates_found: new_matches.len(),
        unique_parents: final_parents,
        candidates_checked,
        duration_secs: duration.as_secs_f64(),
        skipped_short: short_doc_count,
        skipped_boilerplate: boilerplate_doc_count,
    })
}

/// Run deduplication pipeline with a generic document source.
///
/// This is the generic version of `run_dedupe` that works with any `DocumentSource`
/// implementation (PostgreSQL, SQLite, filesystem, etc.).
///
/// # Arguments
/// * `source` - Any implementation of `DocumentSource`
/// * `dedupe_config` - Configuration for the deduplication process
/// * `source_name` - Name to use for the data directory (e.g., dataset name)
///
/// # Returns
/// Statistics about the deduplication run
pub async fn run_dedupe_with_source<S: DocumentSource>(
    source: &S,
    dedupe_config: DedupeConfig,
    source_name: Option<&str>,
) -> Result<DedupeStats> {
    validate_dedupe_config(&dedupe_config)?;
    let start = std::time::Instant::now();
    log_memory("run_dedupe_with_source start");

    // Get source name for directory naming
    let dataset_name = match source_name {
        Some(name) => name.to_string(),
        None => source.source_name().await?,
    };
    info!("Source: {}", dataset_name);

    // Setup data directory
    let dataset_dir = dedupe_config.data_dir.join(&dataset_name);
    std::fs::create_dir_all(&dataset_dir)?;
    let lsh_path = dataset_dir.join("lsh.redb");
    info!("Data directory: {:?}", dataset_dir);
    info!("LSH index: {:?}", lsh_path);
    if dedupe_config.fresh {
        info!("Fresh mode enabled: clearing local sidecar files before rebuild");
        clear_sidecar_files(&dataset_dir)?;
    }

    // Load junk patterns from file (if exists)
    let junk_patterns_path = dataset_dir.join("junk_patterns.txt");
    let junk_patterns: Vec<String> = if junk_patterns_path.exists() {
        load_junk_patterns(&junk_patterns_path)?
    } else {
        info!(
            "No junk_patterns.txt found in {:?}, skipping boilerplate filtering",
            dataset_dir
        );
        Vec::new()
    };

    // Count documents to process
    let total = if dedupe_config.process_all {
        let total = source.count_total().await?;
        info!("Found {} total documents (processing ALL)", total);
        total
    } else {
        let total = source.count_unprocessed().await?;
        info!("Found {} unprocessed documents", total);
        total
    };

    if total == 0 {
        return Ok(DedupeStats::default());
    }

    // ============================================================
    // PHASE 1: INCREMENTAL Index Build
    // ============================================================
    log_memory("Before Phase 1");
    info!("");
    info!("=== Phase 1: Incremental LSH Index Build ===");

    let open_pb = ProgressBar::new_spinner();
    open_pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] Opening LSH index: {msg}")
            .unwrap(),
    );
    open_pb.set_message(lsh_path.display().to_string());
    open_pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let open_start = std::time::Instant::now();
    let lsh = DiskLSH::open(&lsh_path)?;
    lsh.validate_or_initialize_metadata(dedupe_config.seed)?;
    let existing_count = lsh.count()?;
    let has_bands = lsh.has_bands()?;
    open_pb.finish_with_message(format!(
        "Opened in {:.1}s: {} signatures, has_bands: {}",
        open_start.elapsed().as_secs_f64(),
        existing_count,
        has_bands
    ));
    info!(
        "Existing index: {} signatures, has_bands: {}",
        existing_count, has_bands
    );

    if existing_count > 0 && !has_bands {
        info!("Legacy index detected: building bands from existing signatures...");
        let band_start = std::time::Instant::now();
        let band_count = lsh.build_bands_from_signatures()?;
        info!(
            "Built bands for {} docs in {:.2}s",
            band_count,
            band_start.elapsed().as_secs_f64()
        );
    }

    info!("");
    info!("=== Phase 1a: Finding documents not yet indexed ===");
    let seed = dedupe_config.seed;
    let min_content_len = dedupe_config.min_content_length;
    let mut new_doc_ids: Vec<Uuid> = Vec::new();
    let mut short_doc_count = 0usize;
    let mut boilerplate_doc_count = 0usize;
    let mark_filtered_parents = !dedupe_config.skip_db_write && source.supports_write();
    let filtered_parent_batch_size = (dedupe_config.batch_size as usize).max(1);
    let mut filtered_parent_buffer: Vec<Uuid> =
        Vec::with_capacity(filtered_parent_batch_size.min(50_000));

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({per_sec}) Phase 1a: Checking & indexing new docs... ETA: {eta}")
            .unwrap(),
    );

    let mut last_id: Option<Uuid> = None;
    let index_batch_size = 1000;
    let mut pending_entries: Vec<(Uuid, Vec<u32>, usize)> = Vec::with_capacity(index_batch_size);
    let mut processed_count = 0u64;

    loop {
        // Fetch documents using the generic source
        let docs = if dedupe_config.process_all {
            source
                .fetch_all_after(last_id, dedupe_config.batch_size)
                .await?
        } else {
            // For incremental mode, first get IDs then fetch content
            let ids = source
                .fetch_unprocessed_ids_after(last_id, dedupe_config.batch_size)
                .await?;
            if ids.is_empty() {
                break;
            }
            last_id = ids.last().copied();
            source.fetch_by_ids(&ids).await?
        };

        if docs.is_empty() {
            break;
        }
        if dedupe_config.process_all {
            last_id = docs.last().map(|d| d.id);
        }

        for doc in docs {
            // Check for boilerplate/junk first
            if is_boilerplate(&doc.content, &junk_patterns) {
                boilerplate_doc_count += 1;
                filtered_parent_buffer.push(doc.id);
                if filtered_parent_buffer.len() >= filtered_parent_batch_size {
                    flush_filtered_parents_source_run(
                        source,
                        &dataset_dir,
                        mark_filtered_parents,
                        &mut filtered_parent_buffer,
                    )
                    .await?;
                }
                continue;
            }
            if doc.content.len() as i32 <= min_content_len {
                short_doc_count += 1;
                filtered_parent_buffer.push(doc.id);
                if filtered_parent_buffer.len() >= filtered_parent_batch_size {
                    flush_filtered_parents_source_run(
                        source,
                        &dataset_dir,
                        mark_filtered_parents,
                        &mut filtered_parent_buffer,
                    )
                    .await?;
                }
                continue;
            }
            if existing_count > 0 && lsh.has_document(&doc.id)? {
                // Already indexed - add to new_doc_ids for Phase 2 but don't re-index
                if !dedupe_config.process_all {
                    new_doc_ids.push(doc.id);
                }
                continue;
            }
            let sig = compute_signature(&doc.content, seed);
            pending_entries.push((doc.id, sig, doc.content.len()));
            new_doc_ids.push(doc.id);
            if pending_entries.len() >= index_batch_size {
                lsh.insert_batch(&pending_entries)?;
                pending_entries.clear();
            }
        }

        processed_count += dedupe_config.batch_size as u64;
        pb.set_position(processed_count.min(total as u64));
    }
    if !pending_entries.is_empty() {
        lsh.insert_batch(&pending_entries)?;
    }
    flush_filtered_parents_source_run(
        source,
        &dataset_dir,
        mark_filtered_parents,
        &mut filtered_parent_buffer,
    )
    .await?;
    pb.finish_with_message("Phase 1a complete");

    // Short and boilerplate docs are not indexed. They have no meaningful
    // dedupe candidates, so writable sources receive parent marks during
    // Phase 1 in bounded batches instead of accumulating UUIDs.
    if short_doc_count > 0 || boilerplate_doc_count > 0 {
        if short_doc_count > 0 {
            info!(
                "Skipped {} documents with content <= {} UTF-8 bytes",
                short_doc_count, min_content_len
            );
        }
        if boilerplate_doc_count > 0 {
            info!(
                "Skipped {} boilerplate/junk documents",
                boilerplate_doc_count
            );
        }
    }
    info!("Phase 1 complete: indexed {} new docs.", new_doc_ids.len());
    log_memory("After Phase 1");

    // ============================================================
    // PHASE 1.5: Detect and Skip Pathological Clusters
    // ============================================================
    // Pathological clusters are 1K+ docs that hash identically across 14+ bands.
    // Processing these causes O(n²) comparisons and can hang for hours.
    // We detect them BEFORE Phase 2 and mark them as canonical parents to skip.
    let min_bucket_size = 1000;
    let min_bands = 14;

    let pathological_doc_ids =
        match crate::cleanup::run_phase_1_5(&lsh, &dataset_dir, min_bucket_size, min_bands) {
            Ok(ids) => ids,
            Err(e) => {
                warn!(
                    "Failed to detect pathological clusters: {}. Continuing anyway.",
                    e
                );
                HashSet::new()
            }
        };

    // Filter out pathological docs from the processing list for Phase 2, but
    // only sync the pathological docs that are part of this incremental batch.
    // run_phase_1_5 scans the full index, so pathological_doc_ids can include
    // already-synced historical docs.
    let original_count = new_doc_ids.len();
    let pathological_batch_doc_ids =
        take_pathological_batch_ids(&mut new_doc_ids, &pathological_doc_ids);
    if !pathological_doc_ids.is_empty() {
        info!(
            "Detected {} pathological docs in index; {} are in the current batch",
            pathological_doc_ids.len(),
            pathological_batch_doc_ids.len()
        );
    }
    if original_count != new_doc_ids.len() {
        info!(
            "Filtered {} pathological docs, {} remaining for Phase 2",
            original_count - new_doc_ids.len(),
            new_doc_ids.len()
        );
    }

    // ============================================================
    // PHASE 2: Find Duplicates
    // ============================================================
    info!("");

    // If we have neither regular docs nor pathological docs, nothing else to do.
    // Short/boilerplate docs were already flushed as parents during Phase 1
    // when the source supports writes.
    if new_doc_ids.is_empty() && pathological_batch_doc_ids.is_empty() {
        info!("No new documents to dedupe - skipping Phase 2 and 3.");
        return Ok(DedupeStats {
            total_documents: total as usize,
            unique_parents: short_doc_count + boilerplate_doc_count,
            skipped_short: short_doc_count,
            skipped_boilerplate: boilerplate_doc_count,
            ..DedupeStats::default()
        });
    }

    // If only pathological docs (no regular docs to compare), skip Phase 2 but still run Phase 3
    let skip_phase2 = new_doc_ids.is_empty();

    log_memory("Before Phase 2");
    let (new_matches, candidates_checked) = if skip_phase2 {
        // All docs are pathological - no Phase 2 needed, matches already in redb
        info!("=== Phase 2: Skipped (only pathological docs) ===");
        let matches_path = dataset_dir.join("matches.redb");
        let matches_store = crate::storage::MatchStore::open(&matches_path)?;
        let matches = load_connected_matches(
            &matches_store,
            &pathological_batch_doc_ids,
            dedupe_config.edge_lookup_mode,
            "source pathological batch",
        )?;
        (matches, 0)
    } else {
        if !dedupe_config.disk_phase2 {
            warn!("In-memory Phase 2 has been removed; using disk-based Phase 2");
        }
        info!("=== Phase 2: Finding Duplicates (DISK-BASED parallel) ===");
        drop(lsh); // Release file lock
        info!("Released LSH index lock for Phase 2");

        let disk_stats = crate::disk_dedupe::run_disk_dedupe(
            &lsh_path,
            &dataset_dir,
            dedupe_config.num_workers,
            dedupe_config.threshold,
            dedupe_config.size_diff_threshold,
            false,
            Some(new_doc_ids.clone()),
        )?;

        // For disk-based mode, read matches from matches.redb instead of keeping in memory
        // This is critical for large datasets to avoid OOM
        info!("Reading matches from disk for Phase 3 sync...");
        let matches_path = dataset_dir.join("matches.redb");
        let matches_store = crate::storage::MatchStore::open(&matches_path)?;
        let new_matches_disk = load_connected_matches(
            &matches_store,
            &new_doc_ids,
            dedupe_config.edge_lookup_mode,
            "source incremental batch",
        )?;

        let new_matches: Vec<MatchRecord> = new_matches_disk
            .iter()
            .map(|m| MatchRecord {
                child_id: m.child_id,
                parent_id: m.parent_id,
                jaccard_similarity: m.jaccard_similarity,
                size_difference: m.size_difference,
                size_difference_pct: m.size_difference_pct,
            })
            .collect();

        (new_matches, disk_stats.candidates_checked)
    };

    info!(
        "Phase 2 complete: found {} new duplicates from {} candidates.",
        new_matches.len(),
        candidates_checked
    );
    log_memory("After Phase 2");

    if !pathological_batch_doc_ids.is_empty() {
        new_doc_ids.extend(pathological_batch_doc_ids.iter().copied());
        info!(
            "Added {} pathological docs from current batch for Phase 3 sync",
            pathological_batch_doc_ids.len()
        );
    }

    // ============================================================
    // PHASE 3: Sync to Source
    // ============================================================
    if !dedupe_config.skip_db_write && source.supports_write() {
        log_memory("Before Phase 3");
        info!("");
        info!("=== Phase 3: Syncing to Data Source ===");
        perform_incremental_sync_generic(
            source,
            &new_matches,
            &new_doc_ids,
            dedupe_config.batch_size,
        )
        .await?;
        log_memory("After Phase 3");
    } else if dedupe_config.skip_db_write {
        info!("");
        info!("=== Skipping sync (--skip-db-write) ===");
    } else {
        info!("");
        info!("=== Skipping sync (source does not support write) ===");
    }

    let duration = start.elapsed();
    let final_parents = new_doc_ids.len().saturating_sub(new_matches.len())
        + short_doc_count
        + boilerplate_doc_count;
    log_memory("run_dedupe_with_source end");

    Ok(DedupeStats {
        total_documents: total as usize,
        duplicates_found: new_matches.len(),
        unique_parents: final_parents,
        candidates_checked,
        duration_secs: duration.as_secs_f64(),
        skipped_short: short_doc_count,
        skipped_boilerplate: boilerplate_doc_count,
    })
}

/// Generic incremental sync for any DocumentSource.
///
/// IMPORTANT: This function receives all touched component edges for correct
/// union-find. It syncs new documents plus any historical assignments that
/// actually changed because the new batch bridged existing clusters.
async fn perform_incremental_sync_generic<S: DocumentSource>(
    source: &S,
    all_matches: &[MatchRecord],
    new_doc_ids: &[Uuid],
    batch_size: i64,
) -> Result<()> {
    let batch_size = batch_size_usize(batch_size)?;
    info!(
        "Starting incremental sync for {} new documents...",
        new_doc_ids.len()
    );

    // Build a set for O(1) lookup of new doc IDs
    let new_doc_set: std::collections::HashSet<Uuid> = new_doc_ids.iter().copied().collect();

    let component_doc_ids = match_doc_ids(all_matches);
    let existing_parent_ids = source.fetch_existing_parent_ids(&component_doc_ids).await?;
    let existing_dupe_parents = source
        .fetch_existing_dupe_parents(&component_doc_ids)
        .await?;

    // Step 1: Resolve transitivity on all touched edges. In incremental mode,
    // prefer already-marked parents so a later lower UUID does not destabilize
    // historical canonical assignments.
    let (resolved_matches, all_parent_ids, all_child_ids) =
        resolve_transitivity_with_preferred_roots(
            all_matches,
            &existing_parent_ids,
            Some(&new_doc_set),
        );

    // Step 2: Keep matches where child is in new_doc_ids or where an existing
    // assignment changed because a new doc bridged clusters.
    let assignments_to_write =
        assignments_requiring_write(&resolved_matches, &new_doc_set, &existing_dupe_parents);

    info!(
        "Resolved {} total matches, {} require source writes.",
        resolved_matches.len(),
        assignments_to_write.len()
    );

    // Step 3: Write only assignments that are new or changed.
    if !assignments_to_write.is_empty() {
        info!(
            "Writing {} new assignments to source...",
            assignments_to_write.len()
        );
        let source_matches: Vec<SourceDupeMatch> = assignments_to_write
            .iter()
            .map(|m| SourceDupeMatch {
                child_id: m.child_id,
                parent_id: m.parent_id,
                jaccard_similarity: m.jaccard_similarity,
                size_difference: m.size_difference,
                size_difference_pct: m.size_difference_pct,
            })
            .collect();

        for chunk in source_matches.chunks(batch_size) {
            source.write_dupes(chunk).await?;
        }
    }

    // Step 4: Mark new children and any historical parent that became a child.
    if source.tracks_state() {
        let child_ids_to_mark =
            child_marks_to_apply(&all_child_ids, &new_doc_set, &existing_parent_ids);

        if !child_ids_to_mark.is_empty() {
            info!(
                "Marking {} documents as children...",
                child_ids_to_mark.len()
            );
            for chunk in child_ids_to_mark.chunks(batch_size) {
                source.mark_as_children(chunk).await?;
            }
        }

        // Step 5: Mark all NEW documents that are NOT children as parents
        // This includes both:
        // - Cluster roots (docs that have children pointing to them)
        // - Unique docs (docs with no matches at all)
        let parent_ids_to_mark = parent_marks_to_apply(
            &all_parent_ids,
            new_doc_ids,
            &all_child_ids,
            &existing_dupe_parents,
        );

        if !parent_ids_to_mark.is_empty() {
            info!(
                "Marking {} documents as parents...",
                parent_ids_to_mark.len()
            );
            for chunk in parent_ids_to_mark.chunks(batch_size) {
                source.mark_as_parents(chunk).await?;
            }
        }
    }

    info!("Incremental sync complete.");
    Ok(())
}

/// Performs a fully incremental sync to the database.
///
/// IMPORTANT: This function receives all touched component edges for correct
/// union-find. It syncs new documents plus any historical assignments that
/// actually changed because the new batch bridged existing clusters.
///
/// 1. Resolves transitivity on ALL matches for correct cluster assignment.
/// 2. Writes new or changed child records to the `dupes` table.
/// 3. Marks new children and changed historical roots as `is_parent = false`.
/// 4. Marks new parents and changed historical children as `is_parent = true`.
async fn perform_incremental_sync(
    pool: &DbPool,
    all_matches: &[MatchRecord],
    new_doc_ids: &[Uuid],
    batch_size: i64,
) -> Result<()> {
    let batch_size = batch_size_usize(batch_size)?;
    info!(
        "Starting incremental sync for {} new documents...",
        new_doc_ids.len()
    );

    // Build a set for O(1) lookup of new doc IDs
    let new_doc_set: std::collections::HashSet<Uuid> = new_doc_ids.iter().copied().collect();

    let component_doc_ids = match_doc_ids(all_matches);
    let existing_parent_ids = pool.fetch_parent_ids_by_doc_ids(&component_doc_ids).await?;
    let existing_dupe_parents = pool
        .fetch_dupe_parents_by_child_ids(&component_doc_ids)
        .await?;

    // Step 1: Resolve transitivity on all touched edges. Prefer existing
    // canonical parents so incremental runs do not choose a newly-arrived lower
    // UUID as root unless no historical parent exists.
    let (resolved_matches, all_parent_ids, all_child_ids) =
        resolve_transitivity_with_preferred_roots(
            all_matches,
            &existing_parent_ids,
            Some(&new_doc_set),
        );

    // Step 2: Write new child assignments and any historical assignments that
    // actually changed because the current batch bridged existing clusters.
    let assignments_to_write =
        assignments_requiring_write(&resolved_matches, &new_doc_set, &existing_dupe_parents);

    info!(
        "Resolved {} total matches, {} require database writes.",
        resolved_matches.len(),
        assignments_to_write.len()
    );

    // Step 3: Write only assignments that are new or changed.
    if !assignments_to_write.is_empty() {
        info!(
            "Writing {} new assignments to database...",
            assignments_to_write.len()
        );
        for chunk in assignments_to_write.chunks(batch_size) {
            pool.write_dupes(chunk).await?;
        }
    }

    // Step 4: Mark new children and any historical parent that became a child.
    let child_ids_to_mark =
        child_marks_to_apply(&all_child_ids, &new_doc_set, &existing_parent_ids);

    if !child_ids_to_mark.is_empty() {
        info!(
            "Marking {} documents as children...",
            child_ids_to_mark.len()
        );
        for chunk in child_ids_to_mark.chunks(batch_size) {
            pool.mark_as_children(chunk).await?;
        }
    }

    // Step 5: Mark all NEW documents that are NOT children as parents
    // This includes both:
    // - Cluster roots (docs that have children pointing to them)
    // - Unique docs (docs with no matches at all)
    let parent_ids_to_mark = parent_marks_to_apply(
        &all_parent_ids,
        new_doc_ids,
        &all_child_ids,
        &existing_dupe_parents,
    );

    if !parent_ids_to_mark.is_empty() {
        info!(
            "Marking {} documents as parents...",
            parent_ids_to_mark.len()
        );
        for chunk in parent_ids_to_mark.chunks(batch_size) {
            pool.mark_as_parents(chunk).await?;
        }
    }

    info!("Incremental sync complete.");
    Ok(())
}

/// Resolve transitivity using Union-Find to ensure consistent parent assignments.
/// Converts chains like A->B->C to A->C, B->C (all point to root).
/// Returns (resolved_matches, parent_ids, child_ids)
///
/// NOTE on Jaccard metrics: The stored jaccard_similarity is the BEST similarity
/// this document has to ANY cluster member, not necessarily to the canonical root.
/// For example, if A→B (0.95) and B→C (0.90), A will point to C (the root) but
/// store jaccard=0.95 (from the A→B edge). This is intentional:
/// - The similarity score represents "how duplicate-like is this document"
/// - The parent_id represents "which canonical document represents this cluster"
/// - Recomputing similarity to the root would require loading signatures, which
///   is expensive and not necessary for the deduplication use case.
pub fn resolve_transitivity(
    matches: &[MatchRecord],
) -> (Vec<DupeMatch>, HashSet<Uuid>, HashSet<Uuid>) {
    resolve_transitivity_with_preferred_roots(matches, &HashSet::new(), None)
}

fn resolve_transitivity_with_preferred_roots(
    matches: &[MatchRecord],
    preferred_parent_ids: &HashSet<Uuid>,
    new_doc_ids: Option<&HashSet<Uuid>>,
) -> (Vec<DupeMatch>, HashSet<Uuid>, HashSet<Uuid>) {
    use crate::union_find::UnionFind;

    if matches.is_empty() {
        return (vec![], HashSet::new(), HashSet::new());
    }

    // Step 1: Build Union-Find from real duplicate pairs. Self-parent records
    // are legacy coverage markers; they must not compete with real edges.
    let mut uf = UnionFind::new();
    let mut explicit_parent_ids: HashSet<Uuid> = HashSet::new();

    for m in matches {
        if m.child_id == m.parent_id {
            explicit_parent_ids.insert(m.child_id);
        }
    }

    let mut root_preferences = preferred_parent_ids.clone();
    root_preferences.extend(explicit_parent_ids.iter().copied());

    // Track the best similarity score for each document, regardless of the
    // raw edge direction. Incremental root preferences can make either endpoint
    // become the canonical child after transitivity resolution.
    let mut best_similarity: HashMap<Uuid, (f64, i32, f64)> = HashMap::new();

    let mut update_best = |doc_id: Uuid, m: &MatchRecord| {
        best_similarity
            .entry(doc_id)
            .and_modify(|e| {
                if m.jaccard_similarity > e.0 {
                    *e = (
                        m.jaccard_similarity,
                        m.size_difference,
                        m.size_difference_pct,
                    );
                }
            })
            .or_insert((
                m.jaccard_similarity,
                m.size_difference,
                m.size_difference_pct,
            ));
    };

    for m in matches {
        if m.child_id == m.parent_id {
            continue;
        }

        uf.make_set(m.child_id);
        uf.make_set(m.parent_id);
        uf.union_by_key(m.child_id, m.parent_id, |id| {
            let priority = if root_preferences.contains(&id) {
                0u8
            } else if new_doc_ids.is_some_and(|ids| ids.contains(&id)) {
                2u8
            } else {
                1u8
            };
            (priority, id)
        });

        update_best(m.child_id, m);
        update_best(m.parent_id, m);
    }

    // Step 2: Resolve each node to its canonical root
    let mut assignments: Vec<DupeMatch> = Vec::new();
    let mut parent_ids: HashSet<Uuid> = explicit_parent_ids;
    let mut child_ids: HashSet<Uuid> = HashSet::new();

    for node in uf.nodes() {
        let root = uf.find(node);

        if root != node {
            // This node is a child (points to a different root)
            child_ids.insert(node);
            parent_ids.insert(root);

            // Get the best similarity score we have for this child
            let (similarity, size_diff, size_diff_pct) =
                best_similarity.get(&node).copied().unwrap_or((0.8, 0, 0.0));

            assignments.push(DupeMatch {
                child_id: node,
                parent_id: root, // Canonical root parent!
                jaccard_similarity: similarity,
                size_difference: size_diff,
                size_difference_pct: size_diff_pct,
            });
        } else {
            // This node is a root (parent)
            parent_ids.insert(node);
        }
    }

    assignments.sort_by_key(|m| (m.child_id, m.parent_id));

    (assignments, parent_ids, child_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_within_threshold() {
        assert!(size_within_threshold(100, 110, 0.3)); // 10% diff
        assert!(size_within_threshold(100, 130, 0.3)); // 30% diff (exactly at threshold)
        assert!(!size_within_threshold(100, 150, 0.3)); // 50% diff
    }

    #[test]
    fn test_build_parent_assignments() {
        let id1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let id2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let id3 = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();

        // A -> B, B -> C should result in A -> C, B -> C
        let pairs = vec![(id2, id1), (id3, id2)];
        let assignments = build_parent_assignments(&pairs);

        // id1 is the root (smallest)
        assert_eq!(assignments.get(&id2), Some(&id1));
        assert_eq!(assignments.get(&id3), Some(&id1));
        assert!(!assignments.contains_key(&id1)); // Root has no parent
    }

    #[test]
    fn test_tokenize() {
        let text = "hello world test document";
        let tokens = tokenize(text);
        assert_eq!(tokens.len(), 2); // 4 words -> 2 shingles
        assert_eq!(tokens[0], "hello world test");
        assert_eq!(tokens[1], "world test document");
    }

    #[test]
    fn test_take_pathological_batch_ids_only_returns_current_batch() {
        let id1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let id2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let id3 = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
        let historical_id = Uuid::parse_str("00000000-0000-0000-0000-000000000004").unwrap();

        let mut batch_ids = vec![id1, id2, id3];
        let pathological_ids = [id2, historical_id].into_iter().collect();

        let pathological_batch_ids = take_pathological_batch_ids(&mut batch_ids, &pathological_ids);

        assert_eq!(batch_ids, vec![id1, id3]);
        assert_eq!(pathological_batch_ids, vec![id2]);
    }

    #[test]
    fn test_resolve_transitivity_returns_sorted_assignments() {
        let root = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let child_a = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
        let child_b = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let matches = vec![
            MatchRecord {
                child_id: child_a,
                parent_id: root,
                jaccard_similarity: 0.9,
                size_difference: 1,
                size_difference_pct: 0.01,
            },
            MatchRecord {
                child_id: child_b,
                parent_id: root,
                jaccard_similarity: 0.9,
                size_difference: 1,
                size_difference_pct: 0.01,
            },
        ];

        let (resolved, _, _) = resolve_transitivity(&matches);
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].child_id, child_b);
        assert_eq!(resolved[1].child_id, child_a);
    }

    #[test]
    fn test_incremental_transitivity_prefers_existing_parent_over_lower_new_uuid() {
        let new_lower_uuid = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let existing_parent = Uuid::parse_str("00000000-0000-0000-0000-000000000010").unwrap();
        let existing_child = Uuid::parse_str("00000000-0000-0000-0000-000000000011").unwrap();

        let matches = vec![
            MatchRecord {
                child_id: existing_child,
                parent_id: existing_parent,
                jaccard_similarity: 0.95,
                size_difference: 1,
                size_difference_pct: 0.01,
            },
            MatchRecord {
                child_id: existing_parent,
                parent_id: new_lower_uuid,
                jaccard_similarity: 0.90,
                size_difference: 2,
                size_difference_pct: 0.02,
            },
        ];

        let preferred_parent_ids = [existing_parent].into_iter().collect();
        let new_doc_ids = [new_lower_uuid].into_iter().collect();
        let (resolved, parent_ids, child_ids) = resolve_transitivity_with_preferred_roots(
            &matches,
            &preferred_parent_ids,
            Some(&new_doc_ids),
        );

        assert!(parent_ids.contains(&existing_parent));
        assert!(!child_ids.contains(&existing_parent));
        assert!(resolved
            .iter()
            .any(|m| { m.child_id == new_lower_uuid && m.parent_id == existing_parent }));
        assert!(resolved
            .iter()
            .any(|m| { m.child_id == existing_child && m.parent_id == existing_parent }));
    }

    #[test]
    fn test_transitivity_metrics_are_tracked_for_both_edge_endpoints() {
        let raw_child_preferred_root =
            Uuid::parse_str("00000000-0000-0000-0000-000000000010").unwrap();
        let raw_parent_later_child =
            Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();

        let matches = vec![MatchRecord {
            child_id: raw_child_preferred_root,
            parent_id: raw_parent_later_child,
            jaccard_similarity: 0.93,
            size_difference: 7,
            size_difference_pct: 0.07,
        }];

        let preferred_parent_ids = [raw_child_preferred_root].into_iter().collect();
        let (resolved, _, _) =
            resolve_transitivity_with_preferred_roots(&matches, &preferred_parent_ids, None);

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].child_id, raw_parent_later_child);
        assert_eq!(resolved[0].parent_id, raw_child_preferred_root);
        assert!((resolved[0].jaccard_similarity - 0.93).abs() < 0.001);
        assert_eq!(resolved[0].size_difference, 7);
        assert!((resolved[0].size_difference_pct - 0.07).abs() < 0.001);
    }

    #[test]
    fn test_is_boilerplate_empty() {
        let no_patterns: Vec<String> = vec![];
        assert!(is_boilerplate("", &no_patterns));
        assert!(is_boilerplate("   ", &no_patterns));
        assert!(is_boilerplate("\n\n\n", &no_patterns));
    }

    #[test]
    fn test_is_boilerplate_with_patterns() {
        let patterns = vec![
            "{{".to_string(),
            "}}".to_string(),
            "${".to_string(),
            "[EN](/goEnSite)".to_string(),
            "/lw)".to_string(),
            "Powered by Discuz!".to_string(),
        ];
        assert!(is_boilerplate("Hello {{name}} world", &patterns));
        assert!(is_boilerplate("${variable} test", &patterns));
        assert!(is_boilerplate("Some text with }} in it", &patterns));
        assert!(is_boilerplate("[EN](/goEnSite) some content", &patterns));
        assert!(is_boilerplate(
            "* [论文助手](/lw) * [文献直达](/zd)",
            &patterns
        ));
        assert!(is_boilerplate(
            "|小黑屋|枞阳县人民医院 Powered by Discuz!",
            &patterns
        ));
    }

    #[test]
    fn test_is_boilerplate_navigation_heuristic() {
        // Many short lines = navigation (heuristic, no patterns needed)
        let no_patterns: Vec<String> = vec![];
        let nav_content = (0..30)
            .map(|i| format!("Link{}", i))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(is_boilerplate(&nav_content, &no_patterns));
    }

    #[test]
    fn test_is_boilerplate_normal_content() {
        // Normal content should NOT be boilerplate
        let no_patterns: Vec<String> = vec![];
        let article = "This is a normal article about something interesting. \
            It contains multiple sentences and has meaningful content. \
            The paragraphs are reasonably long and provide value to readers.";
        assert!(!is_boilerplate(article, &no_patterns));
    }

    #[test]
    fn test_is_boilerplate_pattern_with_newline() {
        // Patterns loaded from file can have \n converted to actual newlines
        let patterns = vec!["首页\n新闻".to_string()];
        assert!(is_boilerplate("首页\n新闻金昌要闻", &patterns));
    }
}
