//! LSH (Locality-Sensitive Hashing) implementation with disk-backed storage.
//!
//! This module provides both in-memory and disk-backed LSH indices using redb.
//! The disk-backed version allows processing datasets larger than memory.

use crate::minhash::{calculate_band_hash, NUM_BANDS, NUM_PERM, ROWS_PER_BAND};
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

/// Table for storing band -> document IDs mapping
/// Key: band_index:band_hash (as string), Value: serialized Vec<Uuid>
const BAND_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("bands");

/// Table for storing document signatures
/// Key: doc_id (as string), Value: serialized signature
const SIGNATURE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("signatures");

/// Table for index metadata used to detect incompatible sidecar reuse.
const METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const METADATA_KEY: &str = "lsh_config";
const LSH_METADATA_VERSION: u32 = 1;
const TOKENIZER_VERSION: &str = "word_3_shingle_v1";
const HASH_VERSION: &str = "rminhash_fx_v1";
const DEFAULT_LEGACY_SEED: u64 = 42;

/// Metadata that determines whether an existing LSH sidecar can be reused.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LshMetadata {
    pub version: u32,
    pub seed: u64,
    pub num_perm: usize,
    pub num_bands: usize,
    pub rows_per_band: usize,
    pub tokenizer_version: String,
    pub hash_version: String,
}

impl LshMetadata {
    #[must_use]
    pub fn current(seed: u64, num_bands: usize, rows_per_band: usize) -> Self {
        Self {
            version: LSH_METADATA_VERSION,
            seed,
            num_perm: NUM_PERM,
            num_bands,
            rows_per_band,
            tokenizer_version: TOKENIZER_VERSION.to_string(),
            hash_version: HASH_VERSION.to_string(),
        }
    }
}

/// In-memory LSH index for fast lookups (used during batch processing)
#[derive(Debug, Clone)]
pub struct InMemoryLSH {
    /// Hash tables for each band: band_hash -> list of doc IDs
    pub hash_tables: Vec<FxHashMap<u64, Vec<Uuid>>>,
    /// Document signatures: doc_id -> signature
    pub signatures: FxHashMap<Uuid, Vec<u32>>,
    /// Configuration
    pub num_bands: usize,
    pub rows_per_band: usize,
}

impl Default for InMemoryLSH {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryLSH {
    /// Create a new in-memory LSH index with default parameters
    #[must_use]
    pub fn new() -> Self {
        Self::with_params(NUM_BANDS, ROWS_PER_BAND)
    }

    /// Create a new in-memory LSH index with custom parameters
    #[must_use]
    pub fn with_params(num_bands: usize, rows_per_band: usize) -> Self {
        let hash_tables = (0..num_bands).map(|_| FxHashMap::default()).collect();

        Self {
            hash_tables,
            signatures: FxHashMap::default(),
            num_bands,
            rows_per_band,
        }
    }

    /// Insert a document signature into the index
    pub fn insert(&mut self, doc_id: Uuid, signature: Vec<u32>) {
        let _ = self.try_insert(doc_id, signature);
    }

    /// Checked insert for callers that want malformed signatures reported.
    pub fn try_insert(&mut self, doc_id: Uuid, signature: Vec<u32>) -> Result<()> {
        validate_signature_len(&signature, self.num_bands, self.rows_per_band)?;

        // Store signature
        self.signatures.insert(doc_id, signature.clone());

        // Add to hash tables
        for (band_idx, table) in self.hash_tables.iter_mut().enumerate() {
            let start = band_idx * self.rows_per_band;
            let end = start + self.rows_per_band;
            let band_hash = calculate_band_hash(&signature[start..end]);
            table.entry(band_hash).or_default().push(doc_id);
        }

        Ok(())
    }

    /// Query for candidate duplicates (documents that share at least one band)
    #[must_use]
    pub fn query(&self, signature: &[u32]) -> Vec<Uuid> {
        self.try_query(signature).unwrap_or_default()
    }

    /// Checked query for callers that want malformed signatures reported.
    pub fn try_query(&self, signature: &[u32]) -> Result<Vec<Uuid>> {
        validate_signature_len(signature, self.num_bands, self.rows_per_band)?;
        let mut candidates = Vec::new();

        for (band_idx, table) in self.hash_tables.iter().enumerate() {
            let start = band_idx * self.rows_per_band;
            let end = start + self.rows_per_band;
            let band_hash = calculate_band_hash(&signature[start..end]);

            if let Some(doc_ids) = table.get(&band_hash) {
                candidates.extend(doc_ids.iter().copied());
            }
        }

        // Deduplicate
        candidates.sort();
        candidates.dedup();
        Ok(candidates)
    }

    /// Get a signature by document ID
    #[must_use]
    pub fn get_signature(&self, doc_id: &Uuid) -> Option<&Vec<u32>> {
        self.signatures.get(doc_id)
    }

    /// Get number of documents in index
    #[must_use]
    pub fn len(&self) -> usize {
        self.signatures.len()
    }

    /// Check if index is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.signatures.is_empty()
    }

    /// Get all document IDs
    pub fn doc_ids(&self) -> impl Iterator<Item = &Uuid> {
        self.signatures.keys()
    }
}

/// Document entry for disk storage
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DocumentEntry {
    pub signature: Vec<u32>,
    pub content_len: usize,
}

/// Disk-backed LSH index using redb
pub struct DiskLSH {
    db: Database,
    num_bands: usize,
    rows_per_band: usize,
}

fn validate_signature_len(signature: &[u32], num_bands: usize, rows_per_band: usize) -> Result<()> {
    let expected = num_bands
        .checked_mul(rows_per_band)
        .context("Invalid LSH band configuration")?;
    if expected == 0 || signature.len() != expected {
        anyhow::bail!(
            "Invalid signature length: got {}, expected {} ({} bands x {} rows)",
            signature.len(),
            expected,
            num_bands,
            rows_per_band
        );
    }
    Ok(())
}

impl DiskLSH {
    /// Open or create a disk-backed LSH index
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_params(path, NUM_BANDS, ROWS_PER_BAND)
    }

    /// Open or create with custom parameters
    pub fn open_with_params<P: AsRef<Path>>(
        path: P,
        num_bands: usize,
        rows_per_band: usize,
    ) -> Result<Self> {
        let db = Database::create(path.as_ref())
            .with_context(|| format!("Failed to create database at {:?}", path.as_ref()))?;

        // Initialize tables
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(BAND_TABLE)?;
            let _ = write_txn.open_table(SIGNATURE_TABLE)?;
            let _ = write_txn.open_table(METADATA_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self {
            db,
            num_bands,
            rows_per_band,
        })
    }

    /// Validate or initialize sidecar metadata for this index.
    ///
    /// Legacy sidecars created before metadata existed are stamped as compatible
    /// only when the run uses the historical default seed. For custom seeds,
    /// there is no reliable way to know how the existing signatures were built,
    /// so callers should rebuild with `fresh`.
    pub fn validate_or_initialize_metadata(&self, seed: u64) -> Result<()> {
        let expected = LshMetadata::current(seed, self.num_bands, self.rows_per_band);

        let read_txn = self.db.begin_read()?;
        let metadata_table = read_txn.open_table(METADATA_TABLE)?;
        let existing = metadata_table
            .get(METADATA_KEY)?
            .map(|bytes| bincode::deserialize::<LshMetadata>(bytes.value()))
            .transpose()?;
        drop(read_txn);

        match existing {
            Some(existing) if existing == expected => Ok(()),
            Some(existing) => {
                anyhow::bail!(
                    "Existing LSH sidecar was built with incompatible metadata: {:?}; current run expects {:?}. Rebuild with fresh=true/--fresh.",
                    existing,
                    expected
                );
            }
            None => {
                let count = self.count()?;
                if count > 0 && seed != DEFAULT_LEGACY_SEED {
                    anyhow::bail!(
                        "Existing LSH sidecar has no metadata and current seed is {}. Cannot safely verify compatibility; rebuild with fresh=true/--fresh.",
                        seed
                    );
                }

                let write_txn = self.db.begin_write()?;
                {
                    let mut metadata_table = write_txn.open_table(METADATA_TABLE)?;
                    let bytes = bincode::serialize(&expected)?;
                    metadata_table.insert(METADATA_KEY, bytes.as_slice())?;
                }
                write_txn.commit()?;
                Ok(())
            }
        }
    }

    /// Insert a document signature into the index
    pub fn insert(&self, doc_id: Uuid, signature: Vec<u32>, content_len: usize) -> Result<()> {
        validate_signature_len(&signature, self.num_bands, self.rows_per_band)?;
        let write_txn = self.db.begin_write()?;

        {
            let mut band_table = write_txn.open_table(BAND_TABLE)?;
            let mut sig_table = write_txn.open_table(SIGNATURE_TABLE)?;

            // Store signature
            let entry = DocumentEntry {
                signature: signature.clone(),
                content_len,
            };
            let entry_bytes = bincode::serialize(&entry)?;
            sig_table.insert(doc_id.to_string().as_str(), entry_bytes.as_slice())?;

            // Add to band tables
            for band_idx in 0..self.num_bands {
                let start = band_idx * self.rows_per_band;
                let end = start + self.rows_per_band;
                let band_hash = calculate_band_hash(&signature[start..end]);

                let key = format!("{}:{}", band_idx, band_hash);

                // Get existing doc IDs for this band hash
                let mut doc_ids: Vec<Uuid> = if let Some(existing) = band_table.get(key.as_str())? {
                    bincode::deserialize(existing.value())?
                } else {
                    Vec::new()
                };

                if !doc_ids.contains(&doc_id) {
                    doc_ids.push(doc_id);
                }
                let doc_ids_bytes = bincode::serialize(&doc_ids)?;
                band_table.insert(key.as_str(), doc_ids_bytes.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Batch insert multiple documents (more efficient)
    pub fn insert_batch(&self, documents: &[(Uuid, Vec<u32>, usize)]) -> Result<()> {
        for (_, signature, _) in documents {
            validate_signature_len(signature, self.num_bands, self.rows_per_band)?;
        }

        let write_txn = self.db.begin_write()?;

        {
            let mut band_table = write_txn.open_table(BAND_TABLE)?;
            let mut sig_table = write_txn.open_table(SIGNATURE_TABLE)?;

            // First, collect all band updates
            let mut band_updates: FxHashMap<String, Vec<Uuid>> = FxHashMap::default();

            for (doc_id, signature, content_len) in documents {
                // Store signature
                let entry = DocumentEntry {
                    signature: signature.clone(),
                    content_len: *content_len,
                };
                let entry_bytes = bincode::serialize(&entry)?;
                sig_table.insert(doc_id.to_string().as_str(), entry_bytes.as_slice())?;

                // Collect band updates
                for band_idx in 0..self.num_bands {
                    let start = band_idx * self.rows_per_band;
                    let end = start + self.rows_per_band;
                    let band_hash = calculate_band_hash(&signature[start..end]);
                    let key = format!("{}:{}", band_idx, band_hash);
                    band_updates.entry(key).or_default().push(*doc_id);
                }
            }

            // Apply band updates
            for (key, new_ids) in band_updates {
                let mut doc_ids: Vec<Uuid> = if let Some(existing) = band_table.get(key.as_str())? {
                    bincode::deserialize(existing.value())?
                } else {
                    Vec::new()
                };
                doc_ids.extend(new_ids);
                doc_ids.sort();
                doc_ids.dedup();
                let doc_ids_bytes = bincode::serialize(&doc_ids)?;
                band_table.insert(key.as_str(), doc_ids_bytes.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Query for candidate duplicates
    pub fn query(&self, signature: &[u32]) -> Result<Vec<Uuid>> {
        validate_signature_len(signature, self.num_bands, self.rows_per_band)?;
        let read_txn = self.db.begin_read()?;
        let band_table = read_txn.open_table(BAND_TABLE)?;

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

    /// Get a document entry by ID
    pub fn get_document(&self, doc_id: &Uuid) -> Result<Option<DocumentEntry>> {
        let read_txn = self.db.begin_read()?;
        let sig_table = read_txn.open_table(SIGNATURE_TABLE)?;

        if let Some(entry_bytes) = sig_table.get(doc_id.to_string().as_str())? {
            let entry: DocumentEntry = bincode::deserialize(entry_bytes.value())?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    /// Get all document IDs
    pub fn all_doc_ids(&self) -> Result<Vec<Uuid>> {
        let read_txn = self.db.begin_read()?;
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

    /// Get count of documents
    pub fn count(&self) -> Result<u64> {
        let read_txn = self.db.begin_read()?;
        let sig_table = read_txn.open_table(SIGNATURE_TABLE)?;
        Ok(sig_table.len()?)
    }

    /// Compact the database (call periodically for performance)
    pub fn compact(&mut self) -> Result<()> {
        self.db.compact()?;
        Ok(())
    }

    /// Insert signatures only (no band updates) - O(1) per doc
    /// Call build_bands_from_signatures() after all signatures are inserted
    pub fn insert_signatures_only(&self, documents: &[(Uuid, Vec<u32>, usize)]) -> Result<()> {
        let write_txn = self.db.begin_write()?;

        {
            let mut sig_table = write_txn.open_table(SIGNATURE_TABLE)?;

            for (doc_id, signature, content_len) in documents {
                let entry = DocumentEntry {
                    signature: signature.clone(),
                    content_len: *content_len,
                };
                let entry_bytes = bincode::serialize(&entry)?;
                sig_table.insert(doc_id.to_string().as_str(), entry_bytes.as_slice())?;
            }
        }

        write_txn.commit()?;
        Ok(())
    }

    /// Build band index from all signatures in memory, then flush to disk
    /// This is O(n) and much faster than incremental band updates
    pub fn build_bands_from_signatures(&self) -> Result<usize> {
        // Load all signatures into memory
        let read_txn = self.db.begin_read()?;
        let sig_table = read_txn.open_table(SIGNATURE_TABLE)?;
        let sig_count = sig_table.len()?;

        let pb = ProgressBar::new(sig_count);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} ({per_sec}) Phase 1b: Loading signatures... ETA: {eta}")
                .unwrap(),
        );

        let mut all_sigs: Vec<(Uuid, Vec<u32>)> = Vec::with_capacity(sig_count as usize);
        for item in sig_table.iter()? {
            let (key, value) = item?;
            let doc_id = Uuid::parse_str(key.value())?;
            let entry: DocumentEntry = bincode::deserialize(value.value())?;
            all_sigs.push((doc_id, entry.signature));
            pb.inc(1);
        }
        drop(read_txn);
        pb.finish_with_message(format!("Loaded {} signatures", all_sigs.len()));

        let total_docs = all_sigs.len();
        if total_docs == 0 {
            return Ok(0);
        }

        // Build band buckets in memory with progress
        let pb2 = ProgressBar::new(total_docs as u64);
        pb2.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} ({per_sec}) Phase 1b: Building bands... ETA: {eta}")
                .unwrap(),
        );

        let mut band_buckets: FxHashMap<String, Vec<Uuid>> = FxHashMap::default();

        for (doc_id, signature) in &all_sigs {
            validate_signature_len(signature, self.num_bands, self.rows_per_band)?;
            for band_idx in 0..self.num_bands {
                let start = band_idx * self.rows_per_band;
                let end = start + self.rows_per_band;
                let band_hash = calculate_band_hash(&signature[start..end]);
                let key = format!("{}:{}", band_idx, band_hash);
                band_buckets.entry(key).or_default().push(*doc_id);
            }
            pb2.inc(1);
        }
        pb2.finish_with_message(format!("Built {} band buckets", band_buckets.len()));

        // Write all bands to disk in one transaction with progress
        let pb3 = ProgressBar::new(band_buckets.len() as u64);
        pb3.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} ({per_sec}) Phase 1b: Writing bands... ETA: {eta}")
                .unwrap(),
        );

        let write_txn = self.db.begin_write()?;
        {
            let mut band_table = write_txn.open_table(BAND_TABLE)?;

            for (key, doc_ids) in band_buckets {
                let doc_ids_bytes = bincode::serialize(&doc_ids)?;
                band_table.insert(key.as_str(), doc_ids_bytes.as_slice())?;
                pb3.inc(1);
            }
        }
        write_txn.commit()?;
        pb3.finish_with_message("Bands written to disk");

        Ok(total_docs)
    }

    /// Load all signatures into memory for fast lookups
    /// Returns HashMap of doc_id -> (signature, content_len)
    pub fn load_all_signatures(&self) -> Result<FxHashMap<Uuid, (Vec<u32>, usize)>> {
        let read_txn = self.db.begin_read()?;
        let sig_table = read_txn.open_table(SIGNATURE_TABLE)?;

        let mut signatures = FxHashMap::default();
        for item in sig_table.iter()? {
            let (key, value) = item?;
            let doc_id = Uuid::parse_str(key.value())?;
            let entry: DocumentEntry = bincode::deserialize(value.value())?;
            signatures.insert(doc_id, (entry.signature, entry.content_len));
        }

        Ok(signatures)
    }

    /// Check if bands have been built (by checking if band table is non-empty)
    pub fn has_bands(&self) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let band_table = read_txn.open_table(BAND_TABLE)?;
        Ok(band_table.len()? > 0)
    }

    /// Check if a document exists in the index
    pub fn has_document(&self, doc_id: &Uuid) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let sig_table = read_txn.open_table(SIGNATURE_TABLE)?;
        Ok(sig_table.get(doc_id.to_string().as_str())?.is_some())
    }

    /// Load entire index into memory for parallel deduplication
    /// Returns an InMemoryLSH populated with all signatures and bands
    pub fn load_into_memory(&self) -> Result<(InMemoryLSH, FxHashMap<Uuid, usize>)> {
        let read_txn = self.db.begin_read()?;
        let sig_table = read_txn.open_table(SIGNATURE_TABLE)?;
        let band_table = read_txn.open_table(BAND_TABLE)?;

        let sig_count = sig_table.len()?;
        let band_count = band_table.len()?;

        // Progress bar for loading signatures
        let pb = ProgressBar::new(sig_count);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({per_sec}) Loading signatures into memory... ETA: {eta}")
                .unwrap(),
        );

        // Load signatures and content lengths
        let mut signatures: FxHashMap<Uuid, Vec<u32>> = FxHashMap::default();
        let mut content_lens: FxHashMap<Uuid, usize> = FxHashMap::default();
        signatures.reserve(sig_count as usize);
        content_lens.reserve(sig_count as usize);

        for item in sig_table.iter()? {
            let (key, value) = item?;
            let doc_id = Uuid::parse_str(key.value())?;
            let entry: DocumentEntry = bincode::deserialize(value.value())?;
            signatures.insert(doc_id, entry.signature);
            content_lens.insert(doc_id, entry.content_len);
            pb.inc(1);
        }
        pb.finish_with_message(format!("Loaded {} signatures", signatures.len()));

        // Progress bar for loading bands
        let pb2 = ProgressBar::new(band_count);
        pb2.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.green/blue} {pos}/{len} ({per_sec}) Loading band buckets into memory... ETA: {eta}")
                .unwrap(),
        );

        // Load band buckets
        let mut hash_tables: Vec<FxHashMap<u64, Vec<Uuid>>> =
            (0..self.num_bands).map(|_| FxHashMap::default()).collect();

        for item in band_table.iter()? {
            let (key, value) = item?;
            let key_str = key.value();
            // Parse "band_idx:band_hash"
            if let Some((band_idx_str, band_hash_str)) = key_str.split_once(':') {
                if let (Ok(band_idx), Ok(band_hash)) =
                    (band_idx_str.parse::<usize>(), band_hash_str.parse::<u64>())
                {
                    if band_idx < self.num_bands {
                        let doc_ids: Vec<Uuid> = bincode::deserialize(value.value())?;
                        hash_tables[band_idx].insert(band_hash, doc_ids);
                    }
                }
            }
            pb2.inc(1);
        }
        pb2.finish_with_message(format!("Loaded {} band buckets", band_count));

        // Build InMemoryLSH
        let lsh = InMemoryLSH {
            hash_tables,
            signatures,
            num_bands: self.num_bands,
            rows_per_band: self.rows_per_band,
        };

        Ok((lsh, content_lens))
    }

    /// Find the largest band buckets (potential pathological clusters)
    /// Returns Vec of (bucket_key, doc_count, sample_doc_ids) sorted by count descending
    pub fn find_large_buckets(
        &self,
        min_size: usize,
        limit: usize,
    ) -> Result<Vec<(String, usize, Vec<Uuid>)>> {
        use chrono::Local;
        use tracing::info;

        info!(
            "[{}] Opening LSH index for bucket scan...",
            Local::now().format("%H:%M:%S")
        );
        let read_txn = self.db.begin_read()?;
        let band_table = read_txn.open_table(BAND_TABLE)?;

        // Get total bucket count for progress bar
        info!("[{}] Counting buckets...", Local::now().format("%H:%M:%S"));
        let total_buckets = band_table.len()?;
        info!(
            "[{}] Found {} buckets to scan",
            Local::now().format("%H:%M:%S"),
            total_buckets
        );

        let pb = ProgressBar::new(total_buckets);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} buckets ({per_sec}) {msg}")
                .unwrap()
                .progress_chars("█▓▒░ "),
        );
        pb.set_message("Scanning for large buckets...");

        let mut large_buckets: Vec<(String, usize, Vec<Uuid>)> = Vec::new();

        for item in band_table.iter()? {
            pb.inc(1);
            let (key, value) = item?;
            let doc_ids: Vec<Uuid> = bincode::deserialize(value.value())?;

            if doc_ids.len() >= min_size {
                let key_str = key.value().to_string();
                // Keep first 5 doc IDs as samples
                let samples: Vec<Uuid> = doc_ids.iter().take(5).copied().collect();
                large_buckets.push((key_str, doc_ids.len(), samples));
                pb.set_message(format!("Found {} large buckets", large_buckets.len()));
            }
        }

        pb.finish_with_message(format!(
            "Scanned {} buckets, found {} large (>= {} docs)",
            total_buckets,
            large_buckets.len(),
            min_size
        ));

        // Sort by size descending
        large_buckets.sort_by_key(|b| std::cmp::Reverse(b.1));
        large_buckets.truncate(limit);

        Ok(large_buckets)
    }

    /// Get all doc IDs in a specific bucket
    pub fn get_bucket_docs(&self, bucket_key: &str) -> Result<Vec<Uuid>> {
        let read_txn = self.db.begin_read()?;
        let band_table = read_txn.open_table(BAND_TABLE)?;

        if let Some(value) = band_table.get(bucket_key)? {
            let doc_ids: Vec<Uuid> = bincode::deserialize(value.value())?;
            Ok(doc_ids)
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::minhash::RMinHash;
    use tempfile::tempdir;

    #[test]
    fn test_in_memory_lsh_basic() {
        let mut lsh = InMemoryLSH::new();

        let mut mh1 = RMinHash::default();
        mh1.update(&["hello".to_string(), "world".to_string()]);

        let mut mh2 = RMinHash::default();
        mh2.update(&["hello".to_string(), "world".to_string()]);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        lsh.insert(id1, mh1.digest_owned());
        lsh.insert(id2, mh2.digest_owned());

        // Same content should find each other
        let candidates = lsh.query(mh1.digest());
        assert!(candidates.contains(&id1));
        assert!(candidates.contains(&id2));
    }

    #[test]
    fn test_in_memory_lsh_different() {
        let mut lsh = InMemoryLSH::new();

        let mut mh1 = RMinHash::default();
        mh1.update(&["completely".to_string(), "different".to_string()]);

        let mut mh2 = RMinHash::default();
        mh2.update(&["totally".to_string(), "unique".to_string()]);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        lsh.insert(id1, mh1.digest_owned());
        lsh.insert(id2, mh2.digest_owned());

        // Different content should not find each other (usually)
        let candidates = lsh.query(mh1.digest());
        assert!(candidates.contains(&id1)); // Should find itself
    }

    #[test]
    fn test_disk_lsh_basic() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.redb");

        let lsh = DiskLSH::open(&db_path)?;

        let mut mh = RMinHash::default();
        mh.update(&["test".to_string(), "document".to_string()]);

        let id = Uuid::new_v4();
        lsh.insert(id, mh.digest_owned(), 100)?;

        // Query should find it
        let candidates = lsh.query(mh.digest())?;
        assert!(candidates.contains(&id));

        // Get document should work
        let doc = lsh.get_document(&id)?;
        assert!(doc.is_some());
        assert_eq!(doc.unwrap().content_len, 100);

        Ok(())
    }

    #[test]
    fn test_disk_lsh_batch() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.redb");

        let lsh = DiskLSH::open(&db_path)?;

        let mut docs = Vec::new();
        for i in 0..100 {
            let mut mh = RMinHash::default();
            mh.update(&[format!("document {}", i)]);
            docs.push((Uuid::new_v4(), mh.digest_owned(), 100 + i));
        }

        lsh.insert_batch(&docs)?;

        assert_eq!(lsh.count()?, 100);

        Ok(())
    }

    #[test]
    fn test_disk_lsh_reinsert_does_not_duplicate_bucket_membership() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.redb");
        let lsh = DiskLSH::open(&db_path)?;

        let id = Uuid::new_v4();
        let sig = vec![42u32; NUM_BANDS * ROWS_PER_BAND];

        lsh.insert_batch(&[(id, sig.clone(), 100)])?;
        lsh.insert_batch(&[(id, sig.clone(), 100)])?;

        let candidates = lsh.query(&sig)?;
        assert_eq!(candidates, vec![id]);

        Ok(())
    }
}
