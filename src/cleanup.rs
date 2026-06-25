//! Cleanup mode for finding and handling pathological clusters.
//!
//! Pathological clusters are groups of 1000+ nearly-identical documents that:
//! 1. Hash to the same LSH bucket across all 16 bands
//! 2. Are statistically guaranteed to have Jaccard similarity > 0.99
//! 3. Typically represent boilerplate, rate-limit pages, or error pages
//!
//! This module provides tools to detect and clean up these clusters.

use crate::lsh::DiskLSH;
use crate::sources::{DocumentSource, PostgresSource};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Action to take for pathological clusters
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupAction {
    /// Just report what would be done (dry run)
    Report,
    /// Mark all docs in cluster as parents (keeps them but excludes from dedup)
    MarkParent,
    /// Delete all but one doc from each cluster
    Delete,
}

impl std::str::FromStr for CleanupAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "report" | "dry-run" | "dryrun" => Ok(CleanupAction::Report),
            "mark" | "mark-parent" | "mark-self-parent" | "self-parent" => {
                Ok(CleanupAction::MarkParent)
            }
            "delete" | "remove" => Ok(CleanupAction::Delete),
            _ => Err(format!(
                "Unknown cleanup action '{}'. Use: report, mark-parent, or delete",
                s
            )),
        }
    }
}

/// Statistics from cleanup operation
#[derive(Debug, Default)]
pub struct CleanupStats {
    /// Number of pathological clusters found
    pub clusters_found: usize,
    /// Total documents in pathological clusters
    pub docs_in_clusters: usize,
    /// Documents that would be/were deleted (all but one per cluster)
    pub docs_to_delete: usize,
    /// Documents actually deleted
    pub docs_deleted: usize,
    /// Documents marked as parents
    pub docs_marked: usize,
}

/// A pathological cluster detected in the LSH index
#[derive(Debug)]
pub struct PathologicalCluster {
    /// Representative bucket key (from the first band where this cluster was found)
    pub bucket_key: String,
    /// Number of bands this cluster actually appears in (verified by doc ID intersection)
    pub band_count: usize,
    /// All document IDs in this cluster (intersection across all bands)
    pub doc_ids: Vec<Uuid>,
    /// Cluster size
    pub size: usize,
}

/// Find pathological clusters in an LSH index.
///
/// A pathological cluster is defined as:
/// - A group of documents that appear together in the same LSH bucket
/// - The SAME doc IDs appear in min_bands or more bands (verified by intersection)
/// - Cluster size >= threshold (default 10,000)
///
/// This function:
/// 1. Finds all large buckets (>= min_bucket_size docs)
/// 2. Groups buckets by band number (0-15)
/// 3. For each large bucket, checks if the same docs appear in other bands
/// 4. Reports clusters where docs appear in min_bands+ bands together
pub fn find_pathological_clusters(
    lsh: &DiskLSH,
    min_bucket_size: usize,
    min_bands: usize,
) -> Result<Vec<PathologicalCluster>> {
    info!(
        "Scanning for pathological clusters (bucket_size >= {}, min_bands >= {})",
        min_bucket_size, min_bands
    );

    // Get all large buckets with their band numbers
    let large_buckets = lsh.find_large_buckets(min_bucket_size, 10000)?;

    if large_buckets.is_empty() {
        info!(
            "[{}] No large buckets found",
            chrono::Local::now().format("%H:%M:%S")
        );
        return Ok(Vec::new());
    }

    info!(
        "[{}] Bucket scan complete. Found {} large buckets (>= {} docs each)",
        chrono::Local::now().format("%H:%M:%S"),
        large_buckets.len(),
        min_bucket_size
    );
    info!(
        "[{}] Starting cross-band overlap analysis...",
        chrono::Local::now().format("%H:%M:%S")
    );

    // Group buckets by band number
    // Key format is "band_num:hash_value", e.g., "13:1091413489984564584"
    let mut buckets_by_band: HashMap<usize, Vec<(String, usize)>> = HashMap::new();
    for (key, count, _) in &large_buckets {
        if let Some(band_str) = key.split(':').next() {
            if let Ok(band_num) = band_str.parse::<usize>() {
                buckets_by_band
                    .entry(band_num)
                    .or_default()
                    .push((key.clone(), *count));
            }
        }
    }

    info!(
        "Large buckets by band: {:?}",
        buckets_by_band
            .iter()
            .map(|(b, v)| (*b, v.len()))
            .collect::<Vec<_>>()
    );

    info!(
        "[{}] Loading large bucket memberships once...",
        chrono::Local::now().format("%H:%M:%S")
    );
    let mut bucket_doc_sets: HashMap<String, HashSet<Uuid>> = HashMap::new();
    for (key, _, _) in &large_buckets {
        bucket_doc_sets.insert(key.clone(), lsh.get_bucket_docs(key)?.into_iter().collect());
    }

    // Track which doc sets we've already processed to avoid duplicates
    let mut processed_doc_sets: Vec<HashSet<Uuid>> = Vec::new();
    let mut clusters = Vec::new();

    info!(
        "[{}] Analyzing cross-band overlap for {} large buckets...",
        chrono::Local::now().format("%H:%M:%S"),
        large_buckets.len()
    );

    // For each large bucket, check if its docs appear together in other bands
    let mut analyzed_count = 0;
    for (key, count, _) in &large_buckets {
        analyzed_count += 1;
        if analyzed_count % 100 == 0 || analyzed_count == large_buckets.len() {
            info!(
                "[{}] Analyzed {}/{} large buckets, found {} pathological clusters",
                chrono::Local::now().format("%H:%M:%S"),
                analyzed_count,
                large_buckets.len(),
                clusters.len()
            );
        }
        let Some(bucket_docs) = bucket_doc_sets.get(key) else {
            continue;
        };

        if bucket_docs.len() < min_bucket_size {
            continue;
        }

        // Skip if we've already processed a similar doc set
        // (Two doc sets are "similar" if they share >90% of docs)
        let already_processed = processed_doc_sets.iter().any(|existing| {
            let intersection = bucket_docs.intersection(existing).count();
            let union = bucket_docs.union(existing).count();
            union > 0 && (intersection as f64 / union as f64) > 0.9
        });

        if already_processed {
            debug!(
                "Skipping bucket {} - similar doc set already processed",
                key
            );
            continue;
        }

        // Parse band number from this bucket
        let this_band: usize = key
            .split(':')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Now check how many OTHER bands contain these same docs and keep only
        // the documents verified across the counted bands. Counting bands
        // without intersecting can mark different 10% misses as duplicates.
        let mut bands_with_overlap = vec![this_band];
        let mut verified_docs = bucket_docs.clone();

        for (other_band, other_buckets) in &buckets_by_band {
            if *other_band == this_band {
                continue;
            }

            // Check each bucket in this other band
            for (other_key, _) in other_buckets {
                let Some(other_docs) = bucket_doc_sets.get(other_key) else {
                    continue;
                };

                // Calculate overlap: what fraction of our docs appear in this bucket?
                let intersection = bucket_docs.intersection(other_docs).count();
                let overlap_ratio = intersection as f64 / bucket_docs.len() as f64;

                // If >= 90% of our docs appear in this bucket, count this band
                if overlap_ratio >= 0.9 {
                    bands_with_overlap.push(*other_band);
                    verified_docs = verified_docs.intersection(other_docs).copied().collect();
                    debug!(
                        "Bucket {} has {:.1}% overlap with band {} bucket {}",
                        key,
                        overlap_ratio * 100.0,
                        other_band,
                        other_key
                    );
                    break; // Only count each band once
                }
            }

            if bands_with_overlap.len() >= min_bands {
                break;
            }
        }

        bands_with_overlap.sort();
        bands_with_overlap.dedup();

        if bands_with_overlap.len() >= min_bands && verified_docs.len() >= min_bucket_size {
            info!(
                "Found pathological cluster: {} docs appearing in {} bands {:?}",
                verified_docs.len(),
                bands_with_overlap.len(),
                bands_with_overlap
            );

            // Mark this doc set as processed
            processed_doc_sets.push(verified_docs.clone());

            clusters.push(PathologicalCluster {
                bucket_key: key.clone(),
                band_count: bands_with_overlap.len(),
                size: verified_docs.len(),
                doc_ids: verified_docs.into_iter().collect(),
            });
        } else {
            debug!(
                "Bucket {} ({} docs) only has {} verified docs across {} bands - not pathological",
                key,
                count,
                verified_docs.len(),
                bands_with_overlap.len()
            );
        }
    }

    // Sort by size descending
    clusters.sort_by_key(|c| std::cmp::Reverse(c.size));

    info!(
        "Found {} verified pathological clusters with {} total docs",
        clusters.len(),
        clusters.iter().map(|c| c.size).sum::<usize>()
    );

    Ok(clusters)
}

/// Run Phase 1.5: Pathological Cluster Detection and Handling.
///
/// This is a modular function that can be called by any mode (--postgres, --from-index, etc.).
/// It:
/// 1. Opens the LSH index
/// 2. Detects pathological clusters (docs in 14+/16 bands = guaranteed >0.99 Jaccard)
/// 3. Writes child→canonical records to matches.redb
/// 4. Returns the set of doc IDs to SKIP in Phase 2 (avoids O(n²) comparisons)
///
/// # Arguments
/// * `lsh` - The LSH index (caller must have it open)
/// * `output_dir` - Directory containing matches.redb
/// * `min_bucket_size` - Minimum bucket size to check (default: 1000)
/// * `min_bands` - Minimum band overlap required (default: 14 of 16)
///
/// # Returns
/// HashSet of doc IDs that were handled (should be skipped in Phase 2)
pub fn run_phase_1_5(
    lsh: &DiskLSH,
    output_dir: &std::path::Path,
    min_bucket_size: usize,
    min_bands: usize,
) -> Result<HashSet<Uuid>> {
    use crate::storage::{MatchRecord, MatchStore};

    info!("");
    info!("=== Phase 1.5: Pathological Cluster Detection ===");
    info!(
        "[{}] Starting pathological cluster detection (bucket_size >= {}, bands >= {}/16)...",
        chrono::Local::now().format("%H:%M:%S"),
        min_bucket_size,
        min_bands
    );

    let clusters = find_pathological_clusters(lsh, min_bucket_size, min_bands)?;

    if clusters.is_empty() {
        info!(
            "[{}] No pathological clusters found.",
            chrono::Local::now().format("%H:%M:%S")
        );
        return Ok(HashSet::new());
    }

    let total_pathological: usize = clusters.iter().map(|c| c.size).sum();
    let largest_cluster = clusters.iter().map(|c| c.size).max().unwrap_or(0);
    let avg_cluster_size = if clusters.is_empty() {
        0
    } else {
        total_pathological / clusters.len()
    };

    warn!(
        "[{}] Found {} pathological clusters ({} total docs, largest: {}, avg: {})",
        chrono::Local::now().format("%H:%M:%S"),
        clusters.len(),
        total_pathological,
        largest_cluster,
        avg_cluster_size
    );

    // Write pathological cluster records to matches.redb
    info!(
        "[{}] Writing pathological cluster records to matches.redb...",
        chrono::Local::now().format("%H:%M:%S")
    );
    std::fs::create_dir_all(output_dir)?;
    let matches_path = output_dir.join("matches.redb");
    let matches_store = MatchStore::open(&matches_path)?;

    let mut pathological_ids: HashSet<Uuid> = HashSet::new();
    let mut records_written = 0;

    for cluster in &clusters {
        // Pick canonical parent (lexicographically smallest UUID)
        let canonical = cluster.doc_ids.iter().min().copied().unwrap();

        // Track all docs in cluster (to skip in Phase 2)
        pathological_ids.extend(cluster.doc_ids.iter().copied());

        // Write child→canonical records for all non-canonical docs
        // Canonical parent needs no record - it becomes a parent naturally
        let child_records: Vec<MatchRecord> = cluster
            .doc_ids
            .iter()
            .filter(|&&id| id != canonical)
            .map(|&id| MatchRecord {
                child_id: id,
                parent_id: canonical,
                jaccard_similarity: 0.99, // Guaranteed by 14+/16 band overlap
                size_difference: 0,
                size_difference_pct: 0.0,
            })
            .collect();

        records_written += matches_store.insert_batch(&child_records)?;
    }

    info!(
        "[{}] Wrote {} pathological cluster records ({} clusters, {} canonical parents)",
        chrono::Local::now().format("%H:%M:%S"),
        records_written,
        clusters.len(),
        clusters.len()
    );
    info!(
        "[{}] Phase 1.5 complete. {} docs will be skipped in Phase 2.",
        chrono::Local::now().format("%H:%M:%S"),
        pathological_ids.len()
    );

    Ok(pathological_ids)
}

/// Run cleanup on a dataset.
///
/// This function:
/// 1. Opens the LSH index for the dataset
/// 2. Finds pathological clusters (with verified cross-band overlap)
/// 3. Takes action based on the CleanupAction parameter
pub async fn run_cleanup(
    data_dir: &Path,
    source: Option<&PostgresSource>,
    action: CleanupAction,
    min_bucket_size: usize,
    min_bands: usize,
) -> Result<CleanupStats> {
    let lsh_path = data_dir.join("lsh.redb");

    if !lsh_path.exists() {
        anyhow::bail!("LSH index not found at {:?}", lsh_path);
    }

    info!("Opening LSH index: {:?}", lsh_path);
    let lsh = DiskLSH::open(&lsh_path)?;

    let total_docs = lsh.count()?;
    info!("Total documents in index: {}", total_docs);

    // Find pathological clusters
    let clusters = find_pathological_clusters(&lsh, min_bucket_size, min_bands)?;

    let mut stats = CleanupStats {
        clusters_found: clusters.len(),
        ..Default::default()
    };

    if clusters.is_empty() {
        info!("No pathological clusters found - nothing to clean up");
        return Ok(stats);
    }

    // Collect all affected doc IDs (deduplicated)
    let mut all_pathological_docs: HashSet<Uuid> = HashSet::new();
    for cluster in &clusters {
        all_pathological_docs.extend(&cluster.doc_ids);
    }
    stats.docs_in_clusters = all_pathological_docs.len();

    // Report findings
    info!("\n{}", "=".repeat(80));
    info!("PATHOLOGICAL CLUSTERS FOUND (verified cross-band overlap)");
    info!("{}", "=".repeat(80));

    for cluster in &clusters {
        info!(
            "\n  Cluster: {} docs verified in {} of 16 bands",
            cluster.size, cluster.band_count
        );
        info!("    First bucket: {}", cluster.bucket_key);
        info!(
            "    Sample IDs: {}...",
            cluster
                .doc_ids
                .iter()
                .take(3)
                .map(|u| u.to_string()[..8].to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    info!("\n{}", "-".repeat(80));
    info!(
        "SUMMARY: {} clusters, {} total pathological docs ({:.1}% of index)",
        stats.clusters_found,
        stats.docs_in_clusters,
        100.0 * stats.docs_in_clusters as f64 / total_docs as f64
    );

    // For delete action, we keep one doc per cluster
    // Calculate how many would be deleted
    for cluster in &clusters {
        if cluster.doc_ids.len() > 1 {
            stats.docs_to_delete += cluster.doc_ids.len() - 1;
        }
    }

    match action {
        CleanupAction::Report => {
            info!(
                "\n[DRY RUN] Would delete {} docs (keeping 1 per cluster)",
                stats.docs_to_delete
            );
            info!("[DRY RUN] No changes made");
        }

        CleanupAction::MarkParent => {
            let source = source.ok_or_else(|| {
                anyhow::anyhow!("Database connection required for mark-parent action")
            })?;
            info!("\nMarking {} docs as parents...", stats.docs_in_clusters);

            let doc_ids: Vec<Uuid> = all_pathological_docs.into_iter().collect();
            for chunk in doc_ids.chunks(10_000) {
                let marked = source.mark_as_parents(chunk).await?;
                stats.docs_marked += marked as usize;
            }

            info!("Marked {} docs as parents", stats.docs_marked);
        }

        CleanupAction::Delete => {
            let source = source
                .ok_or_else(|| anyhow::anyhow!("Database connection required for delete action"))?;
            warn!("\nDELETING {} docs from database!", stats.docs_to_delete);

            for cluster in &clusters {
                if cluster.doc_ids.len() <= 1 {
                    continue;
                }

                // Keep the lexicographically smallest UUID as canonical
                let canonical = cluster.doc_ids.iter().min().unwrap();
                let to_delete: Vec<Uuid> = cluster
                    .doc_ids
                    .iter()
                    .filter(|id| *id != canonical)
                    .copied()
                    .collect();

                info!(
                    "  Cluster {}: keeping {}, deleting {} docs",
                    &cluster.bucket_key[..20.min(cluster.bucket_key.len())],
                    canonical,
                    to_delete.len()
                );

                // Delete in chunks
                for chunk in to_delete.chunks(10_000) {
                    let deleted = source.pool().delete_documents(chunk).await?;
                    stats.docs_deleted += deleted as usize;
                }
            }

            info!("Deleted {} docs total", stats.docs_deleted);
        }
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleanup_action_from_str() {
        assert_eq!(
            "report".parse::<CleanupAction>().unwrap(),
            CleanupAction::Report
        );
        assert_eq!(
            "dry-run".parse::<CleanupAction>().unwrap(),
            CleanupAction::Report
        );
        assert_eq!(
            "delete".parse::<CleanupAction>().unwrap(),
            CleanupAction::Delete
        );
        assert_eq!(
            "mark-parent".parse::<CleanupAction>().unwrap(),
            CleanupAction::MarkParent
        );
    }
}
