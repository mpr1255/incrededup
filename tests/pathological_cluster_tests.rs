//! Tests for Phase 1.5 pathological-cluster detection.

use incrededup::{find_pathological_clusters, DiskLSH, NUM_BANDS, NUM_PERM, ROWS_PER_BAND};
use uuid::Uuid;

#[test]
fn test_pathological_detection_requires_cross_band_intersection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let lsh_path = temp_dir.path().join("lsh.redb");
    let lsh = DiskLSH::open(&lsh_path).unwrap();

    let mut entries = Vec::new();
    for doc_idx in 0..1000u32 {
        let mut signature = vec![0u32; NUM_PERM];

        // Band 0 contains all docs. Every other band has a large bucket with
        // 990/1000 docs, but each band omits a different 10 docs. The old
        // detector counted those bands and wrote all 1000 source-bucket docs.
        // The fixed detector intersects the verified docs across bands, leaving
        // only 850 docs, below min_bucket_size=900.
        for band in 1..NUM_BANDS {
            let omitted_start = (band as u32 - 1) * 10;
            if (omitted_start..omitted_start + 10).contains(&doc_idx) {
                let start = band * ROWS_PER_BAND;
                for value in signature.iter_mut().skip(start).take(ROWS_PER_BAND) {
                    *value = 10_000 * band as u32 + doc_idx + 1;
                }
            }
        }

        entries.push((Uuid::from_u128(doc_idx as u128 + 1), signature, 1000usize));
    }

    lsh.insert_batch(&entries).unwrap();

    let clusters = find_pathological_clusters(&lsh, 900, 16).unwrap();
    assert!(
        clusters.is_empty(),
        "overlapping large buckets must not mark docs that are not in the cross-band intersection"
    );
}
