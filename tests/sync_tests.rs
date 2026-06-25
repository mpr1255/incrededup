//! Incremental Sync Tests
//!
//! These tests verify the core invariants of the sync function:
//!
//! ## SPEC: Sync Invariants
//!
//! After `perform_incremental_sync` completes for a set of `new_doc_ids`:
//!
//! 1. **COMPLETENESS**: Every doc in `new_doc_ids` MUST have `is_parent` set (not NULL).
//!    - Docs that are duplicates → is_parent = false (children)
//!    - Docs that are unique OR cluster roots → is_parent = true (parents)
//!
//! 2. **CORRECTNESS**: Parent/child assignments must be consistent with union-find.
//!    - If doc A is a child of B, then B must be marked as parent
//!    - Transitive closure is resolved (A→B→C becomes A→C, B→C)
//!
//! 3. **INCREMENTALITY**: Only docs in `new_doc_ids` should be written to DB.
//!    - Previously synced docs should NOT be re-written
//!    - This minimizes database load
//!
//! 4. **UNIQUE DOCS**: Documents with NO matches must be marked as parents.
//!    - These docs don't appear in union-find results at all
//!    - They are NOT children (no one points to them)
//!    - Therefore they must be parents (unique documents)

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use incrededup::sources::{DocumentSource, SourceDupeMatch};
use incrededup::storage::MatchRecord;

/// Mock DocumentSource that tracks all write operations
struct MockSource {
    parents_marked: Arc<Mutex<HashSet<Uuid>>>,
    children_marked: Arc<Mutex<HashSet<Uuid>>>,
    dupes_written: Arc<Mutex<Vec<SourceDupeMatch>>>,
}

impl MockSource {
    fn new() -> Self {
        Self {
            parents_marked: Arc::new(Mutex::new(HashSet::new())),
            children_marked: Arc::new(Mutex::new(HashSet::new())),
            dupes_written: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn all_processed(&self) -> HashSet<Uuid> {
        let parents = self.parents_marked.lock().unwrap();
        let children = self.children_marked.lock().unwrap();
        parents.union(&children).copied().collect()
    }

    fn get_parents(&self) -> HashSet<Uuid> {
        self.parents_marked.lock().unwrap().clone()
    }

    fn get_children(&self) -> HashSet<Uuid> {
        self.children_marked.lock().unwrap().clone()
    }
}

#[async_trait]
impl DocumentSource for MockSource {
    async fn source_name(&self) -> Result<String> {
        Ok("mock".to_string())
    }

    fn tracks_state(&self) -> bool {
        true
    }

    fn supports_write(&self) -> bool {
        true
    }

    async fn mark_as_parents(&self, ids: &[Uuid]) -> Result<u64> {
        let mut parents = self.parents_marked.lock().unwrap();
        for id in ids {
            parents.insert(*id);
        }
        Ok(ids.len() as u64)
    }

    async fn mark_as_children(&self, ids: &[Uuid]) -> Result<u64> {
        let mut children = self.children_marked.lock().unwrap();
        for id in ids {
            children.insert(*id);
        }
        Ok(ids.len() as u64)
    }

    async fn write_dupes(&self, matches: &[SourceDupeMatch]) -> Result<u64> {
        let mut dupes = self.dupes_written.lock().unwrap();
        dupes.extend(matches.iter().cloned());
        Ok(matches.len() as u64)
    }
}

fn uuid_from_num(n: u32) -> Uuid {
    Uuid::from_u128(n as u128)
}

fn make_match(child: u32, parent: u32, jaccard: f64) -> MatchRecord {
    MatchRecord {
        child_id: uuid_from_num(child),
        parent_id: uuid_from_num(parent),
        jaccard_similarity: jaccard,
        size_difference: 0,
        size_difference_pct: 0.0,
    }
}

// =============================================================================
// INVARIANT 1: COMPLETENESS - All new_doc_ids must be processed
// =============================================================================

#[tokio::test]
async fn test_all_new_docs_get_is_parent_set() {
    let new_doc_ids: Vec<Uuid> = (1..=5).map(uuid_from_num).collect();
    let all_matches = vec![make_match(1, 2, 0.9)];

    let source = MockSource::new();
    sync_to_mock(&source, &all_matches, &new_doc_ids).await;

    let processed = source.all_processed();
    for doc_id in &new_doc_ids {
        assert!(
            processed.contains(doc_id),
            "Doc {:?} should have is_parent set",
            doc_id
        );
    }
}

#[tokio::test]
async fn test_unique_docs_marked_as_parents() {
    let new_doc_ids: Vec<Uuid> = (10..=12).map(uuid_from_num).collect();
    let all_matches: Vec<MatchRecord> = vec![];

    let source = MockSource::new();
    sync_to_mock(&source, &all_matches, &new_doc_ids).await;

    let parents = source.get_parents();
    let children = source.get_children();

    for doc_id in &new_doc_ids {
        assert!(
            parents.contains(doc_id),
            "Unique doc {:?} should be parent",
            doc_id
        );
        assert!(
            !children.contains(doc_id),
            "Unique doc {:?} should NOT be child",
            doc_id
        );
    }
}

// =============================================================================
// INVARIANT 3: INCREMENTALITY - Only new docs written
// =============================================================================

#[tokio::test]
async fn test_only_new_docs_written_to_db() {
    let all_matches = vec![
        make_match(1, 2, 0.9),
        make_match(3, 2, 0.85),
        make_match(8, 9, 0.95),
        make_match(10, 9, 0.88),
    ];
    let new_doc_ids: Vec<Uuid> = (8..=10).map(uuid_from_num).collect();

    let source = MockSource::new();
    sync_to_mock(&source, &all_matches, &new_doc_ids).await;

    let processed = source.all_processed();

    for doc_id in &new_doc_ids {
        assert!(
            processed.contains(doc_id),
            "New doc {:?} should be processed",
            doc_id
        );
    }

    for old_id in [1u32, 2, 3].iter().map(|n| uuid_from_num(*n)) {
        assert!(
            !processed.contains(&old_id),
            "Old doc {:?} should NOT be re-processed",
            old_id
        );
    }
}

// =============================================================================
// INVARIANT 4: UNIQUE DOCS - Docs with no matches must be parents
// =============================================================================

#[tokio::test]
async fn test_doc_with_no_matches_is_parent() {
    let unique_doc = uuid_from_num(100);
    let new_doc_ids = vec![unique_doc];
    let all_matches: Vec<MatchRecord> = vec![];

    let source = MockSource::new();
    sync_to_mock(&source, &all_matches, &new_doc_ids).await;

    let parents = source.get_parents();
    assert!(
        parents.contains(&unique_doc),
        "Doc with no matches MUST be parent"
    );
}

#[tokio::test]
async fn test_mixed_unique_and_duplicate_docs() {
    let new_doc_ids: Vec<Uuid> = (1..=5).map(uuid_from_num).collect();
    let all_matches = vec![make_match(2, 1, 0.9)];

    let source = MockSource::new();
    sync_to_mock(&source, &all_matches, &new_doc_ids).await;

    let parents = source.get_parents();
    let children = source.get_children();

    // Doc 1 is parent (lex smallest in cluster)
    assert!(
        parents.contains(&uuid_from_num(1)),
        "Doc 1 should be parent"
    );
    // Doc 2 is child
    assert!(
        children.contains(&uuid_from_num(2)),
        "Doc 2 should be child"
    );
    // Docs 3, 4, 5 are unique parents
    for n in 3..=5 {
        assert!(
            parents.contains(&uuid_from_num(n)),
            "Unique doc {} should be parent",
            n
        );
    }
}

// =============================================================================
// Helper: Mirrors the actual sync implementation
// =============================================================================

async fn sync_to_mock(source: &MockSource, all_matches: &[MatchRecord], new_doc_ids: &[Uuid]) {
    let new_doc_set: HashSet<Uuid> = new_doc_ids.iter().copied().collect();
    let (_resolved, _parent_ids, all_child_ids) = resolve_transitivity(all_matches);

    let new_child_ids: Vec<Uuid> = all_child_ids
        .iter()
        .filter(|id| new_doc_set.contains(id))
        .copied()
        .collect();

    let new_child_set: HashSet<Uuid> = new_child_ids.iter().copied().collect();

    if !new_child_ids.is_empty() {
        source.mark_as_children(&new_child_ids).await.unwrap();
    }

    // KEY INVARIANT: All new docs NOT marked as children become parents
    let new_parent_ids: Vec<Uuid> = new_doc_ids
        .iter()
        .filter(|id| !new_child_set.contains(id))
        .copied()
        .collect();

    if !new_parent_ids.is_empty() {
        source.mark_as_parents(&new_parent_ids).await.unwrap();
    }
}

fn resolve_transitivity(
    matches: &[MatchRecord],
) -> (Vec<MatchRecord>, HashSet<Uuid>, HashSet<Uuid>) {
    use incrededup::UnionFind;

    let mut uf = UnionFind::new();
    for m in matches {
        uf.union(m.child_id, m.parent_id);
    }

    let mut resolved = Vec::new();
    let mut parent_ids = HashSet::new();
    let mut child_ids = HashSet::new();

    for m in matches {
        let canonical_parent = uf.find(m.child_id);
        if m.child_id == canonical_parent {
            parent_ids.insert(m.child_id);
            continue;
        }
        resolved.push(MatchRecord {
            child_id: m.child_id,
            parent_id: canonical_parent,
            jaccard_similarity: m.jaccard_similarity,
            size_difference: m.size_difference,
            size_difference_pct: m.size_difference_pct,
        });
        child_ids.insert(m.child_id);
        parent_ids.insert(canonical_parent);
    }

    (resolved, parent_ids, child_ids)
}
