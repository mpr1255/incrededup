//! Differential harness for the Phase 3 connected-edge lookup.
//!
//! `MatchStore::get_real_edges_connected_to` is the function we are about to
//! optimize: today it re-scans the entire `matches.redb` table (a full
//! `table.iter()`), repeatedly, until the touched component stops growing. On
//! the production `webcrawls` sidecar (29 GB) that single call dominates each
//! daemon batch (~158 s of a ~216 s batch). The planned replacement is an
//! adjacency side-index that finds the same component by point lookups.
//!
//! Before changing any behavior, this file pins down the *current* output as a
//! regression oracle. The safety contract for the optimization is exactly:
//!
//!   optimized_lookup(seeds)  ==SET==  full_scan_lookup(seeds)
//!
//! That equality is the whole merge gate. Downstream, the edge set is fed to
//! `resolve_transitivity`, which is a pure function of the edge set (plus
//! database-derived preference sets). So if the returned edge sets are equal,
//! every subsequent database write — dupe rows, parent marks, child marks — is
//! identical regardless of database state. We therefore assert edge-set
//! equality directly, and additionally confirm the resolved sync outputs match.
//!
//! The oracle is intentionally derived from a *different* code path than the
//! function under test: it reads raw records via `iter_real_matches` (a trivial
//! full dump) and computes the connected component with an in-test breadth-first
//! search. An independent second implementation is what makes the comparison
//! meaningful — a shared bug cannot hide in both.

use std::collections::{HashMap, HashSet};

use incrededup::{resolve_transitivity, DupeMatch, MatchRecord, MatchStore};
use tempfile::TempDir;
use uuid::Uuid;

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

/// Build a `MatchRecord` from compact numeric ids. Size fields are fixed; only
/// the (child, parent) topology and jaccard matter for these tests.
fn rec(child: u128, parent: u128, jaccard: f64) -> MatchRecord {
    MatchRecord {
        child_id: u(child),
        parent_id: u(parent),
        jaccard_similarity: jaccard,
        size_difference: 1,
        size_difference_pct: 0.01,
    }
}

/// Open a fresh on-disk `MatchStore` seeded with `edges`. The returned `TempDir`
/// must be held for the lifetime of the store (it owns the backing directory).
fn build_store(edges: &[MatchRecord]) -> (TempDir, MatchStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = MatchStore::open(dir.path().join("matches.redb")).unwrap();
    store.insert_batch(edges).unwrap();
    (dir, store)
}

/// Canonical, order-independent, exact representation of a set of match records.
/// Floats are compared by their bit pattern: both the function and the oracle
/// read the same stored bytes, so exact equality is the right check.
type RecordKey = (Uuid, Uuid, u64, i32, u64);

fn normalize_records(records: &[MatchRecord]) -> Vec<RecordKey> {
    let mut v: Vec<RecordKey> = records
        .iter()
        .map(|r| {
            (
                r.child_id,
                r.parent_id,
                r.jaccard_similarity.to_bits(),
                r.size_difference,
                r.size_difference_pct.to_bits(),
            )
        })
        .collect();
    v.sort();
    v
}

fn normalize_dupes(dupes: &[DupeMatch]) -> Vec<RecordKey> {
    let mut v: Vec<RecordKey> = dupes
        .iter()
        .map(|d| {
            (
                d.child_id,
                d.parent_id,
                d.jaccard_similarity.to_bits(),
                d.size_difference,
                d.size_difference_pct.to_bits(),
            )
        })
        .collect();
    v.sort();
    v
}

fn sorted_ids(ids: &HashSet<Uuid>) -> Vec<Uuid> {
    let mut v: Vec<Uuid> = ids.iter().copied().collect();
    v.sort();
    v
}

/// Independent oracle for `get_real_edges_connected_to`.
///
/// Source of records: `iter_real_matches` (full dump, no component logic).
/// Component logic: a plain BFS over the real-edge graph, written here in the
/// test. Returns every real edge with at least one endpoint in the component(s)
/// reachable from `seeds` — which is exactly the contract of the function under
/// test (any included edge pulls both endpoints into the component, so "at least
/// one endpoint in the component" and "both endpoints in the component" coincide
/// for the returned set).
fn oracle_connected_edges(store: &MatchStore, seeds: &[Uuid]) -> Vec<MatchRecord> {
    let all = store.iter_real_matches().unwrap();

    let mut adjacency: HashMap<Uuid, Vec<usize>> = HashMap::new();
    for (i, e) in all.iter().enumerate() {
        adjacency.entry(e.child_id).or_default().push(i);
        adjacency.entry(e.parent_id).or_default().push(i);
    }

    let mut component: HashSet<Uuid> = seeds.iter().copied().collect();
    let mut stack: Vec<Uuid> = seeds.to_vec();
    while let Some(node) = stack.pop() {
        if let Some(edge_idxs) = adjacency.get(&node) {
            for &i in edge_idxs {
                for endpoint in [all[i].child_id, all[i].parent_id] {
                    if component.insert(endpoint) {
                        stack.push(endpoint);
                    }
                }
            }
        }
    }

    all.into_iter()
        .filter(|e| component.contains(&e.child_id) || component.contains(&e.parent_id))
        .collect()
}

/// Assert the resolved sync outputs (dupe rows, parent set, child set) of two
/// edge sets are identical. This is the "same database writes" property.
fn assert_same_sync_outputs(a: &[MatchRecord], b: &[MatchRecord]) {
    let (dupes_a, parents_a, children_a) = resolve_transitivity(a);
    let (dupes_b, parents_b, children_b) = resolve_transitivity(b);
    assert_eq!(normalize_dupes(&dupes_a), normalize_dupes(&dupes_b));
    assert_eq!(sorted_ids(&parents_a), sorted_ids(&parents_b));
    assert_eq!(sorted_ids(&children_a), sorted_ids(&children_b));
}

/// Core assertion: the full-scan lookup, the adjacency-indexed lookup, and the
/// independent oracle all return the same edge set and resolve to the same
/// database writes. This is the merge gate for the optimization.
fn assert_lookup_matches_oracle(edges: &[MatchRecord], seeds: &[u128]) {
    let (_dir, store) = build_store(edges);
    let seed_ids: Vec<Uuid> = seeds.iter().map(|&n| u(n)).collect();

    let from_function = store.get_real_edges_connected_to(&seed_ids).unwrap();
    let from_oracle = oracle_connected_edges(&store, &seed_ids);
    assert_eq!(
        normalize_records(&from_function),
        normalize_records(&from_oracle),
        "full-scan lookup disagreed with the independent oracle for seeds {seeds:?}"
    );

    // The adjacency-indexed path must produce exactly the same edge set.
    store.build_adjacency_index().unwrap();
    let from_index = store
        .get_real_edges_connected_to_indexed(&seed_ids)
        .unwrap();
    assert_eq!(
        normalize_records(&from_index),
        normalize_records(&from_oracle),
        "indexed lookup disagreed with the independent oracle for seeds {seeds:?}"
    );

    // Equal edge sets must yield identical resolved sync outputs.
    assert_same_sync_outputs(&from_function, &from_oracle);
    assert_same_sync_outputs(&from_index, &from_function);
}

#[test]
fn single_chain_from_one_seed() {
    // a -> b -> c. Seeding any member returns the whole chain.
    let edges = [rec(2, 1, 0.90), rec(3, 2, 0.85)];
    assert_lookup_matches_oracle(&edges, &[1]);
    assert_lookup_matches_oracle(&edges, &[2]);
    assert_lookup_matches_oracle(&edges, &[3]);
}

#[test]
fn disjoint_components_only_return_the_seeded_one() {
    // Component {1,2,3} and component {10,11}. Seeding inside one must not pull
    // in the other.
    let edges = [rec(2, 1, 0.9), rec(3, 1, 0.9), rec(11, 10, 0.9)];
    assert_lookup_matches_oracle(&edges, &[1]);
    assert_lookup_matches_oracle(&edges, &[10]);
}

#[test]
fn new_doc_bridges_two_preexisting_clusters() {
    // The critical incremental case: two historical clusters that share no edge,
    // joined only because a freshly arrived doc (100) matches a member of each.
    // The lookup must surface edges from BOTH clusters plus the bridges, so
    // transitivity can collapse them into one component.
    let cluster_a = [rec(2, 1, 0.95), rec(3, 1, 0.92)];
    let cluster_b = [rec(11, 10, 0.93), rec(12, 10, 0.91)];
    let bridges = [rec(100, 2, 0.88), rec(100, 11, 0.87)];
    let edges: Vec<MatchRecord> = cluster_a
        .into_iter()
        .chain(cluster_b)
        .chain(bridges)
        .collect();
    assert_lookup_matches_oracle(&edges, &[100]);
}

#[test]
fn self_parent_edges_are_excluded() {
    // Legacy self-parent markers (child == parent) are not real duplicate edges
    // and must never appear in the result, even when mixed with real edges that
    // share an endpoint.
    let edges = [
        rec(1, 1, 1.0), // self edge, must be dropped
        rec(2, 1, 0.9), // real edge touching the same node
        rec(5, 5, 1.0), // isolated self edge
    ];
    assert_lookup_matches_oracle(&edges, &[1]);
    assert_lookup_matches_oracle(&edges, &[5]);
}

#[test]
fn isolated_seed_returns_no_edges() {
    let edges = [rec(2, 1, 0.9)];
    assert_lookup_matches_oracle(&edges, &[999]);

    // And an empty seed list returns nothing.
    let (_dir, store) = build_store(&edges);
    assert!(store.get_real_edges_connected_to(&[]).unwrap().is_empty());
}

#[test]
fn multiple_seeds_span_distinct_components() {
    let edges = [
        rec(2, 1, 0.9),
        rec(3, 1, 0.9),
        rec(11, 10, 0.9),
        rec(21, 20, 0.9),
    ];
    // Seed two of the three components; the third (20,21) must stay out.
    assert_lookup_matches_oracle(&edges, &[1, 10]);
}

#[test]
fn duplicate_keys_keep_the_higher_jaccard() {
    // matches.redb keeps the higher jaccard for an identical (child, parent)
    // key. The oracle reads the same stored record, so the lookup must return
    // the surviving (higher) score, not the originally-inserted one.
    let edges = [rec(2, 1, 0.70), rec(2, 1, 0.95), rec(3, 2, 0.80)];
    assert_lookup_matches_oracle(&edges, &[1]);

    // Sanity-check the store actually kept 0.95 for (2, 1).
    let (_dir, store) = build_store(&edges);
    let kept = store
        .get_real_edges_connected_to(&[u(1)])
        .unwrap()
        .into_iter()
        .find(|m| m.child_id == u(2) && m.parent_id == u(1))
        .expect("edge (2,1) should be present");
    assert!((kept.jaccard_similarity - 0.95).abs() < 1e-12);
}

#[test]
fn larger_tree_is_fully_recovered_from_any_seed() {
    // A deterministic 13-node tree: node k (2..=13) attaches to node k/2.
    // Whichever node we seed, the entire tree must come back.
    let mut edges = Vec::new();
    for k in 2..=13u128 {
        edges.push(rec(k, k / 2, 0.9));
    }
    for seed in [1u128, 7, 13] {
        assert_lookup_matches_oracle(&edges, &[seed]);
    }
}

#[test]
fn cycle_does_not_loop_forever_and_matches_oracle() {
    // a -> b -> c -> a forms a cycle. The fixed-point loop must terminate and
    // return all three edges.
    let edges = [rec(2, 1, 0.9), rec(3, 2, 0.9), rec(1, 3, 0.9)];
    assert_lookup_matches_oracle(&edges, &[1]);
}

#[test]
fn indexed_reader_performs_no_full_scan() {
    // The whole point of the index: a component lookup must not touch the full
    // matches table. The full-scan reader is checked too, as a sanity control.
    let edges = [rec(2, 1, 0.9), rec(3, 2, 0.9), rec(11, 10, 0.9)];
    let (_dir, store) = build_store(&edges);
    store.build_adjacency_index().unwrap();

    let before = store.full_scan_count();
    let _ = store.get_real_edges_connected_to_indexed(&[u(1)]).unwrap();
    assert_eq!(
        store.full_scan_count(),
        before,
        "indexed reader must not scan the full matches table"
    );

    // Control: the full-scan reader does increment the counter.
    let before = store.full_scan_count();
    let _ = store.get_real_edges_connected_to(&[u(1)]).unwrap();
    assert!(store.full_scan_count() > before);
}

#[test]
fn auto_falls_back_before_build_and_uses_index_after() {
    let edges = [rec(2, 1, 0.95), rec(3, 2, 0.90)];
    let (_dir, store) = build_store(&edges);
    let seeds = [u(1)];

    // Before any build, auto must fall back to the full scan and still be right.
    let (records_before, used_index_before) =
        store.get_real_edges_connected_to_auto(&seeds).unwrap();
    assert!(!used_index_before, "index must not be used before a build");
    assert_same_sync_outputs(
        &records_before,
        &store.get_real_edges_connected_to(&seeds).unwrap(),
    );

    // After a build, auto must use the index and return the same edge set.
    store.build_adjacency_index().unwrap();
    let (records_after, used_index_after) = store.get_real_edges_connected_to_auto(&seeds).unwrap();
    assert!(used_index_after, "index must be used after a build");
    assert_eq!(
        normalize_records(&records_after),
        normalize_records(&records_before),
    );
}

#[test]
fn build_is_idempotent() {
    let edges = [rec(2, 1, 0.9), rec(3, 1, 0.9), rec(4, 3, 0.9)];
    let (_dir, store) = build_store(&edges);

    let first = store.build_adjacency_index().unwrap();
    let count_after_first = store.adjacency_entry_count().unwrap();
    let second = store.build_adjacency_index().unwrap();
    let count_after_second = store.adjacency_entry_count().unwrap();

    // 3 real edges -> 6 adjacency entries, stable across rebuilds. The test
    // store uses the maintain-on-write path, so the builder has nothing missing
    // to add.
    assert_eq!(first.edges_indexed, 3);
    assert_eq!(first.entries_written, 0);
    assert_eq!(second.edges_indexed, 3);
    assert_eq!(second.entries_written, 0);
    assert_eq!(count_after_first, 6);
    assert_eq!(count_after_second, 6);

    assert_eq!(
        normalize_records(&store.get_real_edges_connected_to_indexed(&[u(1)]).unwrap()),
        normalize_records(&store.get_real_edges_connected_to(&[u(1)]).unwrap()),
    );
}

#[test]
fn maintain_on_write_keeps_index_current_without_rebuild() {
    // After the index is built, edges inserted later are indexed in the same
    // transaction as the matches write, so the indexed reader sees them with no
    // rebuild. This is what lets the daemon keep using the index incrementally.
    let (_dir, store) = build_store(&[rec(2, 1, 0.95)]);
    store.build_adjacency_index().unwrap();

    // A later batch bridges in a new doc (100) and extends the chain.
    store
        .insert_batch(&[rec(3, 2, 0.90), rec(100, 1, 0.88)])
        .unwrap();

    let seeds = [u(1)];
    let from_index = store.get_real_edges_connected_to_indexed(&seeds).unwrap();
    let from_scan = store.get_real_edges_connected_to(&seeds).unwrap();
    assert_eq!(
        normalize_records(&from_index),
        normalize_records(&from_scan),
        "maintain-on-write must keep the index consistent with the canonical matches"
    );
    // The new edges must actually be present.
    assert_eq!(from_index.len(), 3);
}
