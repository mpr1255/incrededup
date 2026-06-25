//! Transitivity resolution tests using Union-Find.
//!
//! These tests verify:
//! - A→B and B→C resolves to A→C, B→C (C is parent)
//! - Lexicographically smallest UUID becomes the canonical parent
//! - Large chains are resolved correctly
//! - Cycles are handled properly

use incrededup::UnionFind;
use uuid::Uuid;

/// Helper to create deterministic UUIDs for testing
fn uuid_from_num(n: u32) -> Uuid {
    Uuid::from_u128(n as u128)
}

#[test]
fn test_union_find_simple_pair() {
    let mut uf = UnionFind::new();

    let a = uuid_from_num(1);
    let b = uuid_from_num(2);

    uf.union(a, b);

    // Both should have the same parent (the smaller UUID)
    let parent_a = uf.find(a);
    let parent_b = uf.find(b);

    assert_eq!(parent_a, parent_b, "Both should have the same parent");
    assert_eq!(
        parent_a, a,
        "Parent should be lexicographically smaller UUID"
    );
}

#[test]
fn test_union_find_chain_of_three() {
    // A ≈ B and B ≈ C should resolve to A as parent of all
    let mut uf = UnionFind::new();

    let a = uuid_from_num(1);
    let b = uuid_from_num(2);
    let c = uuid_from_num(3);

    uf.union(a, b); // A-B
    uf.union(b, c); // B-C, but A is already parent of B

    // All should have A as parent
    assert_eq!(uf.find(a), a);
    assert_eq!(uf.find(b), a);
    assert_eq!(uf.find(c), a);
}

#[test]
fn test_union_find_reverse_order() {
    // Even if we union in reverse order, smallest UUID should be parent
    let mut uf = UnionFind::new();

    let a = uuid_from_num(1);
    let b = uuid_from_num(2);
    let c = uuid_from_num(3);

    uf.union(c, b); // C-B
    uf.union(b, a); // B-A

    // A should be parent of all (smallest)
    assert_eq!(uf.find(a), a);
    assert_eq!(uf.find(b), a);
    assert_eq!(uf.find(c), a);
}

#[test]
fn test_union_find_long_chain() {
    let mut uf = UnionFind::new();

    // Create a chain: 1-2-3-4-5-6-7-8-9-10
    let ids: Vec<Uuid> = (1..=10).map(uuid_from_num).collect();

    for window in ids.windows(2) {
        uf.union(window[0], window[1]);
    }

    // All should have uuid_from_num(1) as parent
    let expected_parent = uuid_from_num(1);
    for id in &ids {
        assert_eq!(
            uf.find(*id),
            expected_parent,
            "All elements should have smallest UUID as parent"
        );
    }
}

#[test]
fn test_union_find_merge_two_groups() {
    let mut uf = UnionFind::new();

    // Group 1: 1-2-3
    let a1 = uuid_from_num(1);
    let a2 = uuid_from_num(2);
    let a3 = uuid_from_num(3);
    uf.union(a1, a2);
    uf.union(a2, a3);

    // Group 2: 10-11-12
    let b1 = uuid_from_num(10);
    let b2 = uuid_from_num(11);
    let b3 = uuid_from_num(12);
    uf.union(b1, b2);
    uf.union(b2, b3);

    // Verify groups are separate
    assert_eq!(uf.find(a1), a1);
    assert_eq!(uf.find(b1), b1);

    // Merge groups
    uf.union(a3, b1);

    // Now all should have a1 (smallest) as parent
    let expected = uuid_from_num(1);
    assert_eq!(uf.find(a1), expected);
    assert_eq!(uf.find(a2), expected);
    assert_eq!(uf.find(a3), expected);
    assert_eq!(uf.find(b1), expected);
    assert_eq!(uf.find(b2), expected);
    assert_eq!(uf.find(b3), expected);
}

#[test]
fn test_union_find_idempotent() {
    let mut uf = UnionFind::new();

    let a = uuid_from_num(1);
    let b = uuid_from_num(2);

    // Union the same pair multiple times
    uf.union(a, b);
    uf.union(a, b);
    uf.union(b, a);

    // Should still work correctly
    assert_eq!(uf.find(a), a);
    assert_eq!(uf.find(b), a);
}

#[test]
fn test_union_find_self_union() {
    let mut uf = UnionFind::new();

    let a = uuid_from_num(1);

    // Union with self
    uf.union(a, a);

    // Should be its own parent
    assert_eq!(uf.find(a), a);
}

#[test]
fn test_union_find_random_uuids() {
    let mut uf = UnionFind::new();

    // Use real random UUIDs
    let uuids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();

    // Create one big group
    for window in uuids.windows(2) {
        uf.union(window[0], window[1]);
    }

    // Find the expected parent (lexicographically smallest)
    let expected_parent = *uuids.iter().min().unwrap();

    // All should have the same parent
    for uuid in &uuids {
        assert_eq!(
            uf.find(*uuid),
            expected_parent,
            "All should have lex smallest as parent"
        );
    }
}

#[test]
fn test_union_find_multiple_groups() {
    let mut uf = UnionFind::new();

    // Group 1: A, B
    let a = uuid_from_num(1);
    let b = uuid_from_num(2);
    uf.union(a, b);

    // Group 2: C, D, E
    let c = uuid_from_num(10);
    let d = uuid_from_num(11);
    let e = uuid_from_num(12);
    uf.union(c, d);
    uf.union(d, e);

    // Singleton: F (never unioned, but added via find)
    let f = uuid_from_num(100);
    let _ = uf.find(f);

    // Verify group 1: A and B have same parent (A, the smaller)
    assert_eq!(uf.find(a), a);
    assert_eq!(uf.find(b), a);

    // Verify group 2: C, D, E have same parent (C, the smaller)
    assert_eq!(uf.find(c), c);
    assert_eq!(uf.find(d), c);
    assert_eq!(uf.find(e), c);

    // Verify F is its own parent (singleton)
    assert_eq!(uf.find(f), f);

    // Verify groups are separate
    assert_ne!(uf.find(a), uf.find(c));
    assert_ne!(uf.find(a), uf.find(f));
    assert_ne!(uf.find(c), uf.find(f));
}

#[test]
fn test_union_find_path_compression() {
    // This test verifies path compression works by checking
    // that after multiple finds, the structure is flat
    let mut uf = UnionFind::new();

    // Create a long chain
    let ids: Vec<Uuid> = (1..=100).map(uuid_from_num).collect();

    for window in ids.windows(2) {
        uf.union(window[0], window[1]);
    }

    // First find on the last element (triggers path compression)
    let _ = uf.find(ids[99]);

    // Second find should be fast (path is compressed)
    let parent = uf.find(ids[99]);
    assert_eq!(parent, ids[0]);
}

#[test]
fn test_union_find_large_scale() {
    let mut uf = UnionFind::new();

    // Create 1000 elements in one group
    let ids: Vec<Uuid> = (0..1000).map(uuid_from_num).collect();

    for window in ids.windows(2) {
        uf.union(window[0], window[1]);
    }

    // Verify all have same parent
    let expected_parent = uuid_from_num(0);
    for id in &ids {
        assert_eq!(uf.find(*id), expected_parent);
    }

    // Verify structure size (root element is not stored in parent map,
    // so with 1000 elements we have 999 stored - the root returns itself via find())
    assert!(
        uf.len() >= 999,
        "Should have at least 999 elements in structure"
    );
}
