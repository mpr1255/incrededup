//! Union-Find (Disjoint Set Union) data structure for transitivity resolution.
//!
//! This module provides a Union-Find implementation with path compression
//! and lexicographic ordering. It's used to resolve duplicate chains like
//! A->B->C into flat assignments A->C, B->C where C is the canonical parent.
//!
//! # Example
//!
//! ```
//! use incrededup::union_find::UnionFind;
//! use uuid::Uuid;
//!
//! let mut uf = UnionFind::new();
//!
//! let a = Uuid::new_v4();
//! let b = Uuid::new_v4();
//! let c = Uuid::new_v4();
//!
//! // Register a match between a and b
//! uf.make_set(a);
//! uf.make_set(b);
//! uf.union(a, b);
//!
//! // Both now have the same root (lexicographically smaller UUID)
//! assert_eq!(uf.find(a), uf.find(b));
//! ```

use std::collections::HashMap;
use uuid::Uuid;

/// Union-Find data structure for transitivity resolution.
///
/// Resolves chains like A->B->C to A->C, B->C (all children point to root parent).
/// Uses path compression for O(α(n)) amortized operations and lexicographic
/// ordering to ensure deterministic, consistent parent assignments.
#[derive(Debug, Default)]
pub struct UnionFind {
    parent: HashMap<Uuid, Uuid>,
}

impl UnionFind {
    /// Create a new empty UnionFind structure.
    pub fn new() -> Self {
        Self {
            parent: HashMap::new(),
        }
    }

    /// Create a UnionFind with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            parent: HashMap::with_capacity(capacity),
        }
    }

    /// Initialize a node as its own parent (idempotent).
    ///
    /// If the node already exists, this does nothing.
    pub fn make_set(&mut self, x: Uuid) {
        self.parent.entry(x).or_insert(x);
    }

    /// Find the root of a node with path compression.
    ///
    /// Path compression flattens the tree structure for faster subsequent lookups.
    /// Returns the node itself if it hasn't been added to any set.
    pub fn find(&mut self, x: Uuid) -> Uuid {
        if let Some(&p) = self.parent.get(&x) {
            if p != x {
                let root = self.find(p);
                self.parent.insert(x, root);
                return root;
            }
        }
        x
    }

    /// Union two sets, using lexicographically smaller UUID as root.
    ///
    /// This ensures deterministic parent assignments regardless of the order
    /// in which matches are processed.
    pub fn union(&mut self, x: Uuid, y: Uuid) {
        self.union_by_key(x, y, |id| id);
    }

    /// Union two sets, using a caller-provided ordering key to choose the root.
    ///
    /// The lower key wins. This is useful when deterministic roots need to be
    /// biased by external state, for example keeping an already-synced parent
    /// stable during incremental deduplication.
    pub fn union_by_key<K, F>(&mut self, x: Uuid, y: Uuid, root_key: F)
    where
        K: Ord,
        F: Fn(Uuid) -> K,
    {
        let root_x = self.find(x);
        let root_y = self.find(y);

        if root_x != root_y {
            if root_key(root_x) <= root_key(root_y) {
                self.parent.insert(root_y, root_x);
            } else {
                self.parent.insert(root_x, root_y);
            }
        }
    }

    /// Get all nodes in the UnionFind structure.
    pub fn nodes(&self) -> Vec<Uuid> {
        self.parent.keys().copied().collect()
    }

    /// Get the number of nodes in the structure.
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Check if the structure is empty.
    pub fn is_empty(&self) -> bool {
        self.parent.is_empty()
    }

    /// Build parent assignments from a list of duplicate pairs.
    ///
    /// This is a convenience method that:
    /// 1. Initializes all nodes from the pairs
    /// 2. Unions all pairs
    /// 3. Returns a map of child -> parent for all non-root nodes
    ///
    /// # Example
    ///
    /// ```
    /// use incrededup::union_find::UnionFind;
    /// use uuid::Uuid;
    ///
    /// let pairs = vec![
    ///     (Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap(),
    ///      Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()),
    /// ];
    ///
    /// let assignments = UnionFind::from_pairs(&pairs);
    /// // UUID ...002 maps to ...001 (lexicographically smaller)
    /// ```
    pub fn from_pairs(pairs: &[(Uuid, Uuid)]) -> HashMap<Uuid, Uuid> {
        if pairs.is_empty() {
            return HashMap::new();
        }

        let mut uf = Self::with_capacity(pairs.len() * 2);

        // Initialize each node as its own parent
        for &(child, parent) in pairs {
            uf.make_set(child);
            uf.make_set(parent);
        }

        // Union all pairs
        for &(child, parent) in pairs {
            uf.union(child, parent);
        }

        // Build final assignments (only non-root nodes)
        let all_nodes: Vec<Uuid> = uf.nodes();
        let mut assignments = HashMap::new();

        for node in all_nodes {
            let root = uf.find(node);
            if root != node {
                assignments.insert(node, root);
            }
        }

        assignments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(s: &str) -> Uuid {
        Uuid::parse_str(s).unwrap()
    }

    #[test]
    fn test_basic_union_find() {
        let mut uf = UnionFind::new();

        let a = uuid("00000000-0000-0000-0000-000000000001");
        let b = uuid("00000000-0000-0000-0000-000000000002");

        uf.make_set(a);
        uf.make_set(b);
        uf.union(a, b);

        // Both should have the same root (lexicographically smaller = a)
        assert_eq!(uf.find(a), a);
        assert_eq!(uf.find(b), a);
    }

    #[test]
    fn test_transitivity_chain() {
        let mut uf = UnionFind::new();

        let a = uuid("00000000-0000-0000-0000-000000000001");
        let b = uuid("00000000-0000-0000-0000-000000000002");
        let c = uuid("00000000-0000-0000-0000-000000000003");

        // Chain: a -> b -> c (but a is smallest, so becomes root)
        uf.make_set(a);
        uf.make_set(b);
        uf.make_set(c);

        uf.union(b, c); // b and c in same set
        uf.union(a, b); // a joins that set

        // All should point to a (lexicographically smallest)
        assert_eq!(uf.find(a), a);
        assert_eq!(uf.find(b), a);
        assert_eq!(uf.find(c), a);
    }

    #[test]
    fn test_from_pairs() {
        let a = uuid("00000000-0000-0000-0000-000000000001");
        let b = uuid("00000000-0000-0000-0000-000000000002");
        let c = uuid("00000000-0000-0000-0000-000000000003");

        let pairs = vec![(b, a), (c, b)];

        let assignments = UnionFind::from_pairs(&pairs);

        // b and c should both map to a
        assert_eq!(assignments.get(&b), Some(&a));
        assert_eq!(assignments.get(&c), Some(&a));
        // a is the root, so it's not in assignments
        assert_eq!(assignments.get(&a), None);
    }

    #[test]
    fn test_empty_pairs() {
        let assignments = UnionFind::from_pairs(&[]);
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_lexicographic_ordering() {
        let mut uf = UnionFind::new();

        // b is lexicographically larger than a
        let a = uuid("00000000-0000-0000-0000-000000000001");
        let b = uuid("ffffffff-ffff-ffff-ffff-ffffffffffff");

        uf.make_set(a);
        uf.make_set(b);
        uf.union(b, a); // order shouldn't matter

        // a should be root because it's lexicographically smaller
        assert_eq!(uf.find(a), a);
        assert_eq!(uf.find(b), a);
    }

    #[test]
    fn test_path_compression() {
        let mut uf = UnionFind::new();

        let a = uuid("00000000-0000-0000-0000-000000000001");
        let b = uuid("00000000-0000-0000-0000-000000000002");
        let c = uuid("00000000-0000-0000-0000-000000000003");
        let d = uuid("00000000-0000-0000-0000-000000000004");

        uf.make_set(a);
        uf.make_set(b);
        uf.make_set(c);
        uf.make_set(d);

        // Create chain: d -> c -> b -> a
        uf.union(b, a);
        uf.union(c, b);
        uf.union(d, c);

        // After finding d, path should be compressed
        assert_eq!(uf.find(d), a);

        // Now d should point directly to a (path compressed)
        assert_eq!(uf.find(d), a);
    }

    #[test]
    fn test_disjoint_sets() {
        let mut uf = UnionFind::new();

        let a = uuid("00000000-0000-0000-0000-000000000001");
        let b = uuid("00000000-0000-0000-0000-000000000002");
        let c = uuid("00000000-0000-0000-0000-000000000003");
        let d = uuid("00000000-0000-0000-0000-000000000004");

        uf.make_set(a);
        uf.make_set(b);
        uf.make_set(c);
        uf.make_set(d);

        // Two separate groups
        uf.union(a, b);
        uf.union(c, d);

        // a and b in one group
        assert_eq!(uf.find(a), uf.find(b));
        // c and d in another
        assert_eq!(uf.find(c), uf.find(d));
        // Groups are separate
        assert_ne!(uf.find(a), uf.find(c));
    }
}
