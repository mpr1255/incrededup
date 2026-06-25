# Changelog

## 0.2.4 - 2026-06-24

Initial public release candidate.

Included:

1. Disk-backed MinHash LSH indexing with persistent `redb` sidecars.
2. PostgreSQL and SQLite CLI modes.
3. `DocumentSource` trait for custom sources.
4. `FileSystemSource` for text-like files on disk.
5. Incremental daemon mode for new PostgreSQL rows.
6. Transitive duplicate resolution with canonical parent assignment.
7. Optional adjacency side-index for large incremental syncs.
8. Regression coverage for MinHash, LSH, Phase 2 matching, sync invariants,
   source integrations, filesystem input, SQLite input, and PostgreSQL writes.
