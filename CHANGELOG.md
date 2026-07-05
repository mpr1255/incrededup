# Changelog

## 0.3.1 - 2026-07-05

Fixed:

1. Replaced word shingling with the unicode char-3-gram tokenizer used by the
   Workbench deduper, fixing false negatives for Chinese and other text without
   whitespace-delimited words.
2. Refuse populated legacy LSH sidecars that lack tokenizer/hash metadata,
   because they cannot be proven compatible with the current tokenizer.
3. Stream Phase 3 connected-edge resolution from `matches.redb` instead of
   materializing the full component edge set in memory.
4. Added `--max-matches-per-doc` to bound per-document match fanout for very
   dense duplicate clusters.

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
