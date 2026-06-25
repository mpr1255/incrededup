# Architecture

This is a map for future maintainers and LLM agents. It is not the CLI
reference. Use `docs/cli.md` for the generated command-line contract and
`docs/CUSTOM_SOURCES.md` for source schemas.

## What it is

`incrededup` is a disk-backed, incremental near-duplicate detector for large
text corpora. It reads documents from a `DocumentSource`, writes MinHash LSH
state to `redb` sidecars, stores raw duplicate edges, resolves transitive
components, and writes one canonical parent assignment per duplicate child.

The normal incremental path avoids loading the full LSH index or full match
graph into memory. Standalone `--sync` is different: it loads the whole match
store so it can replay all sidecar results into PostgreSQL.

## Main flow

1. Phase 1 in `src/dedupe/mod.rs` fetches new documents, filters short or
   boilerplate text, and writes signatures plus LSH band buckets to `lsh.redb`.
2. Phase 1.5 in `src/cleanup.rs` detects large pathological LSH buckets and
   writes canonical raw edges for those clusters.
3. Phase 2 in `src/disk_dedupe.rs` queries `lsh.redb`, verifies candidates,
   writes raw edges to `matches.redb`, then marks processed IDs in `state.redb`.
4. Phase 3 in `src/dedupe/mod.rs` loads only edges connected to the current
   batch, resolves transitivity, writes `dupes`, and marks `is_parent`.

## Sidecars

Sidecars live under `<data-dir>/<source-name>/`.

`lsh.redb` stores signatures and LSH buckets.

`matches.redb` stores raw real duplicate edges keyed by `(child_id, parent_id)`.
It may also contain an adjacency side-index for faster connected-edge lookup.

`state.redb` stores Phase 2 processed IDs and standalone sync progress.

`filtered_parents.redb` stores UUIDs for short or boilerplate documents when
source writes are skipped.

Source names are chosen by the caller: PostgreSQL table name by default,
`--scope` for scoped PostgreSQL, dataset name for legacy datasets, and SQLite
file stem for `--sqlite`.

## Important files

`src/main.rs` owns CLI dispatch, daemon loops, standalone sync, inspection,
adjacency backfill, and cleanup entry points.

`src/dedupe/mod.rs` owns the full pipeline for `DocumentSource` inputs and the
incremental Phase 3 sync logic.

`src/disk_dedupe.rs` owns disk-backed Phase 2 matching and checkpoint order.

`src/storage/mod.rs` owns the `redb` stores, including match records,
adjacency entries, filtered parents, and sync progress.

`src/lsh/mod.rs` owns the on-disk LSH index.

`src/sources/` contains the `DocumentSource` trait plus PostgreSQL, SQLite, and
filesystem implementations.

`src/db/mod.rs` contains PostgreSQL connection pooling, table queries, writes,
and legacy dataset support.

## Mode groups

Full pipeline modes are PostgreSQL, scoped PostgreSQL, legacy dataset,
SQLite, and their daemon variants.

Sidecar-only modes are `--inspect` and `--build-adjacency`.

Replay mode is `--sync`, which writes sidecars back to PostgreSQL.

Cleanup mode is `--cleanup`; `report` is read-only, while `mark-parent` and
`delete` write to PostgreSQL.

## Non-obvious invariants

Phase 2 must write matches before marking IDs processed in `state.redb`.

`matches.redb` stores raw edges, not final components. Union-Find chooses the
canonical parent later.

Incremental Phase 3 should load connected edges for the current batch, not the
full historical match graph.

The adjacency table is a derived lookup index. `matches.redb` raw edges remain
the source of truth.

Standalone `--sync` sorts resolved assignments, parent IDs, and child IDs
before checkpointed writes so resume behavior is deterministic.

PostgreSQL `write_dupes` deletes and reinserts child rows inside a transaction
so it works with both historical composite uniqueness and the documented
child-primary-key schema.

Phase 2 is disk-backed. `--memory` and `--disk-lsh` are compatibility flags,
not separate active implementations.
