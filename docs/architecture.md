# Architecture

This is the high-level map for `incrededup`. Pair it with `README.md` and
`docs/CUSTOM_SOURCES.md` for public usage details.

## 1. What this binary does

`incrededup` is a disk-backed, incremental document deduplicator. It reads documents from a `DocumentSource`, finds near-duplicates with MinHash + LSH, and writes one canonical `child_id -> parent_id` assignment back to the source when writes are enabled.

The normal incremental path is designed to avoid loading the full LSH index or full match graph into RAM. Its main disk stores are:

- `lsh.redb`: MinHash signatures and LSH band buckets for indexed documents.
- `matches.redb`: raw duplicate edges plus an optional adjacency side-index.
- `state.redb`: Phase 2 processed IDs and standalone sync progress.
- `filtered_parents.redb`: optional UUID-only sidecar used only when writes are skipped; see below.

Important caveat: not every CLI mode is dataset-size-independent in memory. The incremental pipeline reads only the match components touched by the current batch, but standalone `--sync` loads all raw matches and indexed document IDs, and non-incremental `--from-index` loads the index document ID list. Incremental Phase 3 can use the adjacency side-index through `--edge-lookup auto`; without a completed adjacency backfill it falls back to the historical full scan.

## 2. Pipeline

### Phase 1: Index Build (`src/dedupe/mod.rs`)

Phase 1 fetches documents in batches, filters documents that should not be indexed, and writes signatures and band memberships to `lsh.redb`.

Short and boilerplate documents are intentionally excluded from `lsh.redb` and `matches.redb`; they do not get self-edge records. With normal source writes enabled, Phase 1 marks their IDs as parents in bounded batches. With `--skip-db-write`, current code writes only their UUIDs to `filtered_parents.redb` so a later `--sync` can still mark them as parents. It does not store their content, signatures, or band memberships.

### Phase 1.5: Pathological Cluster Detection (`src/cleanup.rs`)

Phase 1.5 scans large LSH buckets, verifies that a cluster shares enough band membership across bands, writes canonical duplicate edges to `matches.redb`, and returns the affected IDs so Phase 2 can skip expensive per-document candidate checks for those documents.

### Phase 2: Duplicate Finding (`src/disk_dedupe.rs`)

Phase 2 uses Rayon over the current document ID list. Each worker opens a redb read transaction against `lsh.redb`, queries candidate buckets, applies the size and Jaccard filters, and appends raw `(child_id, parent_id)` matches to a shared pending buffer. `flush_pending_writes()` writes matches to `matches.redb` before marking document IDs processed in `state.redb`. If the adjacency side-index has been marked built, the same write transaction also maintains the two adjacency entries for each real edge.

`disk_dedupe.rs` writes `matches.redb` directly using the same on-disk key format as `storage::MatchStore`; it does not route Phase 2 writes through `MatchStore`.

### Phase 3: Sync (`src/dedupe/mod.rs`, `src/main.rs`)

The normal pipeline reads real duplicate edges connected to the current batch, resolves transitivity with Union-Find, writes only assignments for current-batch children, then marks current-batch children and parents in the source. `--edge-lookup scan` uses the historical full scan of `matches.redb`. `--edge-lookup auto` uses the adjacency table after `adjacency_built=true`, falling back to the scan before a completed backfill. `--edge-lookup shadow` computes both paths, returns the scan result, and logs whether the adjacency result is identical.

Standalone `--sync` is different: it loads all records from `matches.redb`, resolves transitivity for the full store, loads indexed IDs from `lsh.redb` for parent coverage, and also loads UUIDs from `filtered_parents.redb` if that sidecar exists.

## 3. Module Map

| Area | Main files |
|---|---|
| CLI, daemon loop, standalone sync | `src/main.rs` |
| Phase 1 orchestration and incremental Phase 3 sync | `src/dedupe/mod.rs` |
| Phase 2 disk-based duplicate finding | `src/disk_dedupe.rs` |
| Pathological cluster detection and cleanup mode | `src/cleanup.rs` |
| Disk LSH storage | `src/lsh/mod.rs` |
| MinHash and band hashing | `src/minhash/mod.rs`, `src/minhash/hasher.rs` |
| Match, filtered-parent, and sync state stores | `src/storage/mod.rs` |
| Transitive canonical parent selection | `src/union_find.rs` |
| PostgreSQL implementation | `src/db/mod.rs`, `src/sources/postgres.rs` |
| Source abstraction and non-Postgres sources | `src/sources/*` |

## 4. Store Lifecycle

| File | Written by | Read by | Notes |
|---|---|---|---|
| `lsh.redb` | Phase 1 | Phase 1.5, Phase 2, standalone `--sync` | Accumulates indexed docs. `--fresh` clears it. |
| `matches.redb` | Phase 1.5, Phase 2, `--build-adjacency` | Phase 3, `--inspect`, `--sync` | Stores raw real edges keyed by `(child_id, parent_id)` plus optional adjacency entries keyed by endpoint. New self-parent records are not written. `--fresh` clears it. |
| `state.redb` | Phase 2, standalone `--sync` | Phase 2 resume, `--sync` resume | `phase2_processed` and `sync_state` are separate tables. `--fresh` clears it. |
| `filtered_parents.redb` | Phase 1 only when source writes are disabled | standalone `--sync` | Optional UUID-only sidecar for filtered docs. It is not part of the normal write-enabled pipeline. `--fresh` clears it. |

## 5. Load-Bearing Invariants

- Filtered short/boilerplate docs never enter `lsh.redb` or `matches.redb`.
- Every document that remains in `new_doc_ids` reaches Phase 3 and is marked either child or parent when source writes are enabled.
- `matches.redb` stores raw edges keyed by `(child_id, parent_id)`. Union-Find chooses one canonical parent later.
- Incremental Phase 3 uses connected real edges, not the full historical match graph. The adjacency table is a lookup index, not a second source of truth.
- Phase 2 marks a document processed only after its pending matches have been committed.
- Standalone `--sync` sorts resolved assignments and parent/child ID lists before resumable write loops.

## 6. Concurrency Model

Rayon handles CPU-bound work:

- Phase 1 signature computation uses `par_iter`.
- Phase 2 runs inside a Rayon thread pool via `pool.install(|| doc_ids.par_iter()...)`.
- Phase 2 shared state is limited to pending writes, counters, and failure flags; each worker gets its own redb read transaction.

Tokio handles source I/O:

- `DocumentSource` calls are async.
- Standalone `--sync` uses `tokio::spawn` workers for fresh parent/child marking, and falls back to sequential loops when resuming from a partial mark step.

Plain PostgreSQL daemon mode processes the configured table. The legacy
multi-dataset daemon serializes per-dataset work with a transaction-level
PostgreSQL advisory lock (`pg_try_advisory_xact_lock`) held on a dedicated
connection for the duration of that dataset run.

## 7. `DocumentSource` Trait

The extension surface is:

- Reading: `count_total`, `count_unprocessed`, `fetch_all_after`, `fetch_unprocessed_ids_after`, `fetch_by_ids`.
- Writing: `write_dupes`, `mark_as_parents`, `mark_as_children`.
- Capabilities: `supports_write()`, `tracks_state()`.

SQL-backed sources share SQL construction in `src/sources/sql_dialect.rs` where possible.

## 8. Operating Modes

| Mode | Phase 1 | Phase 1.5 | Phase 2 | Phase 3 |
|---|---|---|---|---|
| `--postgres` | Yes | Yes | Yes | Yes |
| `--postgres --scope N --scope-where SQL` | Yes | Yes | Yes | Yes, scoped PostgreSQL |
| `--dataset N` | Yes | Yes | Yes | Yes, legacy dataset_ids filter |
| `--sqlite PATH` | Yes | Yes | Yes | Yes |
| `--daemon --postgres` | Yes | Yes | Yes | Yes, configured PostgreSQL table |
| `--daemon` | Yes | Yes | Yes | Yes, legacy per-dataset PostgreSQL |
| `--from-index PATH` | No | Yes | Yes | Yes unless `--no-sync` |
| `--sync PATH` | No | No | No | Yes, standalone full-store sync |
| `--inspect PATH` | No | No | No | No, read-only |
| `--cleanup PATH` | No | Aggressive cleanup scan | No | Direct mark/delete action |
| `--build-adjacency PATH` | No | No | No | No, redb side-index backfill |

## 9. Historical Failure Points

- Phase 2 checkpoint order matters: write matches first, then processed state.
- Match keys must include both child and parent IDs or transitive clusters lose raw edges.
- Incremental sync should not load all historical matches; use connected real edges for the current batch.
- Large production stores should use `--build-adjacency` plus `--edge-lookup auto` after shadow validation so Phase 3 does not scan every historical match edge.
- Standalone sync needs deterministic ordering before checkpointed write loops.
- `write_dupes` uses delete-then-insert in a transaction to handle both historical composite uniqueness and the documented child-primary-key shape. Keep dupe write chunks below the tokio-postgres bind parameter encoding limit.
- Pathological cluster verification must intersect membership across counted bands, not only check per-band overlap.
- Tokenization must handle content whose tokens would otherwise produce no shingles.

## 10. Reading Order

1. `README.md` for public CLI usage and comparator behavior.
2. `docs/CUSTOM_SOURCES.md` for source schemas and custom sources.
3. `src/dedupe/mod.rs` around `run_dedupe` and `run_dedupe_with_source`.
4. `src/disk_dedupe.rs` around `DiskDeduplicator::run`.
5. `src/storage/mod.rs` for the redb stores.
