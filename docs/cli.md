# CLI reference

This file is generated from the current Clap definition in `src/cli.rs`. Do
not edit it by hand. Regenerate it with:

```bash
cargo run --example generate_cli_docs
```

Version: `incrededup 0.3.1`

The option order follows the declaration order in `src/cli.rs`: input and mode
selection first, then matching parameters, state/write controls, daemon
controls, and sidecar maintenance.

## Primary contract

```text
Performant, disk-based, incremental deduplication using MinHash LSH

Usage: incrededup [OPTIONS]

Options:
      --postgres
          Process a PostgreSQL table using DATABASE_URL.
          
          By default this processes the whole table. Use --scope/--scope-where when one physical table contains multiple logical corpora.

      --scope <SCOPE>
          Name for a logical subset of the PostgreSQL table.
          
          Requires --scope-where. The name is used for the sidecar directory.

      --scope-where <SCOPE_WHERE>
          Trusted SQL predicate for --scope, for example: corpus = 'news'.
          
          The predicate is appended to read/update queries as an additional WHERE condition. It should select a stable logical corpus.

  -d, --dataset <DATASET>
          Legacy dataset UUID or name to process (requires a datasets table and dataset_ids JSONB)

      --from-index <FROM_INDEX>
          Run deduplication from an existing LSH index.
          
          Point to an lsh.redb file or its containing sidecar directory.

      --output-dir <OUTPUT_DIR>
          Output directory for --from-index mode (default: same dir as index)

  -t, --threshold <THRESHOLD>
          Jaccard similarity threshold (0.0 - 1.0)
          
          [default: 0.8]

      --size-diff <SIZE_DIFF>
          Maximum size difference ratio to consider pairs
          
          [default: 0.3]

  -b, --batch-size <BATCH_SIZE>
          Database fetch batch size
          
          [default: 10000]

      --disk-lsh <DISK_LSH>
          Deprecated and ignored. Sidecars live under --data-dir/<source>/

      --seed <SEED>
          MinHash seed for reproducibility
          
          [default: 42]

      --table <TABLE>
          Database table name
          
          [default: documents]

  -w, --workers <WORKERS>
          Number of worker threads (default: number of CPUs)

      --dry-run
          Dry-run supported modes.
          
          PostgreSQL one-shot modes count documents only. --sync, including the auto-sync step after --from-index, resolves planned writes without writing. Other modes ignore this flag.

  -v, --verbose
          Verbose output

      --all
          Process all documents and rebuild local sidecars from scratch

      --data-dir <DATA_DIR>
          Base directory for data files (LSH index, matches, state)
          
          [default: ./data]

      --skip-db-write
          Skip writing duplicate results and is_parent updates to the source

      --memory
          Deprecated. Phase 2 always uses the disk-backed implementation

      --fresh
          Start fresh by clearing local sidecars before processing

      --no-sync
          Skip database sync after --from-index completes (by default, syncs to DB)

      --daemon
          Run in daemon mode - continuously poll for unprocessed documents.
          
          With --postgres, polls the configured table. With --sqlite, polls that SQLite database. Without either, uses the legacy multi-dataset PostgreSQL schema.

      --run-once
          Exit after one daemon pass instead of looping.
          
          Useful with cron and flock. Requires --daemon.

      --interval <INTERVAL>
          Polling interval in seconds for daemon mode (default: 5)
          
          [default: 5]

      --log-file <LOG_FILE>
          Log file path (for daemon mode). Logs to stdout if not specified

      --min-content-len <MIN_CONTENT_LEN>
          Minimum UTF-8 byte length to index.
          
          Shorter documents are skipped and marked as parents.
          
          [default: 500]

      --edge-lookup <EDGE_LOOKUP>
          Connected-edge lookup mode for Phase 3: scan, auto, or shadow

          Possible values:
          - scan:   Existing behavior: scan matches.redb for connected edges
          - auto:   Use the adjacency side-index once it has been fully backfilled
          - shadow: Compare adjacency output against scan output, but return scan output
          
          [default: scan]

      --max-matches-per-doc <MAX_MATCHES_PER_DOC>
          Keep only the top-M Phase 2 matches per processed document; 0 disables the cap
          
          [default: 0]

      --sync <SYNC>
          Sync matches to PostgreSQL with transitivity resolution.
          
          Point to a sidecar directory containing matches.redb.

      --inspect <INSPECT>
          Inspect matches.redb file contents.
          
          Point to a sidecar directory containing matches.redb.

      --build-adjacency <BUILD_ADJACENCY>
          Build the adjacency side-index for a source directory or matches.redb file

      --inspect-limit <INSPECT_LIMIT>
          Limit for --inspect mode (number of sample matches to show)
          
          [default: 20]

      --inspect-sample
          Show detailed samples in --inspect mode

      --sqlite <SQLITE>
          Use a SQLite database instead of PostgreSQL.
          
          Point to a .sqlite or .db file containing a documents table.

      --cleanup <CLEANUP>
          Detect and optionally handle pathological clusters.
          
          Point to a sidecar directory containing lsh.redb. Use --cleanup-action to choose report, mark-parent, or delete.

      --cleanup-action <CLEANUP_ACTION>
          Action to take in cleanup mode: report (dry-run), mark-parent, delete
          
          [default: report]

      --cleanup-min-bucket <CLEANUP_MIN_BUCKET>
          Minimum bucket size to consider pathological (default: 10000)
          
          [default: 10000]

      --cleanup-min-bands <CLEANUP_MIN_BANDS>
          Minimum number of LSH bands to consider pathological (default: 14 of 16)
          
          [default: 14]

      --keep-in-memory
          Keep index memory resident in daemon mode.
          
          By default, daemon releases memory after --memory-idle-timeout minutes of no activity.

      --memory-idle-timeout <MEMORY_IDLE_TIMEOUT>
          Minutes of idle time before releasing memory back to the OS.
          
          Set to 0 to release immediately after each batch. Ignored if --keep-in-memory is set
          
          [default: 60]

      --search-index-error-backoff-secs <SEARCH_INDEX_ERROR_BACKOFF_SECS>
          Seconds to back off after transient search-index corruption is detected
          
          [default: 600]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

## Runtime notes

1. PostgreSQL modes read `DATABASE_URL`.
2. SQLite modes take the database path from `--sqlite`.
3. Standalone `--sync` writes to PostgreSQL and reads `DATABASE_URL`.
4. `--cleanup report` only reads sidecars. `--cleanup mark-parent` and
   `--cleanup delete` write to PostgreSQL and read `DATABASE_URL`.
5. `--sync` uses `SYNC_WORKERS` for parent and child marking. Default: `8`.
6. Sidecars live under `<data-dir>/<source-name>/`.
7. `--inspect`, `--build-adjacency`, and `--cleanup report` do not require a
   database connection.
