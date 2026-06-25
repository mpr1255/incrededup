# Operations

This page covers sidecars and large-store maintenance. Use `docs/cli.md` for
the complete command-line reference.

## Sidecar files

Each source gets a directory under `--data-dir`:

```text
lsh.redb               MinHash signatures and LSH buckets
matches.redb           raw duplicate edges and optional adjacency side-index
state.redb             resumable phase state
filtered_parents.redb  short or boilerplate docs when source writes are skipped
```

`matches.redb` stores real duplicate edges keyed by `(child_id, parent_id)`.
Unique documents are not written as self-parent edges; sync marks current-batch
documents with no child assignment as parents.

## Adjacency side-index

Large historical match stores can make incremental sync spend most of its time
scanning `matches.redb` for edges connected to the current batch. Build the
side-index once, then switch daemon runs to `auto` after validating with
`shadow`:

```bash
incrededup --build-adjacency /var/lib/incrededup/my_dataset
incrededup --daemon --edge-lookup shadow
incrededup --daemon --edge-lookup auto
```

Stop writers while `--build-adjacency` runs so `matches.redb` is stable. The
builder is resumable and marks the index usable only after a full backfill
completes.
