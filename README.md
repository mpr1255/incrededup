# incrededup

<table align="right" border="0" cellspacing="0" cellpadding="0">
<tr><td><img src="assets/logo.png" alt="incrededup mascot" width="150"></td></tr>
<tr><td align="center"><sub>incrededup mascot<br>courtesy of Nano Banana</sub></td></tr>
</table>

`incrededup` (**incre**mental **dedup**licator) is a disk-backed document deduplicator for large text
corpora. It builds persistent MinHash LSH sidecar indexes with `redb`, finds
near-duplicate edges, resolves transitive duplicate components, and writes a
single canonical assignment for each duplicate child.

It is meant for workflows where documents keep arriving over time and new rows
need to be compared against an existing corpus without rebuilding the whole
index, whether in memory or on disk.

The architecture is aimed at cases where memory is limited relative to the
dedupe target size. It has run in production against a PostgreSQL corpus of
about 50 million records, with `redb` sidecar indexes totaling about 0.9 TB on
disk. In this setup, the database is the source of truth and the `incrededup` sidecars hold the LSH
index, raw duplicate edges, and resumable processing state. Attempting to run this scale of workflow would otherwise require either a highly expensive cluster with a lot of memory or repeated batch rebuilds as new documents keep arriving.

`incrededup` ships as a CLI rather than library, because it keeps its own sidecar index and it seemed simpler to address it as a self-contained system.

This software is provided as-is, without any warranty or guarantee of any kind. All code was produced by a rotating cast of agents over ~six months. It has a lot of tests, and it reliably runs daily on real workloads, but there may be bugs and unexpected behaviors. The rest of this readme was produced by LLMs.

## Documentation

Start with the full CLI reference: [`docs/cli.md`](docs/cli.md).

Other public docs:

1. [`docs/CUSTOM_SOURCES.md`](docs/CUSTOM_SOURCES.md) explains source schemas
   and custom `DocumentSource` implementations.
2. [`docs/architecture.md`](docs/architecture.md) explains the internal
   pipeline and sidecar layout.

## What it supports

| Source | Interface | Writes results |
|---|---|---|
| PostgreSQL | CLI and `PostgresSource` | yes |
| SQLite | CLI and `SqliteSource` | yes |
| Text files | `FileSystemSource` | optional JSON report |
| Custom stores | `DocumentSource` trait | optional |

The filesystem source reads text-like files. It does not parse PDFs, Word
documents, HTML, images, or archives. Extract text with your own pipeline, then
pass the resulting strings, SQLite rows, PostgreSQL rows, or text files to
`incrededup`.

## Quick start: SQLite

This example can be run end-to-end. You can inspect the temporary database to
see what is being deduplicated and how.

```bash
cargo build --release

python3 scripts/generate_fake_sqlite.py /tmp/incrededup-demo.sqlite

./target/release/incrededup \
  --sqlite /tmp/incrededup-demo.sqlite \
  --data-dir /tmp/incrededup-index \
  --min-content-len 100

sqlite3 /tmp/incrededup-demo.sqlite \
  "SELECT child_id, parent_id, jaccard_similarity FROM dupes LIMIT 10;"
```

The SQLite source expects a `documents` table with UUID text IDs, document
content, content length, optional filename, and nullable `is_parent` state. See
`docs/CUSTOM_SOURCES.md` for the exact schema and custom-source options.

## Quick start: PostgreSQL

You need a running PostgreSQL database for this example.

```bash
export DATABASE_URL='postgresql://user:password@localhost:5432/documents'

./target/release/incrededup --postgres --data-dir /var/lib/incrededup

./target/release/incrededup \
  --postgres \
  --daemon \
  --data-dir /var/lib/incrededup \
  --interval 60
```

PostgreSQL mode expects:

```sql
CREATE TABLE documents (
    id UUID PRIMARY KEY,
    content TEXT NOT NULL,
    content_len INTEGER,
    filename TEXT,
    is_parent BOOLEAN
);

CREATE TABLE dupes (
    child_id UUID PRIMARY KEY,
    parent_id UUID NOT NULL,
    jaccard_similarity DOUBLE PRECISION NOT NULL,
    size_difference INTEGER,
    size_difference_pct DOUBLE PRECISION
);
```

Unprocessed documents have `is_parent IS NULL`. After a run, duplicate children
have `is_parent = false`; unique documents and canonical roots have
`is_parent = true`.

If one table carries several logical corpora, define a named scope:

```bash
./target/release/incrededup \
  --postgres \
  --scope court_opinions \
  --scope-where "corpus = 'court_opinions'"
```

The scope name is used for the sidecar directory. `--scope-where` is trusted SQL
added to the PostgreSQL `WHERE` clause, so it can match your existing schema:
`corpus = 'x'`, `source_id = '...'`, `project_ids ? '...'`, and so on.

`--dataset <name-or-uuid>` remains as a legacy shortcut for the original
incrededup deployment. It expects a `datasets` table and a `dataset_ids JSONB`
array on `documents`.

## Quick start: text files

```bash
cargo run --example text_files_demo
```

The example creates a temporary directory of `.txt` files, runs deduplication
through `FileSystemSource`, writes the same `redb` sidecars as the other
sources, and emits a JSON duplicate report. In production, file-heavy pipelines
usually extract text first and then either load it into PostgreSQL or SQLite for
the CLI, or call `FileSystemSource`/`DocumentSource` from a small Rust wrapper.

## Comparator contract

Duplicate detection is based on text similarity after a small, fixed
normalization step:

1. Content is split with Rust `split_whitespace()`.
2. Tokens with length 1 are ignored.
3. If at least three tokens remain, signatures are built from 3-word shingles.
   If fewer than three remain, the remaining tokens are used directly.
4. MinHash uses 128 permutations and the configured seed.
5. LSH uses 16 bands with 8 rows per band.
6. Candidate pairs are rejected when `abs(len_a - len_b) / max(len_a, len_b)`
   is greater than `--size-diff`. The default is `0.3`.
7. Candidate pairs are duplicates when signature Jaccard similarity is at least
   `--threshold`. The default is `0.8`.
8. The larger document is stored as the child. Ties use the document currently
   being processed as the child. Transitive sync later chooses the
   lexicographically smallest UUID in a component as the canonical parent.

The stored `jaccard_similarity` is the fraction of equal MinHash values across
the two signatures, not exact set Jaccard over source tokens.

## Comparison software

Most near-duplicate text tools use MinHash and LSH. The practical difference is
whether the index can live outside RAM, whether raw duplicate pairs are saved
for later transitive resolution, and whether new rows can be deduplicated
against an existing corpus without rebuilding everything.

Legend: ✓ yes, ◐ partial or requires surrounding application code, – no.

| Software | MinHash/LSH | Index outside RAM | Saves duplicate pairs | Adds new rows without rebuild | Writes DB results |
|---|---:|---:|---:|---:|---:|
| incrededup | ✓ | ✓ | ✓ | ✓ | ✓ |
| [Duplodocus](https://github.com/allenai/duplodocus) | ✓ | ✓ | ✓ | – | – |
| [DataTrove](https://github.com/huggingface/datatrove) | ✓ | ✓ | ✓ | ◐ | – |
| [text-dedup](https://github.com/ChenghaoMou/text-dedup) | ✓ | – | – | – | – |
| [datasketch](https://github.com/ekzhu/datasketch) | ✓ | ◐ | – | ◐ | – |
| [Rensa](https://github.com/beowolx/rensa) | ✓ | – | – | – | – |

`Index outside RAM` means the tool has a built-in disk, remote-file, or external
backend path for the MinHash/LSH index. `Saves duplicate pairs` means it writes
raw pairwise duplicate edges, even if only as batch intermediate files. `Adds
new rows without rebuild` means new documents can arrive after the first run and
be compared against existing corpus state without rebuilding the whole corpus.

Duplodocus and DataTrove are the closest batch comparators. `datasketch` and
Rensa are useful building blocks, but the corpus pipeline is yours. `text-dedup`
is convenient for dataset cleanup, but its MinHash path is not shaped as a
disk-backed database service.

Duplodocus is disk-backed, but batch-oriented: adding files means running the
corpus-level file-map, signature, edge, union-find, and clean stages again, not
appending to a live index.

This does not mean every operation has tiny peak memory. A genuinely large
duplicate component still has to be resolved in Phase 3, and that connected edge
set can be several GB. The design goal is narrower: do not require the full
MinHash/LSH index or full historical match graph to fit in RAM just to keep
deduplicating a growing corpus.

## Operating modes

See [`docs/cli.md`](docs/cli.md) for the complete CLI contract, including all
modes, flags, environment variables, sidecar paths, and write behavior.

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
side-index once, then switch daemon runs to `auto`:

```bash
./target/release/incrededup --build-adjacency /var/lib/incrededup/my_dataset
./target/release/incrededup --daemon --edge-lookup shadow
./target/release/incrededup --daemon --edge-lookup auto
```

Stop writers while `--build-adjacency` runs so `matches.redb` is stable. The
builder is resumable and marks the index usable only after a full backfill
completes.

## Development checks

```bash
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --doc
cargo run --example sqlite_demo
cargo run --example text_files_demo
cargo publish --dry-run --allow-dirty
```

The Postgres integration tests use a local Unix socket when available. In CI,
set `POSTGRES_TEST_URL`, for example:

```bash
POSTGRES_TEST_URL='postgresql://postgres:postgres@localhost:5432/postgres' \
  cargo test --test postgres_integration_tests
```

## License

Licensed under `MIT OR Apache-2.0`. See the canonical SPDX entries for
[`MIT`](https://spdx.org/licenses/MIT.html) and
[`Apache-2.0`](https://spdx.org/licenses/Apache-2.0.html).

## Acknowledgments

The MinHash and LSH implementation started from ideas in
`https://github.com/beowolx/rensa`.
