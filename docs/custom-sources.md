# custom sources

`incrededup` reads documents through the `DocumentSource` trait. The built-in
sources cover PostgreSQL, SQLite, and plain text-like files on disk.

The source boundary is text. If your corpus is PDF, DOCX, HTML, email, or some
other container format, extract text before passing documents to `incrededup`.
This crate deliberately does not own document parsing.

## SQLite schema

`SqliteSource` creates this schema when opening a new database:

```sql
CREATE TABLE documents (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    content_len INTEGER NOT NULL,
    filename TEXT,
    is_parent INTEGER
);

CREATE TABLE dupes (
    child_id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    jaccard_similarity REAL NOT NULL,
    size_difference INTEGER NOT NULL,
    size_difference_pct REAL NOT NULL
);
```

State values are:

1. `is_parent IS NULL`: unprocessed.
2. `is_parent = 1`: canonical parent or unique document.
3. `is_parent = 0`: duplicate child.

Run the included SQLite demo with:

```bash
cargo run --example sqlite_demo
```

## PostgreSQL schema

PostgreSQL mode expects a plain documents table:

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

`content_len` may be null; PostgreSQL queries can compute it from `content`.
Unprocessed documents have `is_parent IS NULL`.

If one physical table contains several logical corpora, use a named scope:

```bash
incrededup \
  --postgres \
  --scope court_opinions \
  --scope-where "corpus = 'court_opinions'"
```

The scope name becomes the sidecar directory name. `--scope-where` is trusted
SQL added to the PostgreSQL `WHERE` clause, so it can use your existing schema:
`corpus = 'x'`, `source_id = '...'`, `project_ids ? '...'`, and so on.

The older `--dataset` shortcut is still available for the original deployment
schema. It expects:

```sql
CREATE TABLE datasets (
    id UUID PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

ALTER TABLE documents ADD COLUMN dataset_ids JSONB;
```

`dataset_ids` should be a JSONB array containing dataset UUID strings.

Run the Postgres integration tests with:

```bash
POSTGRES_TEST_URL='postgresql://postgres:postgres@localhost:5432/postgres' \
  cargo test --test postgres_integration_tests
```

Without `POSTGRES_TEST_URL`, the tests try `/var/run/postgresql` and skip when
local Postgres is unavailable.

## Files on disk

`FileSystemSource` treats each selected file as one document. It reads file
contents as UTF-8, falling back to lossy UTF-8 for non-UTF-8 bytes.

```rust
use incrededup::{
    run_dedupe_with_source, DedupeConfig, FileSystemSource,
    sources::filesystem::FileSystemConfig,
};

let config = FileSystemConfig::new("/path/to/texts").with_extensions(vec!["txt"]);
let source = FileSystemSource::new(config).with_output_dir("/tmp/incrededup-report");
let dedupe = DedupeConfig::default();

let stats = run_dedupe_with_source(&source, dedupe, Some("text_files")).await?;
```

Run a complete temporary-file example with:

```bash
cargo run --example text_files_demo
```

## Custom source implementation

Implement `DocumentSource` when your storage does not match the built-in
schemas.

```rust
use async_trait::async_trait;
use incrededup::{DocumentSource, SourceDocument, SourceDupeMatch};
use uuid::Uuid;

pub struct MySource;

#[async_trait]
impl DocumentSource for MySource {
    async fn source_name(&self) -> anyhow::Result<String> {
        Ok("my_source".to_string())
    }

    async fn count_total(&self) -> anyhow::Result<i64> {
        todo!()
    }

    async fn count_unprocessed(&self) -> anyhow::Result<i64> {
        self.count_total().await
    }

    async fn fetch_all_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> anyhow::Result<Vec<SourceDocument>> {
        let _ = (last_id, limit);
        todo!()
    }

    async fn fetch_by_ids(&self, ids: &[Uuid]) -> anyhow::Result<Vec<SourceDocument>> {
        let _ = ids;
        todo!()
    }

    async fn write_dupes(&self, matches: &[SourceDupeMatch]) -> anyhow::Result<u64> {
        let _ = matches;
        Ok(0)
    }

    async fn mark_as_parents(&self, ids: &[Uuid]) -> anyhow::Result<u64> {
        let _ = ids;
        Ok(0)
    }

    async fn mark_as_children(&self, ids: &[Uuid]) -> anyhow::Result<u64> {
        let _ = ids;
        Ok(0)
    }

    fn supports_write(&self) -> bool {
        false
    }

    fn tracks_state(&self) -> bool {
        false
    }
}
```

If `tracks_state()` returns false, each run behaves like a full read of the
source. If `supports_write()` returns false, duplicate assignments are not
written back unless the source implements its own reporting behavior.
