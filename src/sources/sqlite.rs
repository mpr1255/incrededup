//! SQLite document source implementation.
//!
//! Provides a SQLite-backed document source for testing and lightweight deployments.
//! Creates the schema automatically if it doesn't exist.

use super::{DocumentSource, SourceDocument, SourceDupeMatch, SqlDialect};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use uuid::Uuid;

const DOCUMENT_SELECT_COLUMNS: &str =
    "id, COALESCE(content, ''), COALESCE(content_len, length(COALESCE(content, ''))), filename";

/// SQLite document source.
///
/// Uses a SQLite database to store documents. The schema mirrors the PostgreSQL
/// schema for compatibility:
///
/// ```sql
/// CREATE TABLE documents (
///     id TEXT PRIMARY KEY,  -- UUID as text
///     content TEXT NOT NULL,
///     content_len INTEGER NOT NULL,
///     filename TEXT,
///     is_parent INTEGER  -- NULL=unprocessed, 1=parent, 0=child
/// );
///
/// CREATE TABLE dupes (
///     child_id TEXT PRIMARY KEY,
///     parent_id TEXT NOT NULL,
///     jaccard_similarity REAL NOT NULL,
///     size_difference INTEGER NOT NULL,
///     size_difference_pct REAL NOT NULL
/// );
/// ```
pub struct SqliteSource {
    conn: Mutex<Connection>,
    source_name: String,
}

fn parse_uuid_column(value: String, column: usize) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(&value).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(column, rusqlite::types::Type::Text, Box::new(e))
    })
}

impl SqliteSource {
    /// Create a new SQLite source from a database file path.
    /// Creates the database and schema if they don't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open SQLite database: {:?}", path))?;

        let source = Self {
            conn: Mutex::new(conn),
            source_name: path.display().to_string(),
        };

        source.init_schema()?;
        Ok(source)
    }

    /// Create an in-memory SQLite source (useful for testing)
    pub fn in_memory() -> Result<Self> {
        let conn =
            Connection::open_in_memory().context("Failed to create in-memory SQLite database")?;

        let source = Self {
            conn: Mutex::new(conn),
            source_name: ":memory:".to_string(),
        };

        source.init_schema()?;
        Ok(source)
    }

    /// Initialize the database schema
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                content_len INTEGER NOT NULL,
                filename TEXT,
                is_parent INTEGER
            );

            CREATE TABLE IF NOT EXISTS dupes (
                child_id TEXT PRIMARY KEY,
                parent_id TEXT NOT NULL,
                jaccard_similarity REAL NOT NULL,
                size_difference INTEGER NOT NULL,
                size_difference_pct REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_documents_is_parent ON documents(is_parent);
            "#,
        )
        .context("Failed to initialize SQLite schema")?;

        Ok(())
    }

    /// Insert a document into the database
    pub fn insert_document(&self, doc: &SourceDocument) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO documents (id, content, content_len, filename, is_parent) VALUES (?1, ?2, ?3, ?4, NULL)",
            params![
                doc.id.to_string(),
                doc.content,
                doc.content_len,
                doc.filename,
            ],
        )?;
        Ok(())
    }

    /// Insert multiple documents in a batch
    pub fn insert_documents(&self, docs: &[SourceDocument]) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO documents (id, content, content_len, filename, is_parent) VALUES (?1, ?2, ?3, ?4, NULL)",
            )?;

            for doc in docs {
                stmt.execute(params![
                    doc.id.to_string(),
                    doc.content,
                    doc.content_len,
                    doc.filename,
                ])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Get all duplicate matches from the database
    pub fn get_all_dupes(&self) -> Result<Vec<SourceDupeMatch>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT child_id, parent_id, jaccard_similarity, size_difference, size_difference_pct FROM dupes",
        )?;

        let matches = stmt
            .query_map([], |row| {
                let child_id: String = row.get(0)?;
                let parent_id: String = row.get(1)?;
                Ok(SourceDupeMatch {
                    child_id: parse_uuid_column(child_id, 0)?,
                    parent_id: parse_uuid_column(parent_id, 1)?,
                    jaccard_similarity: row.get(2)?,
                    size_difference: row.get(3)?,
                    size_difference_pct: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(matches)
    }

    /// Clear all data (useful for testing)
    pub fn clear(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("DELETE FROM dupes; DELETE FROM documents;")?;
        Ok(())
    }

    /// Get document count by state
    pub fn count_by_state(&self, is_parent: Option<bool>) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = match is_parent {
            None => conn.query_row(
                "SELECT COUNT(*) FROM documents WHERE is_parent IS NULL",
                [],
                |row| row.get(0),
            )?,
            Some(true) => conn.query_row(
                "SELECT COUNT(*) FROM documents WHERE is_parent = 1",
                [],
                |row| row.get(0),
            )?,
            Some(false) => conn.query_row(
                "SELECT COUNT(*) FROM documents WHERE is_parent = 0",
                [],
                |row| row.get(0),
            )?,
        };
        Ok(count)
    }
}

#[async_trait]
impl DocumentSource for SqliteSource {
    async fn source_name(&self) -> Result<String> {
        Ok(self.source_name.clone())
    }

    async fn count_total(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))?;
        Ok(count)
    }

    async fn count_unprocessed(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM documents WHERE is_parent IS NULL",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    async fn fetch_all_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<SourceDocument>> {
        let conn = self.conn.lock().unwrap();

        let mut docs = Vec::new();

        match last_id {
            Some(id) => {
                let query = format!(
                    "SELECT {DOCUMENT_SELECT_COLUMNS} FROM documents WHERE id > ?1 ORDER BY id LIMIT ?2"
                );
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(params![id.to_string(), limit], |row| {
                    let id_str: String = row.get(0)?;
                    Ok(SourceDocument {
                        id: parse_uuid_column(id_str, 0)?,
                        content: row.get(1)?,
                        content_len: row.get(2)?,
                        filename: row.get(3)?,
                    })
                })?;
                for row in rows {
                    docs.push(row?);
                }
            }
            None => {
                let query =
                    format!("SELECT {DOCUMENT_SELECT_COLUMNS} FROM documents ORDER BY id LIMIT ?1");
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(params![limit], |row| {
                    let id_str: String = row.get(0)?;
                    Ok(SourceDocument {
                        id: parse_uuid_column(id_str, 0)?,
                        content: row.get(1)?,
                        content_len: row.get(2)?,
                        filename: row.get(3)?,
                    })
                })?;
                for row in rows {
                    docs.push(row?);
                }
            }
        }

        Ok(docs)
    }

    async fn fetch_unprocessed_ids_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Uuid>> {
        let conn = self.conn.lock().unwrap();

        let mut ids = Vec::new();

        match last_id {
            Some(id) => {
                let mut stmt = conn.prepare(
                    "SELECT id FROM documents WHERE is_parent IS NULL AND id > ?1 ORDER BY id LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![id.to_string(), limit], |row| {
                    let id_str: String = row.get(0)?;
                    parse_uuid_column(id_str, 0)
                })?;
                for row in rows {
                    ids.push(row?);
                }
            }
            None => {
                let mut stmt = conn.prepare(
                    "SELECT id FROM documents WHERE is_parent IS NULL ORDER BY id LIMIT ?1",
                )?;
                let rows = stmt.query_map(params![limit], |row| {
                    let id_str: String = row.get(0)?;
                    parse_uuid_column(id_str, 0)
                })?;
                for row in rows {
                    ids.push(row?);
                }
            }
        }

        Ok(ids)
    }

    async fn fetch_by_ids(&self, ids: &[Uuid]) -> Result<Vec<SourceDocument>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.conn.lock().unwrap();

        // Build query with placeholders
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{}", i)).collect();
        let query = format!(
            "SELECT {DOCUMENT_SELECT_COLUMNS} FROM documents WHERE id IN ({})",
            placeholders.join(", ")
        );

        let mut stmt = conn.prepare(&query)?;

        // Convert UUIDs to strings for params
        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        let params: Vec<&dyn rusqlite::ToSql> = id_strings
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let rows = stmt.query_map(params.as_slice(), |row| {
            let id_str: String = row.get(0)?;
            Ok(SourceDocument {
                id: parse_uuid_column(id_str, 0)?,
                content: row.get(1)?,
                content_len: row.get(2)?,
                filename: row.get(3)?,
            })
        })?;

        let mut docs = Vec::new();
        for row in rows {
            docs.push(row?);
        }

        Ok(docs)
    }

    async fn fetch_existing_parent_ids(&self, ids: &[Uuid]) -> Result<HashSet<Uuid>> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }

        let conn = self.conn.lock().unwrap();
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{}", i)).collect();
        let query = format!(
            "SELECT id FROM documents WHERE id IN ({}) AND is_parent = 1",
            placeholders.join(", ")
        );
        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        let params: Vec<&dyn rusqlite::ToSql> = id_strings
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(params.as_slice(), |row| {
            let id_str: String = row.get(0)?;
            parse_uuid_column(id_str, 0)
        })?;

        let mut parent_ids = HashSet::new();
        for row in rows {
            parent_ids.insert(row?);
        }
        Ok(parent_ids)
    }

    async fn fetch_existing_dupe_parents(&self, child_ids: &[Uuid]) -> Result<HashMap<Uuid, Uuid>> {
        if child_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.conn.lock().unwrap();
        let placeholders: Vec<String> = (1..=child_ids.len()).map(|i| format!("?{}", i)).collect();
        let query = format!(
            "SELECT child_id, parent_id FROM dupes WHERE child_id IN ({})",
            placeholders.join(", ")
        );
        let id_strings: Vec<String> = child_ids.iter().map(|id| id.to_string()).collect();
        let params: Vec<&dyn rusqlite::ToSql> = id_strings
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(params.as_slice(), |row| {
            let child_id: String = row.get(0)?;
            let parent_id: String = row.get(1)?;
            Ok((
                parse_uuid_column(child_id, 0)?,
                parse_uuid_column(parent_id, 1)?,
            ))
        })?;

        let mut parents = HashMap::new();
        for row in rows {
            let (child_id, parent_id) = row?;
            parents.insert(child_id, parent_id);
        }
        Ok(parents)
    }

    async fn mark_as_parents(&self, ids: &[Uuid]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut count = 0u64;
        {
            // Use shared SqlDialect for SQL generation - prevents divergence from PostgreSQL
            let sql = SqlDialect::sqlite().mark_as_parents_sql();
            let mut stmt = tx.prepare(&sql)?;
            for id in ids {
                count += stmt.execute(params![id.to_string()])? as u64;
            }
        }

        tx.commit()?;
        Ok(count)
    }

    async fn mark_as_children(&self, ids: &[Uuid]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut count = 0u64;
        {
            // Use shared SqlDialect for SQL generation - prevents divergence from PostgreSQL
            let sql = SqlDialect::sqlite().mark_as_children_sql();
            let mut stmt = tx.prepare(&sql)?;
            for id in ids {
                count += stmt.execute(params![id.to_string()])? as u64;
            }
        }

        tx.commit()?;
        Ok(count)
    }

    async fn write_dupes(&self, matches: &[SourceDupeMatch]) -> Result<u64> {
        if matches.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO dupes (child_id, parent_id, jaccard_similarity, size_difference, size_difference_pct) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;

            for m in matches {
                stmt.execute(params![
                    m.child_id.to_string(),
                    m.parent_id.to_string(),
                    m.jaccard_similarity,
                    m.size_difference,
                    m.size_difference_pct,
                ])?;
            }
        }

        tx.commit()?;
        Ok(matches.len() as u64)
    }

    fn supports_write(&self) -> bool {
        true
    }

    fn tracks_state(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_docs(count: usize) -> Vec<SourceDocument> {
        (0..count)
            .map(|i| SourceDocument {
                id: Uuid::new_v4(),
                content: format!("Test document {} with some content for testing.", i),
                content_len: 50,
                filename: Some(format!("doc{}.txt", i)),
            })
            .collect()
    }

    #[tokio::test]
    async fn test_sqlite_source_in_memory() {
        let source = SqliteSource::in_memory().unwrap();
        let count = source.count_total().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_sqlite_insert_and_count() {
        let source = SqliteSource::in_memory().unwrap();
        let docs = create_test_docs(10);

        source.insert_documents(&docs).unwrap();

        let count = source.count_total().await.unwrap();
        assert_eq!(count, 10);

        let unprocessed = source.count_unprocessed().await.unwrap();
        assert_eq!(unprocessed, 10);
    }

    #[tokio::test]
    async fn test_sqlite_fetch_all() {
        let source = SqliteSource::in_memory().unwrap();
        let docs = create_test_docs(5);
        source.insert_documents(&docs).unwrap();

        let fetched = source.fetch_all_after(None, 100).await.unwrap();
        assert_eq!(fetched.len(), 5);
    }

    #[tokio::test]
    async fn test_sqlite_pagination() {
        let source = SqliteSource::in_memory().unwrap();
        let docs = create_test_docs(10);
        source.insert_documents(&docs).unwrap();

        // Fetch first 3
        let batch1 = source.fetch_all_after(None, 3).await.unwrap();
        assert_eq!(batch1.len(), 3);

        // Fetch next 3
        let last_id = batch1.last().unwrap().id;
        let batch2 = source.fetch_all_after(Some(last_id), 3).await.unwrap();
        assert_eq!(batch2.len(), 3);

        // Ensure no overlap
        let batch1_ids: Vec<_> = batch1.iter().map(|d| d.id).collect();
        for doc in &batch2 {
            assert!(!batch1_ids.contains(&doc.id));
        }
    }

    #[tokio::test]
    async fn test_sqlite_fetch_by_ids() {
        let source = SqliteSource::in_memory().unwrap();
        let docs = create_test_docs(5);
        source.insert_documents(&docs).unwrap();

        let target_ids = vec![docs[0].id, docs[2].id];
        let fetched = source.fetch_by_ids(&target_ids).await.unwrap();

        assert_eq!(fetched.len(), 2);
        let fetched_ids: Vec<_> = fetched.iter().map(|d| d.id).collect();
        assert!(fetched_ids.contains(&docs[0].id));
        assert!(fetched_ids.contains(&docs[2].id));
    }

    #[tokio::test]
    async fn test_sqlite_fetch_tolerates_nullable_legacy_columns() {
        let source = SqliteSource::in_memory().unwrap();
        let with_content = Uuid::new_v4();
        let without_content = Uuid::new_v4();

        {
            let conn = source.conn.lock().unwrap();
            conn.execute_batch(
                r#"
                DROP TABLE documents;
                CREATE TABLE documents (
                    id TEXT PRIMARY KEY,
                    content TEXT,
                    content_len INTEGER,
                    filename TEXT,
                    is_parent INTEGER
                );
                "#,
            )
            .unwrap();
            conn.execute(
                "INSERT INTO documents (id, content, content_len, filename, is_parent) VALUES (?1, ?2, NULL, ?3, NULL)",
                rusqlite::params![with_content.to_string(), "abc", "with_content.txt"],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO documents (id, content, content_len, filename, is_parent) VALUES (?1, NULL, NULL, ?2, NULL)",
                rusqlite::params![without_content.to_string(), "without_content.txt"],
            )
            .unwrap();
        }

        let fetched = source.fetch_all_after(None, 10).await.unwrap();
        assert_eq!(fetched.len(), 2);

        let doc = fetched.iter().find(|doc| doc.id == with_content).unwrap();
        assert_eq!(doc.content, "abc");
        assert_eq!(doc.content_len, 3);

        let doc = source
            .fetch_by_ids(&[without_content])
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(doc.content, "");
        assert_eq!(doc.content_len, 0);
    }

    #[tokio::test]
    async fn test_sqlite_mark_parents_children() {
        let source = SqliteSource::in_memory().unwrap();
        let docs = create_test_docs(5);
        source.insert_documents(&docs).unwrap();

        // Mark some as parents, some as children
        let parent_ids = vec![docs[0].id, docs[1].id];
        let child_ids = vec![docs[2].id, docs[3].id];

        source.mark_as_parents(&parent_ids).await.unwrap();
        source.mark_as_children(&child_ids).await.unwrap();

        // Check counts
        let unprocessed = source.count_unprocessed().await.unwrap();
        assert_eq!(unprocessed, 1); // docs[4] still unprocessed

        let parents = source.count_by_state(Some(true)).unwrap();
        assert_eq!(parents, 2);

        let children = source.count_by_state(Some(false)).unwrap();
        assert_eq!(children, 2);
    }

    #[tokio::test]
    async fn test_sqlite_write_and_read_dupes() {
        let source = SqliteSource::in_memory().unwrap();
        let docs = create_test_docs(3);
        source.insert_documents(&docs).unwrap();

        let dupes = vec![
            SourceDupeMatch {
                child_id: docs[1].id,
                parent_id: docs[0].id,
                jaccard_similarity: 0.95,
                size_difference: 5,
                size_difference_pct: 0.1,
            },
            SourceDupeMatch {
                child_id: docs[2].id,
                parent_id: docs[0].id,
                jaccard_similarity: 0.85,
                size_difference: 10,
                size_difference_pct: 0.2,
            },
        ];

        let written = source.write_dupes(&dupes).await.unwrap();
        assert_eq!(written, 2);

        let read_dupes = source.get_all_dupes().unwrap();
        assert_eq!(read_dupes.len(), 2);
    }

    #[tokio::test]
    async fn test_sqlite_file_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create and populate
        {
            let source = SqliteSource::open(&db_path).unwrap();
            let docs = create_test_docs(3);
            source.insert_documents(&docs).unwrap();
        }

        // Reopen and verify
        {
            let source = SqliteSource::open(&db_path).unwrap();
            let count = source.count_total().await.unwrap();
            assert_eq!(count, 3);
        }
    }
}
