//! PostgreSQL database integration for document deduplication.
//!
//! Provides async database operations using tokio-postgres with connection pooling.
//!
//! Schema:
//! - `documents` table: id, content, content_len, filename, is_parent
//! - optional caller-defined scope predicate for multi-corpus tables
//! - `dupes` table: child_id, parent_id, jaccard_similarity, size_difference, size_difference_pct

use anyhow::{Context, Result};
use deadpool_postgres::{Config, Pool, Runtime};
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::sources::SqlDialect;

/// Document record from the database
#[derive(Debug, Clone)]
pub struct Document {
    pub id: Uuid,
    pub content: String,
    pub content_len: i32,
    pub filename: Option<String>,
}

/// Duplicate match to write to dupes table
#[derive(Debug, Clone)]
pub struct DupeMatch {
    pub child_id: Uuid,
    pub parent_id: Uuid,
    pub jaccard_similarity: f64,
    pub size_difference: i32,
    pub size_difference_pct: f64,
}

// tokio-postgres encodes the bind parameter count through i16. Each dupe row
// uses five bind params, so keep write chunks well under that limit.
const POSTGRES_BIND_PARAM_LIMIT: usize = i16::MAX as usize;
const DUPE_INSERT_PARAM_COUNT: usize = 5;
const DUPE_WRITE_CHUNK_SIZE: usize = 5000;
const _: () = assert!(DUPE_WRITE_CHUNK_SIZE * DUPE_INSERT_PARAM_COUNT < POSTGRES_BIND_PARAM_LIMIT);

/// Named subset of a PostgreSQL table.
///
/// The predicate is trusted SQL supplied by the caller. It is appended to read
/// and "mark remaining" queries so one physical table can hold multiple logical
/// corpora without forcing a particular schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresScope {
    pub name: String,
    pub where_sql: String,
}

/// Database configuration
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub table_name: String,
    pub scope: Option<PostgresScope>,
    pub dataset_id: Option<Uuid>,
}

impl DbConfig {
    /// Create config from DATABASE_URL environment variable
    pub fn from_env() -> Result<Self> {
        let url =
            std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;

        Self::from_url(&url)
    }

    /// Parse a PostgreSQL URL
    pub fn from_url(url: &str) -> Result<Self> {
        let url = url
            .strip_prefix("postgresql://")
            .or_else(|| url.strip_prefix("postgres://"))
            .context("Invalid database URL format")?;

        let (auth, rest) = url.rsplit_once('@').context("Missing @ in URL")?;
        let (user, password) = auth.split_once(':').context("Missing : in auth")?;
        if user.is_empty() {
            anyhow::bail!("Missing user in database URL");
        }

        let (host_port, dbname_and_params) = rest.split_once('/').context("Missing / in URL")?;
        let dbname = dbname_and_params
            .split(['?', '#'])
            .next()
            .unwrap_or_default();
        if dbname.is_empty() {
            anyhow::bail!("Missing database name in database URL");
        }

        let (host, port) = parse_host_port(host_port)?;

        Ok(Self {
            host,
            port,
            user: user.to_string(),
            password: password.to_string(),
            dbname: dbname.to_string(),
            table_name: "documents".to_string(),
            scope: None,
            dataset_id: None,
        })
    }

    /// Set a named PostgreSQL table scope.
    #[must_use]
    pub fn with_scope(mut self, name: &str, where_sql: &str) -> Self {
        self.scope = Some(PostgresScope {
            name: name.to_string(),
            where_sql: where_sql.to_string(),
        });
        self
    }

    /// Set the legacy dataset ID filter.
    #[must_use]
    pub fn with_dataset(mut self, dataset_id: Uuid) -> Self {
        self.dataset_id = Some(dataset_id);
        self.scope = Some(PostgresScope {
            name: format!("dataset_{}", dataset_id),
            where_sql: legacy_dataset_where_sql(dataset_id),
        });
        self
    }

    /// Set the legacy dataset ID filter with a human-readable sidecar name.
    #[must_use]
    pub fn with_dataset_name(mut self, dataset_id: Uuid, name: &str) -> Self {
        self.dataset_id = Some(dataset_id);
        self.scope = Some(PostgresScope {
            name: name.to_string(),
            where_sql: legacy_dataset_where_sql(dataset_id),
        });
        self
    }

    /// Set the table name
    #[must_use]
    pub fn with_table(mut self, table_name: &str) -> Self {
        self.table_name = table_name.to_string();
        self
    }

    /// Validate operator-supplied SQL identifiers before query construction.
    pub fn validate(&self) -> Result<()> {
        validate_table_name(&self.table_name)
            .with_context(|| format!("Invalid table name: {}", self.table_name))
    }

    /// Name used for this source's sidecar directory.
    pub fn source_name(&self) -> String {
        self.scope
            .as_ref()
            .map(|scope| scope.name.clone())
            .unwrap_or_else(|| self.table_name.clone())
    }
}

/// PostgreSQL connection pool
pub struct DbPool {
    pool: Pool,
    config: DbConfig,
}

/// Dedicated transaction-level advisory lock for one dataset.
///
/// This intentionally uses a non-pooled connection and keeps a transaction open.
/// Transaction-level locks work correctly through PgBouncer transaction pooling,
/// while session-level advisory locks may disappear underneath us.
pub struct DatasetLockGuard {
    client: Option<tokio_postgres::Client>,
    connection_task: JoinHandle<()>,
    key: i64,
}

impl DatasetLockGuard {
    pub async fn release(mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            client
                .batch_execute("ROLLBACK")
                .await
                .with_context(|| format!("Failed to release dataset advisory lock {}", self.key))?;
        }
        self.connection_task.abort();
        Ok(())
    }
}

impl Drop for DatasetLockGuard {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

fn parse_host_port(host_port: &str) -> Result<(String, u16)> {
    if host_port.is_empty() {
        anyhow::bail!("Missing host in database URL");
    }

    if let Some(rest) = host_port.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .context("Invalid bracketed IPv6 host in database URL")?;
        let port = if let Some(port) = suffix.strip_prefix(':') {
            port.parse()
                .with_context(|| format!("Invalid port in database URL: {}", port))?
        } else {
            5432
        };
        return Ok((host.to_string(), port));
    }

    if let Some((host, port)) = host_port.rsplit_once(':') {
        if !host.contains(':') {
            let port = port
                .parse()
                .with_context(|| format!("Invalid port in database URL: {}", port))?;
            return Ok((host.to_string(), port));
        }
    }

    Ok((host_port.to_string(), 5432))
}

fn dataset_lock_key(dataset_id: Uuid) -> i64 {
    let bytes = dataset_id.as_bytes();
    let high = i64::from_be_bytes(bytes[0..8].try_into().expect("uuid high bytes"));
    let low = i64::from_be_bytes(bytes[8..16].try_into().expect("uuid low bytes"));
    high ^ low ^ (0x1cde_ded0_cafe_beefu64 as i64)
}

fn quote_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn legacy_dataset_where_sql(dataset_id: Uuid) -> String {
    format!(
        "dataset_ids ? {}",
        quote_sql_literal(&dataset_id.to_string())
    )
}

fn validate_table_name(table_name: &str) -> Result<()> {
    if table_name.is_empty() {
        anyhow::bail!("table name must not be empty");
    }

    for part in table_name.split('.') {
        if !is_sql_identifier(part) {
            anyhow::bail!(
                "table name must contain only unquoted SQL identifiers separated by dots"
            );
        }
    }

    Ok(())
}

fn is_sql_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }

    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

impl DbPool {
    /// Create a new connection pool
    pub async fn new(config: DbConfig) -> Result<Self> {
        config.validate()?;

        let mut cfg = Config::new();
        cfg.host = Some(config.host.clone());
        cfg.port = Some(config.port);
        cfg.user = Some(config.user.clone());
        cfg.password = Some(config.password.clone());
        cfg.dbname = Some(config.dbname.clone());

        // Configure pool to avoid stale connections
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: 16,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(std::time::Duration::from_secs(30)),
                create: Some(std::time::Duration::from_secs(30)),
                recycle: Some(std::time::Duration::from_secs(30)),
            },
            ..Default::default()
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context("Failed to create connection pool")?;

        Ok(Self { pool, config })
    }

    fn where_clause(&self, mut conditions: Vec<String>) -> String {
        if let Some(scope) = &self.config.scope {
            conditions.push(format!("({})", scope.where_sql));
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }

    /// Try to acquire a database-wide lock for one dataset.
    ///
    /// Returns `Ok(None)` when another process already holds the lock.
    pub async fn try_acquire_dataset_lock(
        config: &DbConfig,
        dataset_id: Uuid,
    ) -> Result<Option<DatasetLockGuard>> {
        config.validate()?;

        let mut pg_config = tokio_postgres::Config::new();
        pg_config.host(&config.host);
        pg_config.port(config.port);
        pg_config.user(&config.user);
        pg_config.password(&config.password);
        pg_config.dbname(&config.dbname);

        let (client, connection) = pg_config
            .connect(NoTls)
            .await
            .context("Failed to open dedicated advisory-lock connection")?;
        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!("Dataset advisory-lock connection closed: {}", e);
            }
        });

        let key = dataset_lock_key(dataset_id);
        client
            .batch_execute("BEGIN")
            .await
            .context("Failed to begin advisory-lock transaction")?;

        let row = client
            .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&key])
            .await?;
        let acquired: bool = row.get(0);

        if acquired {
            Ok(Some(DatasetLockGuard {
                client: Some(client),
                connection_task,
                key,
            }))
        } else {
            let _ = client.batch_execute("ROLLBACK").await;
            connection_task.abort();
            Ok(None)
        }
    }

    /// Count unprocessed documents (where is_parent IS NULL, meaning not yet deduplicated)
    pub async fn count_unprocessed(&self) -> Result<i64> {
        let client = self.pool.get().await?;

        let query = format!(
            "SELECT COUNT(*) FROM {} {}",
            self.config.table_name,
            self.where_clause(vec!["is_parent IS NULL".to_string()])
        );

        let row = client.query_one(&query, &[]).await?;

        Ok(row.get(0))
    }

    /// Fetch a chunk of documents using OFFSET/LIMIT (DEPRECATED - use fetch_chunk_after instead)
    /// This is O(n²) and gets slower as offset increases!
    pub async fn fetch_chunk(&self, offset: i64, limit: i64) -> Result<Vec<Document>> {
        let client = self.pool.get().await?;

        let query = format!(
            r#"
            SELECT id, content, COALESCE(content_len, LENGTH(content)), filename
            FROM {}
            {}
            ORDER BY id
            OFFSET $1 LIMIT $2
            "#,
            self.config.table_name,
            self.where_clause(Vec::new())
        );

        let rows = client.query(&query, &[&offset, &limit]).await?;

        let documents = rows
            .into_iter()
            .map(|row| Document {
                id: row.get(0),
                content: row.get(1),
                content_len: row.get(2),
                filename: row.get(3),
            })
            .collect();

        Ok(documents)
    }

    /// Fetch a chunk of documents using keyset pagination (O(1) per batch!)
    /// Pass None for last_id on first call, then pass the last document's ID from the previous batch.
    /// This is MUCH faster than OFFSET/LIMIT for large datasets.
    pub async fn fetch_chunk_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Document>> {
        let client = self.pool.get().await?;

        let rows = match last_id {
            None => {
                let query = format!(
                    r#"
                    SELECT id, content, COALESCE(content_len, LENGTH(content)), filename
                    FROM {}
                    {}
                    ORDER BY id
                    LIMIT $1
                    "#,
                    self.config.table_name,
                    self.where_clause(Vec::new())
                );
                client.query(&query, &[&limit]).await?
            }
            Some(cursor) => {
                let query = format!(
                    r#"
                    SELECT id, content, COALESCE(content_len, LENGTH(content)), filename
                    FROM {}
                    {}
                    ORDER BY id
                    LIMIT $2
                    "#,
                    self.config.table_name,
                    self.where_clause(vec!["id > $1".to_string()])
                );
                client.query(&query, &[&cursor, &limit]).await?
            }
        };

        let documents = rows
            .into_iter()
            .map(|row| Document {
                id: row.get(0),
                content: row.get(1),
                content_len: row.get(2),
                filename: row.get(3),
            })
            .collect();

        Ok(documents)
    }

    /// Count total documents in dataset
    pub async fn count_total(&self) -> Result<i64> {
        let client = self.pool.get().await?;

        let query = format!(
            "SELECT COUNT(*) FROM {} {}",
            self.config.table_name,
            self.where_clause(Vec::new())
        );

        let row = client.query_one(&query, &[]).await?;

        Ok(row.get(0))
    }

    /// Write duplicate matches to the dupes table using batched multi-row INSERTs
    /// Chunks internally to stay under PostgreSQL's parameter limit (~25k params safe)
    pub async fn write_dupes(&self, matches: &[DupeMatch]) -> Result<u64> {
        if matches.is_empty() {
            return Ok(0);
        }

        let mut seen_children = std::collections::HashSet::with_capacity(matches.len());
        for m in matches {
            if !seen_children.insert(m.child_id) {
                anyhow::bail!(
                    "write_dupes received multiple canonical parents for child {}",
                    m.child_id
                );
            }
        }

        let mut client = self.pool.get().await?;

        let mut total_written = 0u64;

        for chunk in matches.chunks(DUPE_WRITE_CHUNK_SIZE) {
            let transaction = client.transaction().await?;
            let child_ids: Vec<Uuid> = chunk.iter().map(|m| m.child_id).collect();

            // Production currently has a composite unique constraint on
            // (child_id, parent_id), while the dedupe invariant is one canonical
            // parent per child. Delete the old child rows first so this method is
            // correct for both deployed and documented schemas.
            transaction
                .execute("DELETE FROM dupes WHERE child_id = ANY($1)", &[&child_ids])
                .await?;

            // Build a single INSERT statement with multiple VALUES clauses for performance
            let mut query = String::from(
                r#"
                INSERT INTO dupes (child_id, parent_id, jaccard_similarity, size_difference, size_difference_pct)
                VALUES
                "#,
            );

            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                Vec::with_capacity(chunk.len() * 5);
            let mut i = 1;
            for (idx, m) in chunk.iter().enumerate() {
                if idx > 0 {
                    query.push_str(", ");
                }
                query.push_str(&format!(
                    "(${}, ${}, ${}, ${}, ${})",
                    i,
                    i + 1,
                    i + 2,
                    i + 3,
                    i + 4
                ));
                params.push(&m.child_id);
                params.push(&m.parent_id);
                params.push(&m.jaccard_similarity);
                params.push(&m.size_difference);
                params.push(&m.size_difference_pct);
                i += 5;
            }

            transaction.execute(query.as_str(), &params[..]).await?;
            transaction.commit().await?;
            total_written += chunk.len() as u64;
        }

        Ok(total_written)
    }

    /// Mark documents as parents (set is_parent = true)
    ///
    /// Uses SqlDialect for SQL generation to ensure consistency with SqliteSource.
    pub async fn mark_as_parents(&self, doc_ids: &[Uuid]) -> Result<u64> {
        if doc_ids.is_empty() {
            return Ok(0);
        }

        let client = self.pool.get().await?;

        // Use batched UPDATE with WHERE id = ANY($1) for performance
        // Process in chunks to avoid overwhelming the database
        const CHUNK_SIZE: usize = 50000;
        let mut total_updated = 0u64;

        // Use shared SqlDialect for SQL generation - prevents divergence from SQLite
        let dialect = SqlDialect::postgres(&self.config.table_name);
        let query = dialect.mark_as_parents_sql();
        let stmt = client.prepare(&query).await?;

        for chunk in doc_ids.chunks(CHUNK_SIZE) {
            let result = client.execute(&stmt, &[&chunk]).await?;
            total_updated += result;
        }

        Ok(total_updated)
    }

    /// Mark documents as children (set is_parent = false)
    ///
    /// Uses SqlDialect for SQL generation to ensure consistency with SqliteSource.
    pub async fn mark_as_children(&self, doc_ids: &[Uuid]) -> Result<u64> {
        if doc_ids.is_empty() {
            return Ok(0);
        }

        let client = self.pool.get().await?;

        // Use batched UPDATE with WHERE id = ANY($1) for performance
        // Process in chunks to avoid overwhelming the database
        const CHUNK_SIZE: usize = 50000;
        let mut total_updated = 0u64;

        // Use shared SqlDialect for SQL generation - prevents divergence from SQLite
        let dialect = SqlDialect::postgres(&self.config.table_name);
        let query = dialect.mark_as_children_sql();
        let stmt = client.prepare(&query).await?;

        for chunk in doc_ids.chunks(CHUNK_SIZE) {
            let result = client.execute(&stmt, &[&chunk]).await?;
            total_updated += result;
        }

        Ok(total_updated)
    }

    /// Get document IDs that are parents from the dupes table
    pub async fn get_parent_ids(&self) -> Result<Vec<Uuid>> {
        let client = self.pool.get().await?;

        let rows = client
            .query("SELECT DISTINCT parent_id FROM dupes", &[])
            .await?;

        let uuids: Vec<Uuid> = rows.iter().map(|row| row.get(0)).collect();
        Ok(uuids)
    }

    /// Get child IDs from the dupes table
    pub async fn get_child_ids(&self) -> Result<Vec<Uuid>> {
        let client = self.pool.get().await?;

        let rows = client
            .query("SELECT DISTINCT child_id FROM dupes", &[])
            .await?;

        let uuids: Vec<Uuid> = rows.iter().map(|row| row.get(0)).collect();
        Ok(uuids)
    }

    /// Fetch the subset of document IDs currently marked as canonical parents.
    pub async fn fetch_parent_ids_by_doc_ids(
        &self,
        doc_ids: &[Uuid],
    ) -> Result<std::collections::HashSet<Uuid>> {
        if doc_ids.is_empty() {
            return Ok(std::collections::HashSet::new());
        }

        let client = self.pool.get().await?;
        let query = format!(
            "SELECT id FROM {} WHERE id = ANY($1) AND is_parent = true",
            self.config.table_name
        );
        let stmt = client.prepare(&query).await?;
        let mut parent_ids = std::collections::HashSet::new();

        for chunk in doc_ids.chunks(50_000) {
            let rows = client.query(&stmt, &[&chunk]).await?;
            parent_ids.extend(rows.into_iter().map(|row| row.get::<_, Uuid>(0)));
        }

        Ok(parent_ids)
    }

    /// Fetch existing canonical dupe assignments keyed by child ID.
    pub async fn fetch_dupe_parents_by_child_ids(
        &self,
        child_ids: &[Uuid],
    ) -> Result<std::collections::HashMap<Uuid, Uuid>> {
        if child_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let client = self.pool.get().await?;
        let stmt = client
            .prepare(
                r#"
                SELECT child_id, parent_id, jaccard_similarity
                FROM dupes
                WHERE child_id = ANY($1)
                "#,
            )
            .await?;

        let mut best: std::collections::HashMap<Uuid, (Uuid, f64)> =
            std::collections::HashMap::new();
        for chunk in child_ids.chunks(50_000) {
            let rows = client.query(&stmt, &[&chunk]).await?;
            for row in rows {
                let child_id: Uuid = row.get(0);
                let parent_id: Uuid = row.get(1);
                let jaccard: f64 = row.get(2);
                best.entry(child_id)
                    .and_modify(|existing| {
                        if jaccard > existing.1 {
                            *existing = (parent_id, jaccard);
                        }
                    })
                    .or_insert((parent_id, jaccard));
            }
        }

        Ok(best
            .into_iter()
            .map(|(child_id, (parent_id, _))| (child_id, parent_id))
            .collect())
    }

    /// Get pool reference for direct access
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Get config reference
    pub fn config(&self) -> &DbConfig {
        &self.config
    }

    /// Look up dataset name by UUID
    pub async fn get_dataset_name(&self, dataset_id: &Uuid) -> Result<Option<String>> {
        let client = self.pool.get().await?;

        let row = client
            .query_opt("SELECT name FROM datasets WHERE id = $1", &[dataset_id])
            .await?;

        Ok(row.map(|r| r.get(0)))
    }

    /// Look up dataset UUID by name
    pub async fn get_dataset_id_by_name(&self, name: &str) -> Result<Option<Uuid>> {
        let client = self.pool.get().await?;

        let row = client
            .query_opt("SELECT id FROM datasets WHERE name = $1", &[&name])
            .await?;

        Ok(row.map(|r| r.get(0)))
    }

    /// Fetch ALL documents (regardless of is_parent status) using keyset pagination.
    /// Use this when --all flag is set to rebuild the entire index.
    pub async fn fetch_chunk_after_all(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Document>> {
        let client = self.pool.get().await?;

        let rows = match last_id {
            None => {
                let query = format!(
                    r#"
                    SELECT id, content, COALESCE(content_len, LENGTH(content)), filename
                    FROM {}
                    {}
                    ORDER BY id
                    LIMIT $1
                    "#,
                    self.config.table_name,
                    self.where_clause(Vec::new())
                );
                client.query(&query, &[&limit]).await?
            }
            Some(cursor) => {
                let query = format!(
                    r#"
                    SELECT id, content, COALESCE(content_len, LENGTH(content)), filename
                    FROM {}
                    {}
                    ORDER BY id
                    LIMIT $2
                    "#,
                    self.config.table_name,
                    self.where_clause(vec!["id > $1".to_string()])
                );
                client.query(&query, &[&cursor, &limit]).await?
            }
        };

        let documents = rows
            .into_iter()
            .map(|row| Document {
                id: row.get(0),
                content: row.get(1),
                content_len: row.get(2),
                filename: row.get(3),
            })
            .collect();

        Ok(documents)
    }

    /// Mark ALL remaining unprocessed documents (is_parent IS NULL) as parents.
    /// This catches documents not in the LSH index (e.g., short docs filtered during indexing).
    /// Returns the number of documents marked.
    pub async fn mark_remaining_as_self_parents(&self) -> Result<u64> {
        let client = self.pool.get().await?;

        let query = format!(
            "UPDATE {} SET is_parent = true {}",
            self.config.table_name,
            self.where_clause(vec!["is_parent IS NULL".to_string()])
        );

        let result = client.execute(&query, &[]).await?;

        Ok(result)
    }

    /// Fetch just the UUIDs of unprocessed documents (is_parent IS NULL) for efficient batch checking
    /// Uses keyset pagination for O(1) per batch performance
    pub async fn fetch_unprocessed_ids_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<Uuid>> {
        let client = self.pool.get().await?;

        let rows = match last_id {
            None => {
                let query = format!(
                    r#"
                    SELECT id
                    FROM {}
                    {}
                    ORDER BY id
                    LIMIT $1
                    "#,
                    self.config.table_name,
                    self.where_clause(vec!["is_parent IS NULL".to_string()])
                );
                client.query(&query, &[&limit]).await?
            }
            Some(cursor) => {
                let query = format!(
                    r#"
                    SELECT id
                    FROM {}
                    {}
                    ORDER BY id
                    LIMIT $2
                    "#,
                    self.config.table_name,
                    self.where_clause(vec!["is_parent IS NULL".to_string(), "id > $1".to_string()])
                );
                client.query(&query, &[&cursor, &limit]).await?
            }
        };

        let ids: Vec<Uuid> = rows.into_iter().map(|row| row.get(0)).collect();
        Ok(ids)
    }

    /// Probe whether the indexed `is_parent` update path is currently healthy.
    ///
    /// The update is run inside a transaction and rolled back. This intentionally
    /// changes `is_parent` for one unprocessed row inside the transaction so
    /// PostgreSQL has to exercise any search/index maintenance hooks.
    pub async fn probe_is_parent_update_path(&self, dataset_id: Option<Uuid>) -> Result<()> {
        let mut client = self.pool.get().await?;

        let scope_sql = dataset_id.map(legacy_dataset_where_sql).or_else(|| {
            self.config
                .scope
                .as_ref()
                .map(|scope| scope.where_sql.clone())
        });

        let mut conditions = vec!["is_parent IS NULL".to_string()];
        if let Some(scope_sql) = scope_sql {
            conditions.push(format!("({})", scope_sql));
        }
        let query = format!(
            r#"
            SELECT id
            FROM {}
            WHERE {}
            ORDER BY id
            LIMIT 1
            "#,
            self.config.table_name,
            conditions.join(" AND ")
        );
        let row = client.query_opt(&query, &[]).await?;

        let Some(row) = row else {
            return Ok(());
        };
        let doc_id: Uuid = row.get(0);

        let transaction = client.transaction().await?;
        let query = format!(
            "UPDATE {} SET is_parent = true WHERE id = $1 AND is_parent IS DISTINCT FROM true",
            self.config.table_name
        );
        let update_result = transaction.execute(&query, &[&doc_id]).await;
        let rollback_result = transaction.rollback().await;

        update_result?;
        rollback_result?;
        Ok(())
    }

    /// Fetch documents by specific IDs (for incremental indexing)
    /// Returns documents for the given IDs that exist in the database
    /// Chunks large ID lists to avoid PostgreSQL parameter limits
    pub async fn fetch_documents_by_ids(&self, ids: &[Uuid]) -> Result<Vec<Document>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // PostgreSQL has a ~65k parameter limit; chunk to stay well under
        const CHUNK_SIZE: usize = 10000;
        let client = self.pool.get().await?;
        let mut all_documents = Vec::with_capacity(ids.len());

        for chunk in ids.chunks(CHUNK_SIZE) {
            // Build a query with multiple placeholders for the IN clause
            let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("${}", i)).collect();
            let query = format!(
                r#"
                SELECT id, content, COALESCE(content_len, LENGTH(content)), filename
                FROM {}
                WHERE id IN ({})
                "#,
                self.config.table_name,
                placeholders.join(", ")
            );

            // Convert UUIDs to params
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = chunk
                .iter()
                .map(|id| id as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            let rows = client.query(&query, &params).await.context(format!(
                "Failed to fetch {} documents by ID (chunk of {})",
                chunk.len(),
                ids.len()
            ))?;

            let documents: Vec<Document> = rows
                .into_iter()
                .map(|row| Document {
                    id: row.get(0),
                    content: row.get(1),
                    content_len: row.get(2),
                    filename: row.get(3),
                })
                .collect();

            all_documents.extend(documents);
        }

        Ok(all_documents)
    }

    /// Delete documents by IDs (for cleanup of pathological clusters)
    /// Returns the number of documents actually deleted
    pub async fn delete_documents(&self, doc_ids: &[Uuid]) -> Result<u64> {
        if doc_ids.is_empty() {
            return Ok(0);
        }

        let client = self.pool.get().await?;

        // Use individual deletes to handle errors gracefully
        let query = format!("DELETE FROM {} WHERE id = $1", self.config.table_name);

        let stmt = client.prepare(&query).await?;
        let mut total_deleted = 0u64;
        let mut errors = 0u64;

        for doc_id in doc_ids {
            match client.execute(&stmt, &[doc_id]).await {
                Ok(n) => total_deleted += n,
                Err(e) => {
                    errors += 1;
                    if errors <= 5 {
                        tracing::warn!("Failed to delete {}: {}", doc_id, e);
                    }
                }
            }
        }

        if errors > 0 {
            tracing::warn!(
                "Failed to delete {} documents ({} succeeded)",
                errors,
                total_deleted
            );
        }

        Ok(total_deleted)
    }

    /// Get all datasets with unprocessed documents (is_parent IS NULL)
    /// Returns Vec<(dataset_id, dataset_name, unprocessed_count)>
    pub async fn get_datasets_with_unprocessed(&self) -> Result<Vec<(Uuid, String, i64)>> {
        let client = self.pool.get().await?;

        // Query to find all datasets with unprocessed documents
        // Uses JSONB array unpacking to get dataset IDs from documents
        let query = format!(
            r#"
            WITH unprocessed_datasets AS (
                SELECT
                    jsonb_array_elements_text(dataset_ids)::uuid AS dataset_id,
                    COUNT(*) as unprocessed_count
                FROM {}
                WHERE is_parent IS NULL
                GROUP BY jsonb_array_elements_text(dataset_ids)::uuid
                HAVING COUNT(*) > 0
            )
            SELECT
                ud.dataset_id,
                d.name as dataset_name,
                ud.unprocessed_count
            FROM unprocessed_datasets ud
            INNER JOIN datasets d ON ud.dataset_id = d.id
            ORDER BY ud.unprocessed_count DESC
            "#,
            self.config.table_name
        );

        let rows = client.query(&query, &[]).await?;

        let datasets: Vec<(Uuid, String, i64)> = rows
            .into_iter()
            .map(|row| {
                let dataset_id: Uuid = row.get(0);
                let dataset_name: String = row.get(1);
                let unprocessed_count: i64 = row.get(2);
                (dataset_id, dataset_name, unprocessed_count)
            })
            .collect();

        Ok(datasets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url() {
        let config = DbConfig::from_url("postgresql://user:password@localhost:5432/mydb").unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.user, "user");
        assert_eq!(config.password, "password");
        assert_eq!(config.dbname, "mydb");
    }

    #[test]
    fn test_parse_url_default_port() {
        let config = DbConfig::from_url("postgresql://user:pass@host/db").unwrap();
        assert_eq!(config.host, "host");
        assert_eq!(config.port, 5432);
    }

    #[test]
    fn test_table_name_validation_accepts_plain_and_schema_names() {
        let config = DbConfig::from_url("postgresql://user:pass@host/db").unwrap();

        assert!(config.clone().with_table("documents").validate().is_ok());
        assert!(config
            .clone()
            .with_table("public.documents_2026")
            .validate()
            .is_ok());
        assert!(config.with_table("_scratch.table1").validate().is_ok());
    }

    #[test]
    fn test_table_name_validation_rejects_sql_fragments() {
        let config = DbConfig::from_url("postgresql://user:pass@host/db").unwrap();

        for table_name in [
            "",
            "documents;",
            "documents WHERE true",
            "public.",
            ".documents",
            "public.documents; DROP TABLE documents",
            "\"documents\"",
            "2026_documents",
        ] {
            assert!(
                config.clone().with_table(table_name).validate().is_err(),
                "{table_name:?} should be rejected"
            );
        }
    }
}
