//! Shared SQL generation layer for database sources.
//!
//! This module ensures that PostgreSQL and SQLite implementations can never diverge
//! on core logic. Both dialects generate SQL from the same methods, with only
//! syntax differences (parameter placeholders, boolean literals, array handling).
//!
//! # Design Principles
//!
//! 1. **Single source of truth**: All SQL logic lives here
//! 2. **Dialect-specific syntax only**: Differences are limited to SQL syntax, not logic
//! 3. **Testable**: SQL generation can be unit tested without a database
//!
//! # Example
//!
//! ```
//! use incrededup::sources::sql_dialect::SqlDialect;
//!
//! let pg = SqlDialect::postgres("documents");
//! let sqlite = SqlDialect::sqlite();
//!
//! // Both generate UPDATE with same logic, different syntax
//! assert!(pg.mark_as_parents_sql().contains("true"));
//! assert!(sqlite.mark_as_parents_sql().contains("1"));
//! ```

/// SQL dialect for generating database-specific SQL.
///
/// This enum captures the syntax differences between PostgreSQL and SQLite,
/// while ensuring the core logic (what columns to update, what conditions to use)
/// is shared.
#[derive(Debug, Clone)]
pub enum SqlDialect {
    /// PostgreSQL dialect with configurable table name
    Postgres { table_name: String },
    /// SQLite dialect (table name is always "documents")
    Sqlite,
}

impl SqlDialect {
    /// Create a PostgreSQL dialect with the given table name
    pub fn postgres(table_name: &str) -> Self {
        SqlDialect::Postgres {
            table_name: table_name.to_string(),
        }
    }

    /// Create a SQLite dialect
    pub fn sqlite() -> Self {
        SqlDialect::Sqlite
    }

    /// Get the table name for this dialect
    pub fn table_name(&self) -> &str {
        match self {
            SqlDialect::Postgres { table_name } => table_name,
            SqlDialect::Sqlite => "documents",
        }
    }

    /// SQL literal for boolean true
    pub fn bool_true(&self) -> &str {
        match self {
            SqlDialect::Postgres { .. } => "true",
            SqlDialect::Sqlite => "1",
        }
    }

    /// SQL literal for boolean false
    pub fn bool_false(&self) -> &str {
        match self {
            SqlDialect::Postgres { .. } => "false",
            SqlDialect::Sqlite => "0",
        }
    }

    /// Whether this dialect supports array parameters (e.g., WHERE id = ANY($1))
    ///
    /// PostgreSQL supports this, SQLite does not (must loop over individual IDs).
    pub fn supports_array_params(&self) -> bool {
        match self {
            SqlDialect::Postgres { .. } => true,
            SqlDialect::Sqlite => false,
        }
    }

    // =========================================================================
    // SQL Generation Methods
    //
    // These are the single source of truth for SQL logic.
    // Both PostgreSQL and SQLite implementations MUST use these methods.
    // =========================================================================

    /// Generate SQL to mark documents as parents (is_parent = true).
    ///
    /// Sync should push sidecar state to DB unconditionally, but avoid no-op
    /// updates because `is_parent` is indexed in production search indexes.
    pub fn mark_as_parents_sql(&self) -> String {
        match self {
            SqlDialect::Postgres { .. } => format!(
                "UPDATE {} SET is_parent = true WHERE id = ANY($1) AND is_parent IS DISTINCT FROM true",
                self.table_name()
            ),
            SqlDialect::Sqlite => {
                "UPDATE documents SET is_parent = 1 WHERE id = ?1 AND is_parent IS NOT 1"
                    .to_string()
            }
        }
    }

    /// Generate SQL to mark documents as children (is_parent = false).
    ///
    /// Sync should push sidecar state to DB unconditionally, but avoid no-op
    /// updates because `is_parent` is indexed in production search indexes.
    pub fn mark_as_children_sql(&self) -> String {
        match self {
            SqlDialect::Postgres { .. } => format!(
                "UPDATE {} SET is_parent = false WHERE id = ANY($1) AND is_parent IS DISTINCT FROM false",
                self.table_name()
            ),
            SqlDialect::Sqlite => {
                "UPDATE documents SET is_parent = 0 WHERE id = ?1 AND is_parent IS NOT 0"
                    .to_string()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_mark_as_parents_sql() {
        let dialect = SqlDialect::postgres("my_table");
        let sql = dialect.mark_as_parents_sql();

        assert_eq!(
            sql,
            "UPDATE my_table SET is_parent = true WHERE id = ANY($1) AND is_parent IS DISTINCT FROM true"
        );
        // CRITICAL: Must NOT contain IS NULL - sync overwrites incorrect values.
        assert!(
            !sql.contains("IS NULL"),
            "mark_as_parents must not filter by IS NULL"
        );
    }

    #[test]
    fn test_postgres_mark_as_children_sql() {
        let dialect = SqlDialect::postgres("my_table");
        let sql = dialect.mark_as_children_sql();

        assert_eq!(
            sql,
            "UPDATE my_table SET is_parent = false WHERE id = ANY($1) AND is_parent IS DISTINCT FROM false"
        );
        assert!(
            !sql.contains("IS NULL"),
            "mark_as_children must not filter by IS NULL"
        );
    }

    #[test]
    fn test_sqlite_mark_as_parents_sql() {
        let dialect = SqlDialect::sqlite();
        let sql = dialect.mark_as_parents_sql();

        assert_eq!(
            sql,
            "UPDATE documents SET is_parent = 1 WHERE id = ?1 AND is_parent IS NOT 1"
        );
        assert!(
            !sql.contains("IS NULL"),
            "mark_as_parents must not filter by IS NULL"
        );
    }

    #[test]
    fn test_sqlite_mark_as_children_sql() {
        let dialect = SqlDialect::sqlite();
        let sql = dialect.mark_as_children_sql();

        assert_eq!(
            sql,
            "UPDATE documents SET is_parent = 0 WHERE id = ?1 AND is_parent IS NOT 0"
        );
        assert!(
            !sql.contains("IS NULL"),
            "mark_as_children must not filter by IS NULL"
        );
    }

    #[test]
    fn test_dialects_have_same_logic() {
        let pg = SqlDialect::postgres("documents");
        let sqlite = SqlDialect::sqlite();

        // Both should update is_parent without any IS NULL filter
        let pg_parents = pg.mark_as_parents_sql();
        let sqlite_parents = sqlite.mark_as_parents_sql();

        // Core structure should be the same (UPDATE table SET is_parent = X WHERE id = Y)
        assert!(pg_parents.starts_with("UPDATE documents SET is_parent = "));
        assert!(sqlite_parents.starts_with("UPDATE documents SET is_parent = "));

        // Neither should have IS NULL
        assert!(!pg_parents.contains("NULL"));
        assert!(!sqlite_parents.contains("NULL"));
    }

    #[test]
    fn test_supports_array_params() {
        assert!(SqlDialect::postgres("t").supports_array_params());
        assert!(!SqlDialect::sqlite().supports_array_params());
    }
}
