use anyhow::{Context, Result};
use incrededup::{run_dedupe, DbConfig, DbPool, DedupeConfig, DupeMatch};
use std::path::Path;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const SOCKET_DIR: &str = "/var/run/postgresql";

struct TestDatabase {
    name: String,
    admin: Client,
    admin_task: JoinHandle<()>,
    config: DbConfig,
}

impl TestDatabase {
    async fn create() -> Result<Option<Self>> {
        if let Ok(admin_url) = std::env::var("POSTGRES_TEST_URL") {
            return Self::create_from_url(&admin_url).await.map(Some);
        }

        if !Path::new(SOCKET_DIR).exists() {
            eprintln!("skipping Postgres integration test: {SOCKET_DIR} does not exist");
            return Ok(None);
        }

        let user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_string());
        let admin_conn = format!("host={SOCKET_DIR} user={user} dbname=postgres");
        let (admin, connection) = match tokio_postgres::connect(&admin_conn, NoTls).await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!(
                    "skipping Postgres integration test: cannot connect to local Postgres: {e}"
                );
                return Ok(None);
            }
        };
        let admin_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres admin connection closed: {e}");
            }
        });

        let name = format!("incrededup_test_{}", Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE DATABASE {}", quote_ident(&name)))
            .await
            .with_context(|| format!("failed to create test database {name}"))?;

        let config = DbConfig {
            host: SOCKET_DIR.to_string(),
            port: 5432,
            user,
            password: String::new(),
            dbname: name.clone(),
            table_name: "documents".to_string(),
            scope: None,
            dataset_id: None,
        };

        Ok(Some(Self {
            name,
            admin,
            admin_task,
            config,
        }))
    }

    async fn create_from_url(admin_url: &str) -> Result<Self> {
        let (admin, connection) = tokio_postgres::connect(admin_url, NoTls)
            .await
            .with_context(|| "failed to connect to POSTGRES_TEST_URL")?;
        let admin_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres admin connection closed: {e}");
            }
        });

        let name = format!("incrededup_test_{}", Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE DATABASE {}", quote_ident(&name)))
            .await
            .with_context(|| format!("failed to create test database {name}"))?;

        let mut config = DbConfig::from_url(admin_url)
            .with_context(|| "failed to parse POSTGRES_TEST_URL into DbConfig")?;
        config.dbname = name.clone();
        config.table_name = "documents".to_string();
        config.scope = None;
        config.dataset_id = None;

        Ok(Self {
            name,
            admin,
            admin_task,
            config,
        })
    }

    async fn drop_database(self) {
        let sql = format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(&self.name)
        );
        if let Err(e) = self.admin.batch_execute(&sql).await {
            eprintln!("failed to drop test database {}: {e}", self.name);
        }
        self.admin_task.abort();
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn connect_config(config: &DbConfig) -> Result<(Client, JoinHandle<()>)> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host(&config.host);
    pg_config.port(config.port);
    pg_config.user(&config.user);
    pg_config.password(&config.password);
    pg_config.dbname(&config.dbname);

    let (client, connection) = pg_config.connect(NoTls).await?;
    let task = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres test connection closed: {e}");
        }
    });
    Ok((client, task))
}

fn match_row(child_id: Uuid, parent_id: Uuid) -> DupeMatch {
    DupeMatch {
        child_id,
        parent_id,
        jaccard_similarity: 0.91,
        size_difference: 12,
        size_difference_pct: 0.12,
    }
}

fn long_test_document(seed_text: &str) -> String {
    format!("{seed_text} ").repeat(24)
}

async fn assert_single_parent(
    config: &DbConfig,
    child_id: Uuid,
    expected_parent: Uuid,
) -> Result<()> {
    let (client, task) = connect_config(config).await?;
    let rows = client
        .query(
            "SELECT parent_id FROM dupes WHERE child_id = $1 ORDER BY parent_id",
            &[&child_id],
        )
        .await?;
    task.abort();

    assert_eq!(
        rows.len(),
        1,
        "child should have exactly one canonical parent"
    );
    let actual_parent: Uuid = rows[0].get(0);
    assert_eq!(actual_parent, expected_parent);
    Ok(())
}

#[tokio::test]
async fn postgres_write_dupes_replaces_child_primary_key_row() -> Result<()> {
    let Some(test_db) = TestDatabase::create().await? else {
        return Ok(());
    };
    let config = test_db.config.clone();

    let result = async {
        let (client, task) = connect_config(&config).await?;
        client
            .batch_execute(
                r#"
                CREATE TABLE dupes (
                    child_id UUID PRIMARY KEY,
                    parent_id UUID NOT NULL,
                    jaccard_similarity DOUBLE PRECISION NOT NULL,
                    size_difference INTEGER,
                    size_difference_pct DOUBLE PRECISION
                );
                "#,
            )
            .await?;
        task.abort();

        let child = Uuid::new_v4();
        let first_parent = Uuid::new_v4();
        let second_parent = Uuid::new_v4();

        let pool = DbPool::new(config.clone()).await?;
        assert_eq!(
            pool.write_dupes(&[match_row(child, first_parent)]).await?,
            1
        );
        assert_eq!(
            pool.write_dupes(&[match_row(child, second_parent)]).await?,
            1
        );
        drop(pool);

        assert_single_parent(&config, child, second_parent).await
    }
    .await;

    test_db.drop_database().await;
    result
}

#[tokio::test]
async fn postgres_write_dupes_enforces_one_parent_on_composite_schema() -> Result<()> {
    let Some(test_db) = TestDatabase::create().await? else {
        return Ok(());
    };
    let config = test_db.config.clone();

    let result = async {
        let (client, task) = connect_config(&config).await?;
        client
            .batch_execute(
                r#"
                CREATE TABLE dupes (
                    id BIGSERIAL PRIMARY KEY,
                    child_id UUID NOT NULL,
                    parent_id UUID NOT NULL,
                    jaccard_similarity DOUBLE PRECISION NOT NULL,
                    size_difference INTEGER,
                    size_difference_pct DOUBLE PRECISION,
                    UNIQUE (child_id, parent_id)
                );
                "#,
            )
            .await?;
        task.abort();

        let child = Uuid::new_v4();
        let first_parent = Uuid::new_v4();
        let second_parent = Uuid::new_v4();

        let pool = DbPool::new(config.clone()).await?;
        assert_eq!(
            pool.write_dupes(&[match_row(child, first_parent)]).await?,
            1
        );
        assert_eq!(
            pool.write_dupes(&[match_row(child, second_parent)]).await?,
            1
        );
        let err = pool
            .write_dupes(&[
                match_row(Uuid::new_v4(), Uuid::new_v4()),
                match_row(child, first_parent),
                match_row(child, second_parent),
            ])
            .await
            .expect_err("multiple parents for one child in one batch must fail");
        assert!(
            err.to_string().contains("multiple canonical parents"),
            "unexpected error: {err:#}"
        );
        drop(pool);

        assert_single_parent(&config, child, second_parent).await
    }
    .await;

    test_db.drop_database().await;
    result
}

#[tokio::test]
async fn postgres_run_dedupe_end_to_end_plain_table_marks_parents_and_children() -> Result<()> {
    let Some(test_db) = TestDatabase::create().await? else {
        return Ok(());
    };
    let config = test_db.config.clone();

    let result = async {
        let doc_a = Uuid::new_v4();
        let doc_b = Uuid::new_v4();
        let unique_doc = Uuid::new_v4();
        let duplicate_content = long_test_document(
            "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima",
        );
        let unique_content = long_test_document(
            "radish saffron turnip umber violet willow xylophone yarrow zephyr quartz onyx jasper",
        );

        let (client, task) = connect_config(&config).await?;
        client
            .batch_execute(
                r#"
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
                "#,
            )
            .await?;
        for (id, content, filename) in [
            (doc_a, duplicate_content.as_str(), "duplicate_a.txt"),
            (doc_b, duplicate_content.as_str(), "duplicate_b.txt"),
            (unique_doc, unique_content.as_str(), "unique.txt"),
        ] {
            client
                .execute(
                    r#"
                    INSERT INTO documents
                        (id, content, content_len, filename, is_parent)
                    VALUES ($1, $2, $3, $4, NULL)
                    "#,
                    &[&id, &content, &(content.len() as i32), &filename],
                )
                .await?;
        }
        task.abort();

        let data_dir = TempDir::new()?;
        let stats = run_dedupe(
            config.clone(),
            DedupeConfig {
                threshold: 0.8,
                batch_size: 10,
                data_dir: data_dir.path().to_path_buf(),
                min_content_length: 100,
                ..Default::default()
            },
        )
        .await?;

        assert_eq!(stats.total_documents, 3);
        assert_eq!(stats.duplicates_found, 1);

        let (client, task) = connect_config(&config).await?;
        let unprocessed: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM documents WHERE is_parent IS NULL",
                &[],
            )
            .await?
            .get(0);
        assert_eq!(unprocessed, 0, "all dataset documents should be processed");

        let dupe_rows = client
            .query(
                "SELECT child_id, parent_id FROM dupes ORDER BY child_id",
                &[],
            )
            .await?;
        assert_eq!(
            dupe_rows.len(),
            1,
            "one duplicate assignment should be written"
        );
        let child_id: Uuid = dupe_rows[0].get(0);
        let parent_id: Uuid = dupe_rows[0].get(1);
        assert_ne!(child_id, parent_id);
        assert!(
            [doc_a, doc_b].contains(&child_id),
            "duplicate child should be one of the duplicated docs"
        );
        assert!(
            [doc_a, doc_b].contains(&parent_id),
            "duplicate parent should be one of the duplicated docs"
        );

        let state_rows = client
            .query("SELECT id, is_parent FROM documents ORDER BY id", &[])
            .await?;
        let mut parent_count = 0;
        let mut child_count = 0;
        for row in state_rows {
            let id: Uuid = row.get(0);
            let is_parent: bool = row.get(1);
            if is_parent {
                parent_count += 1;
            } else {
                child_count += 1;
                assert_eq!(id, child_id, "only the duplicate child should be false");
            }
            if id == unique_doc {
                assert!(is_parent, "unique document should be marked as a parent");
            }
        }
        assert_eq!(parent_count, 2);
        assert_eq!(child_count, 1);
        task.abort();

        assert!(
            data_dir.path().join("documents").join("lsh.redb").exists(),
            "Postgres e2e run should create an LSH sidecar"
        );
        assert!(
            data_dir
                .path()
                .join("documents")
                .join("matches.redb")
                .exists(),
            "Postgres e2e run should create a matches sidecar"
        );

        Ok(())
    }
    .await;

    test_db.drop_database().await;
    result
}

#[tokio::test]
async fn postgres_run_dedupe_end_to_end_scoped_table_only_processes_scope() -> Result<()> {
    let Some(test_db) = TestDatabase::create().await? else {
        return Ok(());
    };
    let config = test_db
        .config
        .clone()
        .with_scope("corpus_a", "corpus = 'a'");

    let result = async {
        let scope_doc_a = Uuid::new_v4();
        let scope_doc_b = Uuid::new_v4();
        let outside_doc = Uuid::new_v4();
        let duplicate_content = long_test_document(
            "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima",
        );
        let outside_content = long_test_document(
            "outside corpus should stay unprocessed saffron turnip violet willow quartz",
        );

        let (client, task) = connect_config(&config).await?;
        client
            .batch_execute(
                r#"
                CREATE TABLE documents (
                    id UUID PRIMARY KEY,
                    corpus TEXT NOT NULL,
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
                "#,
            )
            .await?;
        for (id, corpus, content, filename) in [
            (scope_doc_a, "a", duplicate_content.as_str(), "scope_a.txt"),
            (scope_doc_b, "a", duplicate_content.as_str(), "scope_b.txt"),
            (outside_doc, "b", outside_content.as_str(), "outside.txt"),
        ] {
            client
                .execute(
                    r#"
                    INSERT INTO documents
                        (id, corpus, content, content_len, filename, is_parent)
                    VALUES ($1, $2, $3, $4, $5, NULL)
                    "#,
                    &[&id, &corpus, &content, &(content.len() as i32), &filename],
                )
                .await?;
        }
        task.abort();

        let data_dir = TempDir::new()?;
        let stats = run_dedupe(
            config.clone(),
            DedupeConfig {
                threshold: 0.8,
                batch_size: 10,
                data_dir: data_dir.path().to_path_buf(),
                min_content_length: 100,
                ..Default::default()
            },
        )
        .await?;

        assert_eq!(stats.total_documents, 2);
        assert_eq!(stats.duplicates_found, 1);

        let (client, task) = connect_config(&config).await?;
        let outside_is_parent: Option<bool> = client
            .query_one(
                "SELECT is_parent FROM documents WHERE id = $1",
                &[&outside_doc],
            )
            .await?
            .get(0);
        assert_eq!(
            outside_is_parent, None,
            "documents outside the configured scope should not be touched"
        );

        let scoped_unprocessed: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM documents WHERE corpus = 'a' AND is_parent IS NULL",
                &[],
            )
            .await?
            .get(0);
        assert_eq!(scoped_unprocessed, 0);

        let dupe_count: i64 = client
            .query_one("SELECT COUNT(*) FROM dupes", &[])
            .await?
            .get(0);
        assert_eq!(dupe_count, 1);
        task.abort();

        assert!(
            data_dir.path().join("corpus_a").join("lsh.redb").exists(),
            "scoped Postgres run should create a scope-named LSH sidecar"
        );

        Ok(())
    }
    .await;

    test_db.drop_database().await;
    result
}

#[tokio::test]
async fn postgres_dataset_advisory_lock_excludes_concurrent_workers() -> Result<()> {
    let Some(test_db) = TestDatabase::create().await? else {
        return Ok(());
    };
    let config = test_db.config.clone();

    let result = async {
        let dataset_id = Uuid::new_v4();
        let first = DbPool::try_acquire_dataset_lock(&config, dataset_id)
            .await?
            .expect("first lock acquisition should succeed");
        assert!(
            DbPool::try_acquire_dataset_lock(&config, dataset_id)
                .await?
                .is_none(),
            "second concurrent lock acquisition should be rejected"
        );
        first.release().await?;
        let third = DbPool::try_acquire_dataset_lock(&config, dataset_id)
            .await?
            .expect("lock should be acquirable after release");
        third.release().await
    }
    .await;

    test_db.drop_database().await;
    result
}

#[tokio::test]
async fn postgres_is_parent_probe_rolls_back_update() -> Result<()> {
    let Some(test_db) = TestDatabase::create().await? else {
        return Ok(());
    };
    let config = test_db.config.clone();

    let result = async {
        let dataset_id = Uuid::new_v4();
        let doc_id = Uuid::new_v4();
        let dataset_json = format!(r#"["{}"]"#, dataset_id);

        let (client, task) = connect_config(&config).await?;
        client
            .batch_execute(
                r#"
                CREATE TABLE documents (
                    id UUID PRIMARY KEY,
                    content TEXT NOT NULL,
                    content_len INTEGER,
                    filename TEXT,
                    is_parent BOOLEAN,
                    dataset_ids JSONB NOT NULL
                );
                "#,
            )
            .await?;
        client
            .execute(
                "INSERT INTO documents (id, content, content_len, is_parent, dataset_ids) VALUES ($1, 'content', 7, NULL, $2::text::jsonb)",
                &[&doc_id, &dataset_json],
            )
            .await?;
        task.abort();

        let pool = DbPool::new(config.clone()).await?;
        pool.probe_is_parent_update_path(Some(dataset_id)).await?;
        drop(pool);

        let (client, task) = connect_config(&config).await?;
        let row = client
            .query_one("SELECT is_parent FROM documents WHERE id = $1", &[&doc_id])
            .await?;
        let is_parent: Option<bool> = row.get(0);
        task.abort();

        assert_eq!(is_parent, None, "probe update must be rolled back");
        Ok(())
    }
    .await;

    test_db.drop_database().await;
    result
}
