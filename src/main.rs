//! incrededup CLI: Performant, disk-based, incremental deduplication
//!
//! MODES:
//! 1. Database mode (default): Fetch from DB, build index, dedupe, write results
//!    incrededup --postgres --table documents
//!
//! 2. Index-only mode: Dedupe from existing LSH index (no DB required)
//!    incrededup --from-index /path/to/lsh.redb --output-dir /path/to/output
//!
//! 3. Daemon mode: Continuously watch for unprocessed documents
//!    incrededup --daemon
//!
//! 4. Sync mode: Sync matches to PostgreSQL (with transitivity resolution)
//!    incrededup --sync /path/to/dataset_dir
//!
//! 5. Inspect mode: Inspect matches.redb file contents
//!    incrededup --inspect /path/to/dataset_dir

use anyhow::Result;
use chrono::Local;
use clap::Parser;
use futures::future::join_all;
use incrededup::{
    resolve_transitivity, run_dedupe, run_disk_dedupe, Args, DbConfig, DbPool, DedupeConfig,
    FilteredParentStore, MatchStore,
};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn, Level};
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

/// Get current RSS (Resident Set Size) memory usage in MB.
/// Returns None on non-Linux platforms or if unable to read.
#[cfg(target_os = "linux")]
fn get_memory_mb() -> Option<f64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|kb| kb.parse::<f64>().ok())
                        .map(|kb| kb / 1024.0)
                })
        })
}

#[cfg(not(target_os = "linux"))]
fn get_memory_mb() -> Option<f64> {
    None
}

/// Log current memory usage with a label
fn log_memory(label: &str) {
    if let Some(mb) = get_memory_mb() {
        tracing::info!("[MEMORY] {}: {:.1} MB ({:.2} GB)", label, mb, mb / 1024.0);
    }
}

fn is_search_index_corruption_error(error: &anyhow::Error) -> bool {
    let message = format!("{:#}", error).to_lowercase();
    message.contains("segmentmetaentryheader")
        || (message.contains("unexpectedend") && message.contains("deserialize"))
        || (message.contains("tantivy") && message.contains("corrupt"))
        || (message.contains("pg_search") && message.contains("corrupt"))
}

/// Release memory back to the OS (Linux only).
/// Calls malloc_trim(0) to return unused heap memory to the system.
/// On non-Linux platforms, this is a no-op.
#[cfg(target_os = "linux")]
fn release_memory_to_os() {
    extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    let before = get_memory_mb();
    unsafe {
        malloc_trim(0);
    }
    let after = get_memory_mb();
    if let (Some(b), Some(a)) = (before, after) {
        let released = b - a;
        if released > 10.0 {
            tracing::info!(
                "[MEMORY] malloc_trim released {:.1} MB ({:.2} GB -> {:.2} GB)",
                released,
                b / 1024.0,
                a / 1024.0
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn release_memory_to_os() {
    // No-op on non-Linux platforms
}

/// Custom timer that uses local time (chrono)
struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%d %H:%M:%S"))
    }
}

async fn postgres_config_from_args(args: &Args) -> Result<DbConfig> {
    if args.scope.is_some() != args.scope_where.is_some() {
        anyhow::bail!("--scope and --scope-where must be provided together");
    }
    if args.dataset.is_some() && args.scope.is_some() {
        anyhow::bail!("--dataset is a legacy shorthand and cannot be combined with --scope");
    }

    let mut db_config = DbConfig::from_env()?.with_table(&args.table);

    if let (Some(scope), Some(scope_where)) = (&args.scope, &args.scope_where) {
        if scope.trim().is_empty() || scope.contains('/') || scope.contains('\\') {
            anyhow::bail!("--scope must be a non-empty sidecar name without path separators");
        }
        if scope_where.trim().is_empty() {
            anyhow::bail!("--scope-where must not be empty");
        }
        return Ok(db_config.with_scope(scope, scope_where));
    }

    if let Some(dataset_str) = args.dataset.as_ref() {
        let dataset_id = match Uuid::parse_str(dataset_str) {
            Ok(uuid) => {
                info!("Dataset UUID: {}", uuid);
                uuid
            }
            Err(_) => {
                info!("Looking up dataset by name: {}", dataset_str);

                let temp_pool = DbPool::new(db_config.clone()).await?;
                match temp_pool.get_dataset_id_by_name(dataset_str).await? {
                    Some(uuid) => {
                        info!("Found dataset: {} -> {}", dataset_str, uuid);
                        uuid
                    }
                    None => {
                        anyhow::bail!(
                            "Dataset '{}' not found. Please provide a valid UUID or dataset name.",
                            dataset_str
                        );
                    }
                }
            }
        };

        let scope_name = if let Ok(uuid) = Uuid::parse_str(dataset_str) {
            let temp_pool = DbPool::new(db_config.clone()).await?;
            temp_pool
                .get_dataset_name(&uuid)
                .await?
                .unwrap_or_else(|| format!("dataset_{}", uuid))
        } else {
            dataset_str.clone()
        };
        db_config = db_config.with_dataset_name(dataset_id, &scope_name);
    }

    Ok(db_config)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file from current working directory (silently ignore if not found)
    let _ = dotenvy::dotenv();

    let mut args = Args::parse();

    if (args.scope.is_some() || args.scope_where.is_some()) && !args.postgres {
        anyhow::bail!("--scope and --scope-where require --postgres");
    }

    // Fall back to LOG_FILE env var if --log-file not specified
    if args.log_file.is_none() {
        if let Ok(log_path) = std::env::var("LOG_FILE") {
            args.log_file = Some(PathBuf::from(log_path));
        }
    }

    // Check if no mode was specified - show help without logging/error
    let has_mode = args.postgres
        || args.dataset.is_some()
        || args.from_index.is_some()
        || args.daemon
        || args.sync.is_some()
        || args.inspect.is_some()
        || args.build_adjacency.is_some()
        || args.sqlite.is_some()
        || args.cleanup.is_some();

    if !has_mode {
        // Print clean usage message without tracing/timestamps
        println!("incrededup v{}", env!("CARGO_PKG_VERSION"));
        println!("Performant, disk-based, incremental deduplication using MinHash LSH");
        println!();
        println!("USAGE:");
        println!("    incrededup --postgres [--table documents] [--all]       PostgreSQL mode");
        println!(
            "    incrededup --postgres --scope <name> --scope-where <sql> PostgreSQL scoped table"
        );
        println!("    incrededup --dataset <name-or-uuid> [--all]              Legacy dataset_ids filter");
        println!("    incrededup --sqlite <path.db> [--all] [--disk]          SQLite mode");
        println!("    incrededup --from-index <lsh.redb> --output-dir <dir>   Index-only mode");
        println!("    incrededup --daemon [--interval <secs>]                 Daemon mode");
        println!("    incrededup --sync <dataset_dir>                         Sync matches to DB");
        println!("    incrededup --inspect <dataset_dir> [--inspect-sample]   Inspect matches");
        println!(
            "    incrededup --build-adjacency <dataset_dir>              Build matches side-index"
        );
        println!("    incrededup --cleanup <dataset_dir> [--cleanup-action <action>]  Cleanup pathological clusters");
        println!();
        println!("Run 'incrededup --help' for full options.");
        return Ok(());
    }

    // Initialize logging
    let level = if args.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };

    // If log_file is specified, write to file instead of stdout (with no ANSI colors)
    if let Some(log_path) = &args.log_file {
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .expect("Failed to open log file");

        let subscriber = FmtSubscriber::builder()
            .with_max_level(level)
            .with_target(false)
            .with_thread_ids(false)
            .with_ansi(false) // No ANSI colors in file
            .with_timer(LocalTimer)
            .with_writer(log_file)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    } else {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(level)
            .with_target(false)
            .with_thread_ids(false)
            .with_timer(LocalTimer)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    }

    info!("incrededup v{}", env!("CARGO_PKG_VERSION"));

    // MODE -1: Inspect mode (--inspect) - no database required
    if let Some(inspect_path) = &args.inspect {
        return run_inspect_mode(inspect_path, args.inspect_limit, args.inspect_sample);
    }

    // MODE -0.75: Build adjacency mode (--build-adjacency) - no database required
    if let Some(build_path) = &args.build_adjacency {
        return run_build_adjacency_mode(build_path);
    }

    // MODE -0.5: Sync mode (--sync) - sync matches.redb to PostgreSQL
    if let Some(sync_path) = &args.sync {
        return run_sync_mode(sync_path, args.batch_size as usize, args.dry_run).await;
    }

    // MODE -0.25: Cleanup mode (--cleanup) - detect and handle pathological clusters
    if let Some(cleanup_path) = &args.cleanup {
        return run_cleanup_mode(
            cleanup_path,
            &args.cleanup_action,
            args.cleanup_min_bucket,
            args.cleanup_min_bands,
        )
        .await;
    }

    let num_workers = args.workers.unwrap_or_else(num_cpus::get);

    // MODE 0: Daemon mode (--daemon)
    // Continuously poll for unprocessed documents
    if args.daemon {
        // Warn if MALLOC_ARENA_MAX is not set (recommended for long-running daemons)
        if std::env::var("MALLOC_ARENA_MAX").is_err() {
            warn!(
                "MALLOC_ARENA_MAX is not set. For long-running daemon mode, consider setting \
                 MALLOC_ARENA_MAX=2 to reduce memory fragmentation from parallel workers."
            );
        } else {
            info!(
                "MALLOC_ARENA_MAX={}",
                std::env::var("MALLOC_ARENA_MAX").unwrap_or_default()
            );
        }

        // Check if SQLite daemon mode
        if let Some(sqlite_path) = &args.sqlite {
            info!("=== SQLITE DAEMON MODE ===");
            info!("Database: {:?}", sqlite_path);
            info!("Polling interval: {}s", args.interval);
            info!("Workers: {}", num_workers);
            info!("Data dir: {}", args.data_dir);
            info!("Edge lookup: {:?}", args.edge_lookup);
            if args.keep_in_memory {
                info!("Memory mode: CACHED (keep index in memory forever)");
            } else if args.memory_idle_timeout == 0 {
                info!("Memory mode: AGGRESSIVE (release after each batch)");
            } else {
                info!(
                    "Memory mode: IDLE-RELEASE (release after {} min idle)",
                    args.memory_idle_timeout
                );
            }
            info!("Press Ctrl+C to stop");
            info!("");

            let poll_interval = Duration::from_secs(args.interval);
            let memory_idle_timeout = Duration::from_secs(args.memory_idle_timeout * 60);
            let mut last_activity = Instant::now();
            let mut memory_released = false;
            let source_name = sqlite_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("sqlite");

            loop {
                let source = incrededup::SqliteSource::open(sqlite_path)?;
                let unprocessed = incrededup::DocumentSource::count_unprocessed(&source).await?;

                if unprocessed == 0 {
                    tracing::debug!("No unprocessed documents");

                    // Check if we should release memory due to idle timeout
                    if !args.keep_in_memory && !memory_released {
                        let idle_time = last_activity.elapsed();
                        if args.memory_idle_timeout == 0 || idle_time >= memory_idle_timeout {
                            release_memory_to_os();
                            memory_released = true;
                            if args.memory_idle_timeout > 0 {
                                info!(
                                    "Released memory after {} min idle",
                                    idle_time.as_secs() / 60
                                );
                            }
                        }
                    }
                } else {
                    info!("Found {} unprocessed documents", unprocessed);
                    last_activity = Instant::now();
                    memory_released = false;

                    let dedupe_config = DedupeConfig {
                        threshold: args.threshold,
                        size_diff_threshold: args.size_diff,
                        batch_size: args.batch_size,
                        num_workers,
                        use_disk_lsh: true,
                        disk_lsh_path: args.disk_lsh.clone(),
                        seed: args.seed,
                        process_all: false, // Daemon mode only processes unprocessed docs
                        fresh: false,
                        data_dir: std::path::PathBuf::from(&args.data_dir),
                        skip_db_write: args.skip_db_write,
                        disk_phase2: true, // Daemon mode always uses disk-backed Phase 2.
                        min_content_length: args.min_content_len,
                        edge_lookup_mode: args.edge_lookup.into(),
                    };

                    log_memory(&format!("Before processing SQLite dataset {}", source_name));
                    match incrededup::run_dedupe_with_source(
                        &source,
                        dedupe_config,
                        Some(source_name),
                    )
                    .await
                    {
                        Ok(stats) => {
                            info!(
                                "Completed: {} docs processed, {} dupes found, {:.1}s",
                                stats.total_documents, stats.duplicates_found, stats.duration_secs
                            );
                            log_memory(&format!("After processing SQLite dataset {}", source_name));
                            last_activity = Instant::now();
                        }
                        Err(e) => {
                            // Show full error chain for debugging
                            warn!("Error processing: {:#}", e);
                            log_memory(&format!("After error in SQLite dataset {}", source_name));
                        }
                    }

                    // Always try to release memory after processing to prevent arena buildup
                    release_memory_to_os();
                    log_memory(&format!(
                        "After malloc_trim for SQLite dataset {}",
                        source_name
                    ));
                }

                // Exit after one pass if --run-once is set
                if args.run_once {
                    info!("Run-once mode: exiting after one pass");
                    return Ok(());
                }

                tokio::time::sleep(poll_interval).await;
            }
        }

        if args.postgres && args.dataset.is_none() {
            info!("=== POSTGRESQL DAEMON MODE ===");
            info!("Table: {}", args.table);
            info!("Polling interval: {}s", args.interval);
            info!("Workers: {}", num_workers);
            info!("Data dir: {}", args.data_dir);
            info!("Edge lookup: {:?}", args.edge_lookup);
            if args.keep_in_memory {
                info!("Memory mode: CACHED (keep index in memory forever)");
            } else if args.memory_idle_timeout == 0 {
                info!("Memory mode: AGGRESSIVE (release after each batch)");
            } else {
                info!(
                    "Memory mode: IDLE-RELEASE (release after {} min idle)",
                    args.memory_idle_timeout
                );
            }
            info!("Press Ctrl+C to stop");
            info!("");

            let poll_interval = Duration::from_secs(args.interval);
            let memory_idle_timeout = Duration::from_secs(args.memory_idle_timeout * 60);
            let search_index_error_backoff =
                Duration::from_secs(args.search_index_error_backoff_secs);
            let mut last_activity = Instant::now();
            let mut memory_released = false;
            let mut search_index_backoff_until: Option<Instant> = None;
            let db_config = postgres_config_from_args(&args).await?;
            let pool = DbPool::new(db_config.clone()).await?;

            loop {
                let unprocessed = match pool.count_unprocessed().await {
                    Ok(count) => count,
                    Err(e) => {
                        warn!(
                            "Failed to count unprocessed documents; backing off for {}s: {:#}",
                            args.interval, e
                        );
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                };

                if unprocessed == 0 {
                    tracing::debug!("No unprocessed documents");

                    if !args.keep_in_memory && !memory_released {
                        let idle_time = last_activity.elapsed();
                        if args.memory_idle_timeout == 0 || idle_time >= memory_idle_timeout {
                            release_memory_to_os();
                            memory_released = true;
                            if args.memory_idle_timeout > 0 {
                                info!(
                                    "Released memory after {} min idle",
                                    idle_time.as_secs() / 60
                                );
                            }
                        }
                    }
                } else {
                    if let Some(until) = search_index_backoff_until {
                        let now = Instant::now();
                        if now < until {
                            let remaining = until.saturating_duration_since(now).as_secs();
                            info!(
                                "Skipping table {} for {}s while waiting for search index repair",
                                args.table, remaining
                            );
                            if args.run_once {
                                info!("Run-once mode: exiting after one pass");
                                return Ok(());
                            }
                            tokio::time::sleep(poll_interval).await;
                            continue;
                        }

                        info!("Search-index backoff expired; probing is_parent update path");
                        match pool.probe_is_parent_update_path(None).await {
                            Ok(()) => {
                                info!("Search index update path is healthy again; resuming");
                                search_index_backoff_until = None;
                            }
                            Err(e) if is_search_index_corruption_error(&e) => {
                                search_index_backoff_until =
                                    Some(Instant::now() + search_index_error_backoff);
                                warn!(
                                    "Search index is still unhealthy; backing off another {}s: {:#}",
                                    search_index_error_backoff.as_secs(),
                                    e
                                );
                                continue;
                            }
                            Err(e) => {
                                warn!(
                                    "Search index health probe failed with non-corruption error; backing off one poll: {:#}",
                                    e
                                );
                                continue;
                            }
                        }
                    }

                    if !args.skip_db_write {
                        match pool.probe_is_parent_update_path(None).await {
                            Ok(()) => {}
                            Err(e) if is_search_index_corruption_error(&e) => {
                                search_index_backoff_until =
                                    Some(Instant::now() + search_index_error_backoff);
                                warn!(
                                    "Search index update path is unhealthy; backing off for {}s without running dedupe: {:#}",
                                    search_index_error_backoff.as_secs(),
                                    e
                                );
                                continue;
                            }
                            Err(e) => {
                                warn!(
                                    "is_parent update health probe failed; skipping this poll: {:#}",
                                    e
                                );
                                continue;
                            }
                        }
                    }

                    info!("Found {} unprocessed documents", unprocessed);
                    last_activity = Instant::now();
                    memory_released = false;

                    let dedupe_config = DedupeConfig {
                        threshold: args.threshold,
                        size_diff_threshold: args.size_diff,
                        batch_size: args.batch_size,
                        num_workers,
                        use_disk_lsh: true,
                        disk_lsh_path: args.disk_lsh.clone(),
                        seed: args.seed,
                        process_all: false,
                        fresh: false,
                        data_dir: std::path::PathBuf::from(&args.data_dir),
                        skip_db_write: args.skip_db_write,
                        disk_phase2: true,
                        min_content_length: args.min_content_len,
                        edge_lookup_mode: args.edge_lookup.into(),
                    };

                    log_memory(&format!(
                        "Before processing PostgreSQL source {}",
                        db_config.source_name()
                    ));
                    match run_dedupe(db_config.clone(), dedupe_config).await {
                        Ok(stats) => {
                            info!(
                                "Completed {}: {} docs, {} dupes, {:.1}s",
                                db_config.source_name(),
                                stats.total_documents,
                                stats.duplicates_found,
                                stats.duration_secs
                            );
                            log_memory(&format!(
                                "After processing PostgreSQL source {}",
                                db_config.source_name()
                            ));
                            last_activity = Instant::now();
                        }
                        Err(e) => {
                            if is_search_index_corruption_error(&e) {
                                search_index_backoff_until =
                                    Some(Instant::now() + search_index_error_backoff);
                                warn!(
                                    "Detected search index corruption while processing {}; backing off for {}s before retrying",
                                    args.table,
                                    search_index_error_backoff.as_secs()
                                );
                            }
                            warn!("Error processing {}: {:#}", args.table, e);
                            log_memory(&format!("After error in {}", args.table));
                        }
                    }

                    release_memory_to_os();
                    log_memory(&format!("After malloc_trim for {}", args.table));
                }

                if args.run_once {
                    info!("Run-once mode: exiting after one pass");
                    return Ok(());
                }

                tokio::time::sleep(poll_interval).await;
            }
        }

        // PostgreSQL daemon mode
        info!("=== DAEMON MODE (PostgreSQL) ===");
        info!("Polling interval: {}s", args.interval);
        info!("Workers: {}", num_workers);
        info!("Data dir: {}", args.data_dir);
        info!("Edge lookup: {:?}", args.edge_lookup);
        if args.keep_in_memory {
            info!("Memory mode: CACHED (keep index in memory forever)");
        } else if args.memory_idle_timeout == 0 {
            info!("Memory mode: AGGRESSIVE (release after each batch)");
        } else {
            info!(
                "Memory mode: IDLE-RELEASE (release after {} min idle)",
                args.memory_idle_timeout
            );
        }
        info!("Press Ctrl+C to stop");
        info!("");

        let poll_interval = Duration::from_secs(args.interval);
        let memory_idle_timeout = Duration::from_secs(args.memory_idle_timeout * 60);
        let search_index_error_backoff = Duration::from_secs(args.search_index_error_backoff_secs);
        let mut last_activity = Instant::now();
        let mut memory_released = false;
        let mut search_index_backoff_until: HashMap<Uuid, Instant> = HashMap::new();

        // Create DB config and connection pool ONCE before the loop
        // (avoids connection churn on every poll iteration)
        let base_db_config = DbConfig::from_env()?.with_table(&args.table);
        let pool = DbPool::new(base_db_config.clone()).await?;

        loop {
            // Find datasets with unprocessed documents
            let datasets = match pool.get_datasets_with_unprocessed().await {
                Ok(datasets) => datasets,
                Err(e) => {
                    warn!(
                        "Failed to list datasets with unprocessed documents; backing off for {}s: {:#}",
                        args.interval, e
                    );
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
            };

            if datasets.is_empty() {
                tracing::debug!("No unprocessed documents found");

                // Check if we should release memory due to idle timeout
                if !args.keep_in_memory && !memory_released {
                    let idle_time = last_activity.elapsed();
                    if args.memory_idle_timeout == 0 || idle_time >= memory_idle_timeout {
                        release_memory_to_os();
                        memory_released = true;
                        if args.memory_idle_timeout > 0 {
                            info!(
                                "Released memory after {} min idle",
                                idle_time.as_secs() / 60
                            );
                        }
                    }
                }
            } else {
                info!(
                    "Found {} dataset(s) with unprocessed documents",
                    datasets.len()
                );
                last_activity = Instant::now();
                memory_released = false;

                for (dataset_id, dataset_name, unprocessed_count) in &datasets {
                    let now = Instant::now();
                    if let Some(until) = search_index_backoff_until.get(dataset_id).copied() {
                        if now < until {
                            let remaining = until.saturating_duration_since(now).as_secs();
                            info!(
                                "Skipping {} ({}) for {}s while waiting for search index repair",
                                dataset_name, dataset_id, remaining
                            );
                            continue;
                        }

                        info!(
                            "Search-index backoff expired for {}; probing is_parent update path",
                            dataset_name
                        );
                        match pool.probe_is_parent_update_path(Some(*dataset_id)).await {
                            Ok(()) => {
                                info!(
                                    "Search index update path is healthy again for {}; resuming",
                                    dataset_name
                                );
                                search_index_backoff_until.remove(dataset_id);
                            }
                            Err(e) if is_search_index_corruption_error(&e) => {
                                let next_until = Instant::now() + search_index_error_backoff;
                                search_index_backoff_until.insert(*dataset_id, next_until);
                                warn!(
                                    "Search index is still unhealthy for {}; backing off another {}s: {:#}",
                                    dataset_name,
                                    search_index_error_backoff.as_secs(),
                                    e
                                );
                                continue;
                            }
                            Err(e) => {
                                warn!(
                                    "Search index health probe failed for {} with non-corruption error; backing off one poll: {:#}",
                                    dataset_name, e
                                );
                                continue;
                            }
                        }
                    }

                    if !args.skip_db_write {
                        match pool.probe_is_parent_update_path(Some(*dataset_id)).await {
                            Ok(()) => {}
                            Err(e) if is_search_index_corruption_error(&e) => {
                                let until = Instant::now() + search_index_error_backoff;
                                search_index_backoff_until.insert(*dataset_id, until);
                                warn!(
                                    "Search index update path is unhealthy for {}; backing off for {}s without running dedupe: {:#}",
                                    dataset_name,
                                    search_index_error_backoff.as_secs(),
                                    e
                                );
                                continue;
                            }
                            Err(e) => {
                                warn!(
                                    "is_parent update health probe failed for {}; skipping this poll: {:#}",
                                    dataset_name, e
                                );
                                continue;
                            }
                        }
                    }

                    info!("");
                    log_memory(&format!("Before processing {}", dataset_name));
                    info!(
                        "Processing: {} ({}) - {} unprocessed docs",
                        dataset_name, dataset_id, unprocessed_count
                    );

                    let db_config = DbConfig::from_env()?
                        .with_dataset_name(*dataset_id, dataset_name)
                        .with_table(&args.table);

                    let Some(dataset_lock) =
                        DbPool::try_acquire_dataset_lock(&db_config, *dataset_id).await?
                    else {
                        info!(
                            "Skipping {} ({}) because another worker holds the dataset lock",
                            dataset_name, dataset_id
                        );
                        continue;
                    };

                    let dedupe_config = DedupeConfig {
                        threshold: args.threshold,
                        size_diff_threshold: args.size_diff,
                        batch_size: args.batch_size,
                        num_workers,
                        use_disk_lsh: true,
                        disk_lsh_path: args.disk_lsh.clone(),
                        seed: args.seed,
                        process_all: false, // Daemon mode only processes unprocessed docs
                        fresh: false,
                        data_dir: std::path::PathBuf::from(&args.data_dir),
                        skip_db_write: args.skip_db_write,
                        disk_phase2: true, // Daemon mode always uses disk-backed Phase 2.
                        min_content_length: args.min_content_len,
                        edge_lookup_mode: args.edge_lookup.into(),
                    };

                    let run_result = run_dedupe(db_config.clone(), dedupe_config.clone()).await;
                    if let Err(e) = dataset_lock.release().await {
                        warn!("Failed to release dataset lock for {}: {}", dataset_name, e);
                    }

                    match run_result {
                        Ok(stats) => {
                            info!(
                                "Completed {}: {} docs, {} dupes, {:.1}s",
                                dataset_name,
                                stats.total_documents,
                                stats.duplicates_found,
                                stats.duration_secs
                            );
                            log_memory(&format!("After processing {}", dataset_name));
                            last_activity = Instant::now();
                        }
                        Err(e) => {
                            // Show full error chain for debugging
                            if is_search_index_corruption_error(&e) {
                                let until = Instant::now() + search_index_error_backoff;
                                search_index_backoff_until.insert(*dataset_id, until);
                                warn!(
                                    "Detected search index corruption while processing {}; backing off for {}s before retrying",
                                    dataset_name,
                                    search_index_error_backoff.as_secs()
                                );
                            }
                            warn!("Error processing {}: {:#}", dataset_name, e);
                            log_memory(&format!("After error in {}", dataset_name));
                        }
                    }

                    // Always try to release memory after each dataset to prevent arena buildup
                    release_memory_to_os();
                    log_memory(&format!("After malloc_trim for {}", dataset_name));
                }
            }

            // Exit after one pass if --run-once is set
            if args.run_once {
                info!("Run-once mode: exiting after one pass");
                return Ok(());
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    // MODE 1: Index-only mode (--from-index)
    // Deduplicate from an existing LSH index, no database required
    if let Some(index_path) = &args.from_index {
        info!("=== INDEX-ONLY MODE ===");

        // If path is a directory, look for lsh.redb inside (consistent with --sync)
        let (lsh_path, output_dir) = if index_path.is_dir() {
            (index_path.join("lsh.redb"), index_path.clone())
        } else {
            // Legacy: direct path to lsh.redb file
            (
                index_path.clone(),
                index_path.parent().unwrap_or(index_path).to_path_buf(),
            )
        };

        if !lsh_path.exists() {
            anyhow::bail!("LSH index not found: {:?}", lsh_path);
        }

        info!("LSH index: {:?}", lsh_path);

        let output_dir = args.output_dir.clone().unwrap_or(output_dir);

        info!("Output dir: {:?}", output_dir);
        info!("Workers: {}", num_workers);
        info!("Threshold: {:.2}", args.threshold);
        info!("Size diff: {:.2}", args.size_diff);
        if args.fresh {
            info!("Mode: FRESH (ignoring existing state/matches)");
        } else {
            info!("Mode: RESUME (continue from existing state)");
        }

        // ============================================================
        // PHASE 1.5: Pathological Cluster Detection
        // Detect clusters of docs that match in 14+/16 LSH bands.
        // These have guaranteed Jaccard > 0.99 and would cause O(n²) comparisons.
        // We skip them in Phase 2 and write their matches directly.
        // ============================================================
        let doc_ids_to_process: Option<Vec<Uuid>> = {
            use incrededup::{run_phase_1_5, DiskLSH};

            let lsh = DiskLSH::open(&lsh_path)?;
            let min_bucket_size = 1000; // Same as main pipeline
            let min_bands = 14; // Require 14/16 bands overlap

            match run_phase_1_5(&lsh, &output_dir, min_bucket_size, min_bands) {
                Ok(pathological_ids) => {
                    if pathological_ids.is_empty() {
                        None // Process all docs
                    } else {
                        // Get all doc IDs and filter out pathological ones
                        let all_doc_ids = lsh.all_doc_ids()?;
                        let filtered: Vec<Uuid> = all_doc_ids
                            .into_iter()
                            .filter(|id| !pathological_ids.contains(id))
                            .collect();

                        info!(
                            "Filtered {} pathological docs, {} remaining for Phase 2",
                            pathological_ids.len(),
                            filtered.len()
                        );

                        Some(filtered)
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to detect pathological clusters: {}. Continuing anyway.",
                        e
                    );
                    None
                }
            }
            // lsh is dropped here, releasing the lock for run_disk_dedupe
        };

        let stats = run_disk_dedupe(
            &lsh_path,
            &output_dir,
            num_workers,
            args.threshold,
            args.size_diff,
            args.fresh,
            doc_ids_to_process,
        )?;

        info!("");
        info!("=== Phase 2 Results ===");
        info!("Total documents: {}", stats.total_documents);
        info!("Duplicates found: {}", stats.duplicates_found);
        info!("Duration: {:.2}s", stats.duration_secs);

        if stats.total_documents > 0 {
            let rate = stats.total_documents as f64 / stats.duration_secs;
            info!("Processing rate: {:.0} docs/sec", rate);

            let dedup_rate = stats.duplicates_found as f64 / stats.total_documents as f64 * 100.0;
            info!("Deduplication rate: {:.1}%", dedup_rate);
        }

        // Auto-sync to database unless --no-sync specified
        if !args.no_sync {
            info!("");
            info!("=== Syncing to Database ===");
            run_sync_mode(&output_dir, args.batch_size as usize, args.dry_run).await?;
        } else {
            info!("");
            info!("Skipping database sync (--no-sync specified)");
            info!("Run manually with: incrededup --sync {:?}", output_dir);
        }

        return Ok(());
    }

    // MODE 2a: SQLite mode (--sqlite)
    if let Some(sqlite_path) = &args.sqlite {
        info!("=== SQLITE MODE ===");
        info!("Database: {:?}", sqlite_path);

        let source = incrededup::SqliteSource::open(sqlite_path)?;
        let source_name = sqlite_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("sqlite");

        let dedupe_config = DedupeConfig {
            threshold: args.threshold,
            size_diff_threshold: args.size_diff,
            batch_size: args.batch_size,
            num_workers,
            use_disk_lsh: true,
            disk_lsh_path: args.disk_lsh.clone(),
            seed: args.seed,
            process_all: args.all || args.fresh,
            fresh: args.fresh || args.all,
            data_dir: std::path::PathBuf::from(&args.data_dir),
            skip_db_write: args.skip_db_write,
            disk_phase2: true,
            min_content_length: args.min_content_len,
            edge_lookup_mode: args.edge_lookup.into(),
        };
        if args.memory {
            warn!("--memory is deprecated and ignored; using disk-backed Phase 2");
        }

        info!("Configuration:");
        info!("  Threshold: {:.2}", dedupe_config.threshold);
        info!("  Size diff: {:.2}", dedupe_config.size_diff_threshold);
        info!("  Batch size: {}", dedupe_config.batch_size);
        info!("  Workers: {}", dedupe_config.num_workers);
        info!("  Data dir: {:?}", dedupe_config.data_dir);
        info!("  Edge lookup: {:?}", dedupe_config.edge_lookup_mode);
        info!(
            "  Min content length: {} chars",
            dedupe_config.min_content_length
        );
        info!("  Phase 2: DISK-BASED");
        if dedupe_config.process_all {
            info!("  Mode: FULL REPROCESS (--all flag)");
        } else {
            info!("  Mode: Incremental (only unprocessed docs)");
        }

        let stats =
            incrededup::run_dedupe_with_source(&source, dedupe_config, Some(source_name)).await?;

        info!("");
        info!("=== COMPLETED ===");
        info!("Total documents: {}", stats.total_documents);
        info!("Duplicates found: {}", stats.duplicates_found);
        info!("Unique parents: {}", stats.unique_parents);
        info!("Candidates checked: {}", stats.candidates_checked);
        info!("Duration: {:.2}s", stats.duration_secs);

        if stats.total_documents > 0 {
            let dedup_rate = stats.duplicates_found as f64 / stats.total_documents as f64 * 100.0;
            info!("Deduplication rate: {:.1}%", dedup_rate);
        }

        return Ok(());
    }

    // MODE 2b: PostgreSQL mode.
    if args.postgres || args.dataset.is_some() {
        info!("=== POSTGRESQL MODE ===");

        let db_config = postgres_config_from_args(&args).await?;
        info!("Table: {}", args.table);
        if db_config.source_name() != args.table {
            info!("Scope: {}", db_config.source_name());
        }

        let dedupe_config = DedupeConfig {
            threshold: args.threshold,
            size_diff_threshold: args.size_diff,
            batch_size: args.batch_size,
            num_workers,
            use_disk_lsh: true,
            disk_lsh_path: args.disk_lsh,
            seed: args.seed,
            process_all: args.all || args.fresh,
            fresh: args.fresh || args.all,
            data_dir: std::path::PathBuf::from(&args.data_dir),
            skip_db_write: args.skip_db_write,
            disk_phase2: true,
            min_content_length: args.min_content_len,
            edge_lookup_mode: args.edge_lookup.into(),
        };
        if args.memory {
            warn!("--memory is deprecated and ignored; using disk-backed Phase 2");
        }

        info!("Configuration:");
        info!("  Threshold: {:.2}", dedupe_config.threshold);
        info!("  Size diff: {:.2}", dedupe_config.size_diff_threshold);
        info!("  Batch size: {}", dedupe_config.batch_size);
        info!("  Workers: {}", dedupe_config.num_workers);
        info!("  Data dir: {:?}", dedupe_config.data_dir);
        info!("  Edge lookup: {:?}", dedupe_config.edge_lookup_mode);
        info!(
            "  Min content length: {} chars",
            dedupe_config.min_content_length
        );
        info!("  LSH mode: disk-backed (streaming)");
        info!("  Phase 2: DISK-BASED");
        if dedupe_config.process_all {
            info!("  Mode: FULL REPROCESS (--all flag, ignoring is_parent)");
        } else {
            info!("  Mode: Incremental (only unprocessed docs)");
        }
        if dedupe_config.skip_db_write {
            info!("  DB write: DISABLED (--skip-db-write)");
        }

        if args.dry_run {
            info!("Dry run mode - counting documents only");

            let pool = incrededup::DbPool::new(db_config).await?;
            let total = pool.count_total().await?;
            let unprocessed = pool.count_unprocessed().await?;

            info!("Total documents: {}", total);
            info!("Unprocessed (is_parent IS NULL): {}", unprocessed);
            return Ok(());
        }

        let stats = run_dedupe(db_config, dedupe_config).await?;

        info!("");
        info!("=== Results ===");
        info!("Total documents: {}", stats.total_documents);
        info!("Duplicates found: {}", stats.duplicates_found);
        info!("Unique parents: {}", stats.unique_parents);
        info!("Duration: {:.2}s", stats.duration_secs);

        if stats.total_documents > 0 {
            let rate = stats.total_documents as f64 / stats.duration_secs;
            info!("Processing rate: {:.0} docs/sec", rate);

            let dedup_rate = stats.duplicates_found as f64 / stats.total_documents as f64 * 100.0;
            info!("Deduplication rate: {:.1}%", dedup_rate);
        }

        return Ok(());
    }

    anyhow::bail!("No runnable mode selected. Use --postgres, --sqlite, or another mode.")
}

// =============================================================================
// Helper functions for --inspect and --sync modes
// =============================================================================

/// Build the adjacency side-index used by incremental Phase 3 edge lookup.
fn run_build_adjacency_mode(path: &Path) -> Result<()> {
    let matches_path = if path.is_dir() {
        path.join("matches.redb")
    } else {
        path.to_path_buf()
    };

    if !matches_path.exists() {
        anyhow::bail!("Error: {} does not exist", matches_path.display());
    }

    println!(
        "=== Building adjacency index for {} ===",
        matches_path.display()
    );
    println!("The daemon should be stopped while this runs so matches.redb is stable.");
    println!();

    let store = MatchStore::open(&matches_path)?;
    let was_built = store.is_adjacency_built()?;

    println!("Adjacency already marked built: {}", was_built);
    println!("Skipping pre-counts; counting redb tables is expensive at production scale.");

    let start = Instant::now();
    let stats = store.build_adjacency_index()?;
    let elapsed = start.elapsed();

    println!();
    println!("Indexed real edges: {}", stats.edges_indexed);
    println!(
        "Missing adjacency entries written: {}",
        stats.entries_written
    );
    println!("Adjacency marked built: {}", store.is_adjacency_built()?);
    println!("Duration: {:.2}s", elapsed.as_secs_f64());

    Ok(())
}

/// Inspect mode: analyze matches.redb file contents
fn run_inspect_mode(path: &Path, limit: usize, sample: bool) -> Result<()> {
    let mut path = path.to_path_buf();

    // If path is a directory, look for matches.redb inside
    if path.is_dir() {
        path = path.join("matches.redb");
    }

    if !path.exists() {
        anyhow::bail!("Error: {} does not exist", path.display());
    }

    println!("=== Inspecting {} ===\n", path.display());

    let store = MatchStore::open(&path)?;
    let total = store.count()?;

    println!("Total matches: {}\n", total);

    // Get all matches for analysis
    let matches = store.iter()?;

    // Jaccard distribution
    let mut jaccard_buckets: HashMap<String, usize> = HashMap::new();
    for m in &matches {
        let bucket = if m.jaccard_similarity >= 0.95 {
            "0.95-1.00"
        } else if m.jaccard_similarity >= 0.90 {
            "0.90-0.95"
        } else if m.jaccard_similarity >= 0.85 {
            "0.85-0.90"
        } else if m.jaccard_similarity >= 0.80 {
            "0.80-0.85"
        } else {
            "<0.80"
        };
        *jaccard_buckets.entry(bucket.to_string()).or_default() += 1;
    }

    println!("=== Jaccard Distribution ===");
    for bucket in ["0.95-1.00", "0.90-0.95", "0.85-0.90", "0.80-0.85", "<0.80"] {
        let count = jaccard_buckets.get(bucket).unwrap_or(&0);
        println!("  {}: {:>8}", bucket, count);
    }

    // Sample matches
    println!("\n=== Sample Matches (first {}) ===", limit);
    for (i, m) in matches.iter().take(limit).enumerate() {
        println!(
            "{:3}. child={} parent={} jaccard={:.4} size_diff={} pct={:.4}",
            i + 1,
            m.child_id,
            m.parent_id,
            m.jaccard_similarity,
            m.size_difference,
            m.size_difference_pct
        );
    }

    if sample && matches.len() > limit {
        println!("\n=== High Similarity Samples (jaccard >= 0.95) ===");
        let high_sim: Vec<_> = matches
            .iter()
            .filter(|m| m.jaccard_similarity >= 0.95)
            .take(10)
            .collect();
        for m in high_sim {
            println!(
                "  child={} parent={} jaccard={:.4}",
                m.child_id, m.parent_id, m.jaccard_similarity
            );
        }

        println!("\n=== Lower Similarity Samples (0.80-0.85) ===");
        let low_sim: Vec<_> = matches
            .iter()
            .filter(|m| m.jaccard_similarity >= 0.80 && m.jaccard_similarity < 0.85)
            .take(10)
            .collect();
        for m in low_sim {
            println!(
                "  child={} parent={} jaccard={:.4}",
                m.child_id, m.parent_id, m.jaccard_similarity
            );
        }
    }

    Ok(())
}

/// Sync mode: sync matches.redb to PostgreSQL with transitivity resolution
///
/// Supports resumable sync - if interrupted, will resume from where it left off.
/// Progress is tracked in state.redb. If sync was already completed, re-runs
/// (sync is idempotent - safe to run multiple times).
async fn run_sync_mode(path: &Path, batch_size: usize, dry_run: bool) -> Result<()> {
    use incrededup::{StateStore, SyncProgress, SyncStep};
    use std::time::{SystemTime, UNIX_EPOCH};

    if batch_size == 0 {
        anyhow::bail!("Batch size must be positive");
    }

    let base_path = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent().unwrap_or(path).to_path_buf()
    };

    let matches_path = base_path.join("matches.redb");
    let state_path = base_path.join("state.redb");

    if !matches_path.exists() {
        anyhow::bail!("Error: {} does not exist", matches_path.display());
    }

    println!("=== Syncing {} to database ===", matches_path.display());
    println!("Batch size: {}", batch_size);
    println!("Transitivity resolution: ENABLED (each child -> ONE canonical parent)");
    println!("Resumable: YES (progress tracked in state.redb)");
    if dry_run {
        println!("DRY RUN - no changes will be made");
    }
    println!();

    // Open state store to check for existing sync progress
    let state_store = StateStore::open(&state_path)?;
    let mut existing_progress = state_store.get_sync_progress()?;

    // Check sync state - resume if interrupted, restart if completed
    if existing_progress.step != SyncStep::NotStarted
        && existing_progress.step != SyncStep::Completed
    {
        println!(
            "[{}] RESUMING interrupted sync from step {:?}",
            Local::now().format("%H:%M:%S"),
            existing_progress.step
        );
        println!("  Previous progress:");
        println!(
            "    Dupes: {}/{}",
            existing_progress.dupes_written, existing_progress.dupes_total
        );
        println!(
            "    Parents: {}/{}",
            existing_progress.parents_marked, existing_progress.parents_total
        );
        println!(
            "    Children: {}/{}",
            existing_progress.children_marked, existing_progress.children_total
        );
        println!();
    } else if existing_progress.step == SyncStep::Completed {
        println!(
            "[{}] Previous sync completed, re-running (sync is idempotent)",
            Local::now().format("%H:%M:%S")
        );
        // Reset state to start fresh - update local variable too!
        state_store.reset_sync_progress()?;
        existing_progress = state_store.get_sync_progress()?;
    }

    // Open matches store
    let store = MatchStore::open(&matches_path)?;
    let total = store.count()?;
    println!("Total raw matches in file: {}", total);

    if total == 0 {
        println!("No duplicate edges in matches.redb; indexed docs will be marked as parents if lsh.redb is present.");
    }

    // Memory estimate: ~100 bytes per match for Union-Find + HashMap storage
    let estimated_mb = (total as f64 * 100.0) / (1024.0 * 1024.0);
    if estimated_mb > 1000.0 {
        println!(
            "WARNING: This operation will load {} matches into memory (~{:.1} GB estimated)",
            total,
            estimated_mb / 1024.0
        );
        println!(
            "         For very large datasets, consider running on a machine with sufficient RAM."
        );
    }

    // Load all matches and resolve transitivity
    // (We always need to do this, even when resuming, to get the parent/child lists)
    println!("Loading matches from disk...");
    let start = Instant::now();
    let matches = store.iter()?;
    println!(
        "Loaded {} raw matches in {:.2}s",
        matches.len(),
        start.elapsed().as_secs_f64()
    );

    println!("\nResolving transitivity (Union-Find)...");
    let start = Instant::now();
    let (mut resolved_matches, mut parent_ids, child_ids) = resolve_transitivity(&matches);
    resolved_matches.sort_by_key(|m| (m.child_id, m.parent_id));
    println!(
        "Transitivity resolved in {:.2}s",
        start.elapsed().as_secs_f64()
    );

    let lsh_path = base_path.join("lsh.redb");
    if lsh_path.exists() {
        println!("Loading indexed document IDs for parent coverage...");
        let lsh = incrededup::DiskLSH::open(&lsh_path)?;
        let indexed_doc_ids = lsh.all_doc_ids()?;
        for id in indexed_doc_ids {
            if !child_ids.contains(&id) {
                parent_ids.insert(id);
            }
        }
    }

    let filtered_parents_path = base_path.join("filtered_parents.redb");
    if filtered_parents_path.exists() {
        println!("Loading filtered parent IDs for sync coverage...");
        let filtered_store = FilteredParentStore::open(&filtered_parents_path)?;
        let filtered_parent_ids = filtered_store.iter()?;
        let filtered_count = filtered_parent_ids.len();
        parent_ids.extend(filtered_parent_ids);
        println!("Loaded {} filtered parent IDs", filtered_count);
    }

    println!("\n=== Transitivity Resolution Results ===");
    println!("Raw pairwise matches: {}", matches.len());
    println!(
        "Resolved assignments: {} (one per child)",
        resolved_matches.len()
    );
    println!("Unique parents (roots): {}", parent_ids.len());
    println!("Unique children: {}", child_ids.len());

    // Sanity check
    let unique_children_in_resolved: HashSet<_> =
        resolved_matches.iter().map(|m| m.child_id).collect();
    if unique_children_in_resolved.len() != resolved_matches.len() {
        eprintln!("WARNING: Duplicate children in resolved matches - this shouldn't happen!");
    } else {
        println!("Sanity check PASSED: Each child has exactly ONE parent");
    }

    if dry_run {
        println!(
            "\nDry run complete. Would write {} resolved assignments to dupes table.",
            resolved_matches.len()
        );
        return Ok(());
    }

    // Sort parents and children for consistent ordering on resume
    let mut parent_vec: Vec<_> = parent_ids.into_iter().collect();
    parent_vec.sort();
    let mut child_vec: Vec<_> = child_ids.into_iter().collect();
    child_vec.sort();

    let total_dupes = resolved_matches.len() as u64;
    let total_parents = parent_vec.len() as u64;
    let total_children = child_vec.len() as u64;

    // Initialize or update sync progress
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut progress = if existing_progress.step == SyncStep::NotStarted {
        SyncProgress {
            step: SyncStep::WritingDupes,
            dupes_written: 0,
            dupes_total: total_dupes,
            parents_marked: 0,
            parents_total: total_parents,
            children_marked: 0,
            children_total: total_children,
            started_at: now,
            completed_at: 0,
        }
    } else {
        // Update totals in case they changed (shouldn't happen but be safe)
        let mut p = existing_progress.clone();
        p.dupes_total = total_dupes;
        p.parents_total = total_parents;
        p.children_total = total_children;
        p
    };

    // Connect to database
    println!("\nConnecting to database...");
    let db_config = DbConfig::from_env()?;
    let pool = Arc::new(DbPool::new(db_config).await?);

    // Step 1: Write dupes
    if progress.step == SyncStep::WritingDupes {
        println!(
            "\n[{}] Writing {} resolved assignments to dupes table...",
            Local::now().format("%H:%M:%S"),
            total_dupes
        );
        if progress.dupes_written > 0 {
            println!(
                "  Resuming from offset {} (skipping already-written)",
                progress.dupes_written
            );
        }

        let start = Instant::now();
        let skip = progress.dupes_written as usize;
        let report_interval = (total_dupes / 10).max(100_000) as usize;

        for chunk in resolved_matches
            .iter()
            .skip(skip)
            .collect::<Vec<_>>()
            .chunks(batch_size)
        {
            let chunk_vec: Vec<_> = chunk.iter().map(|r| (*r).clone()).collect();
            pool.write_dupes(&chunk_vec).await?;
            progress.dupes_written += chunk_vec.len() as u64;

            // Checkpoint progress periodically
            if progress.dupes_written as usize % report_interval < batch_size
                || progress.dupes_written == total_dupes
            {
                let pct = 100.0 * progress.dupes_written as f64 / total_dupes as f64;
                let rate =
                    (progress.dupes_written - skip as u64) as f64 / start.elapsed().as_secs_f64();
                println!(
                    "  [{}] Written {}/{} ({:.0}%) - {:.0}/sec",
                    Local::now().format("%H:%M:%S"),
                    progress.dupes_written,
                    total_dupes,
                    pct,
                    rate
                );
                state_store.set_sync_progress(&progress)?;
            }
        }

        println!(
            "[{}] Wrote {} dupes in {:.2}s",
            Local::now().format("%H:%M:%S"),
            progress.dupes_written,
            start.elapsed().as_secs_f64()
        );

        // Move to next step
        progress.step = SyncStep::MarkingParents;
        state_store.set_sync_progress(&progress)?;
    }

    // Step 2: Mark parents (parallel workers)
    if progress.step == SyncStep::MarkingParents {
        // Get worker count from env, default to 8
        let num_workers: usize = std::env::var("SYNC_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8);
        let num_workers = num_workers.max(1);

        println!(
            "\n[{}] Marking {} parents (is_parent = true) with {} workers...",
            Local::now().format("%H:%M:%S"),
            total_parents,
            num_workers
        );

        // For resume, fall back to sequential (simpler tracking)
        if progress.parents_marked > 0 {
            println!(
                "  Resuming from offset {} (sequential mode)",
                progress.parents_marked
            );
            let start = Instant::now();
            let skip = progress.parents_marked as usize;
            let report_interval = (total_parents / 10).max(10_000) as usize;

            for chunk in parent_vec
                .iter()
                .skip(skip)
                .collect::<Vec<_>>()
                .chunks(batch_size)
            {
                let chunk_slice: Vec<Uuid> = chunk.iter().map(|u| **u).collect();
                pool.mark_as_parents(&chunk_slice).await?;
                progress.parents_marked += chunk.len() as u64;

                if progress.parents_marked as usize % report_interval < batch_size
                    || progress.parents_marked == total_parents
                {
                    let pct = 100.0 * progress.parents_marked as f64 / total_parents as f64;
                    let rate = (progress.parents_marked - skip as u64) as f64
                        / start.elapsed().as_secs_f64();
                    println!(
                        "  [{}] Marked {}/{} parents ({:.0}%) - {:.0}/sec",
                        Local::now().format("%H:%M:%S"),
                        progress.parents_marked,
                        total_parents,
                        pct,
                        rate
                    );
                    state_store.set_sync_progress(&progress)?;
                }
            }
            println!(
                "[{}] Marked {} parents in {:.2}s",
                Local::now().format("%H:%M:%S"),
                progress.parents_marked,
                start.elapsed().as_secs_f64()
            );
        } else {
            // Fresh start: use parallel workers
            let start = Instant::now();
            let counter = Arc::new(AtomicU64::new(0));
            let chunk_size = parent_vec.len().div_ceil(num_workers);

            let mut handles = Vec::new();
            for worker_id in 0..num_workers {
                let worker_start = worker_id * chunk_size;
                let worker_end = ((worker_id + 1) * chunk_size).min(parent_vec.len());
                if worker_start >= parent_vec.len() {
                    break;
                }

                let worker_ids: Vec<Uuid> = parent_vec[worker_start..worker_end].to_vec();
                let worker_pool = Arc::clone(&pool);
                let counter_clone = Arc::clone(&counter);
                let batch = batch_size;

                let handle = tokio::spawn(async move {
                    for chunk in worker_ids.chunks(batch) {
                        worker_pool.mark_as_parents(chunk).await?;
                        counter_clone.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    }
                    Ok::<_, anyhow::Error>(())
                });
                handles.push(handle);
            }

            // Progress reporter
            let counter_for_reporter = Arc::clone(&counter);
            let total = total_parents;
            let reporter = tokio::spawn(async move {
                let report_start = Instant::now();
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    let done = counter_for_reporter.load(Ordering::Relaxed);
                    if done >= total {
                        break;
                    }
                    let pct = 100.0 * done as f64 / total as f64;
                    let rate = done as f64 / report_start.elapsed().as_secs_f64();
                    println!(
                        "  [{}] Marked {}/{} parents ({:.0}%) - {:.0}/sec",
                        Local::now().format("%H:%M:%S"),
                        done,
                        total,
                        pct,
                        rate
                    );
                }
            });

            // Wait for all workers
            let results = join_all(handles).await;
            reporter.abort(); // Stop the reporter

            // Check for errors
            for result in results {
                result??;
            }

            progress.parents_marked = total_parents;
            println!(
                "[{}] Marked {} parents in {:.2}s ({} workers)",
                Local::now().format("%H:%M:%S"),
                total_parents,
                start.elapsed().as_secs_f64(),
                num_workers
            );
        }

        // Move to next step
        progress.step = SyncStep::MarkingChildren;
        state_store.set_sync_progress(&progress)?;
    }

    // Step 3: Mark children (parallel workers)
    if progress.step == SyncStep::MarkingChildren {
        // Get worker count from env, default to 8
        let num_workers: usize = std::env::var("SYNC_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8);
        let num_workers = num_workers.max(1);

        println!(
            "\n[{}] Marking {} children (is_parent = false) with {} workers...",
            Local::now().format("%H:%M:%S"),
            total_children,
            num_workers
        );

        // For resume, fall back to sequential (simpler tracking)
        if progress.children_marked > 0 {
            println!(
                "  Resuming from offset {} (sequential mode)",
                progress.children_marked
            );
            let start = Instant::now();
            let skip = progress.children_marked as usize;
            let report_interval = (total_children / 10).max(10_000) as usize;

            for chunk in child_vec
                .iter()
                .skip(skip)
                .collect::<Vec<_>>()
                .chunks(batch_size)
            {
                let chunk_slice: Vec<Uuid> = chunk.iter().map(|u| **u).collect();
                pool.mark_as_children(&chunk_slice).await?;
                progress.children_marked += chunk.len() as u64;

                if progress.children_marked as usize % report_interval < batch_size
                    || progress.children_marked == total_children
                {
                    let pct = 100.0 * progress.children_marked as f64 / total_children as f64;
                    let rate = (progress.children_marked - skip as u64) as f64
                        / start.elapsed().as_secs_f64();
                    println!(
                        "  [{}] Marked {}/{} children ({:.0}%) - {:.0}/sec",
                        Local::now().format("%H:%M:%S"),
                        progress.children_marked,
                        total_children,
                        pct,
                        rate
                    );
                    state_store.set_sync_progress(&progress)?;
                }
            }
            println!(
                "[{}] Marked {} children in {:.2}s",
                Local::now().format("%H:%M:%S"),
                progress.children_marked,
                start.elapsed().as_secs_f64()
            );
        } else {
            // Fresh start: use parallel workers
            let start = Instant::now();
            let counter = Arc::new(AtomicU64::new(0));
            let chunk_size = child_vec.len().div_ceil(num_workers);

            let mut handles = Vec::new();
            for worker_id in 0..num_workers {
                let worker_start = worker_id * chunk_size;
                let worker_end = ((worker_id + 1) * chunk_size).min(child_vec.len());
                if worker_start >= child_vec.len() {
                    break;
                }

                let worker_ids: Vec<Uuid> = child_vec[worker_start..worker_end].to_vec();
                let worker_pool = Arc::clone(&pool);
                let counter_clone = Arc::clone(&counter);
                let batch = batch_size;

                let handle = tokio::spawn(async move {
                    for chunk in worker_ids.chunks(batch) {
                        worker_pool.mark_as_children(chunk).await?;
                        counter_clone.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    }
                    Ok::<_, anyhow::Error>(())
                });
                handles.push(handle);
            }

            // Progress reporter
            let counter_for_reporter = Arc::clone(&counter);
            let total = total_children;
            let reporter = tokio::spawn(async move {
                let report_start = Instant::now();
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                    let done = counter_for_reporter.load(Ordering::Relaxed);
                    if done >= total {
                        break;
                    }
                    let pct = 100.0 * done as f64 / total as f64;
                    let rate = done as f64 / report_start.elapsed().as_secs_f64();
                    println!(
                        "  [{}] Marked {}/{} children ({:.0}%) - {:.0}/sec",
                        Local::now().format("%H:%M:%S"),
                        done,
                        total,
                        pct,
                        rate
                    );
                }
            });

            // Wait for all workers
            let results = join_all(handles).await;
            reporter.abort(); // Stop the reporter

            // Check for errors
            for result in results {
                result??;
            }

            progress.children_marked = total_children;
            println!(
                "[{}] Marked {} children in {:.2}s ({} workers)",
                Local::now().format("%H:%M:%S"),
                total_children,
                start.elapsed().as_secs_f64(),
                num_workers
            );
        }

        // Mark complete
        progress.step = SyncStep::Completed;
        progress.completed_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        state_store.set_sync_progress(&progress)?;
    }

    println!("\n=== Sync complete ===");
    println!("Each child now has exactly ONE canonical parent in the dupes table.");
    println!("Sync state saved to state.redb for verification.");

    Ok(())
}

/// Cleanup mode: detect and handle pathological clusters in LSH index
async fn run_cleanup_mode(
    path: &Path,
    action_str: &str,
    min_bucket_size: usize,
    min_bands: usize,
) -> Result<()> {
    use incrededup::{run_cleanup, CleanupAction, DbConfig, PostgresSource};

    let mut path = path.to_path_buf();

    // If path is a directory, use it as data_dir
    if !path.is_dir() {
        // If they passed a file (like lsh.redb), get its parent dir
        path = path.parent().unwrap_or(&path).to_path_buf();
    }

    // Parse cleanup action
    let action: CleanupAction = action_str.parse().map_err(|e: String| anyhow::anyhow!(e))?;

    info!("=== CLEANUP MODE ===");
    info!("Data directory: {:?}", path);
    info!("Action: {:?}", action);
    info!("Min bucket size: {}", min_bucket_size);
    info!("Min bands: {} of 16", min_bands);
    info!("");

    // For non-report actions, we need database access
    // Report mode doesn't need DB - it just analyzes the LSH index
    let source = if action != CleanupAction::Report {
        info!("Connecting to database...");
        let db_config = DbConfig::from_env()?;
        Some(PostgresSource::new(db_config).await?)
    } else {
        None
    };

    // Run cleanup
    let stats = run_cleanup(&path, source.as_ref(), action, min_bucket_size, min_bands).await?;

    info!("");
    info!("=== CLEANUP COMPLETE ===");
    info!("Clusters found: {}", stats.clusters_found);
    info!("Documents in clusters: {}", stats.docs_in_clusters);
    info!("Documents to delete: {}", stats.docs_to_delete);
    info!("Documents deleted: {}", stats.docs_deleted);
    info!("Documents marked as parents: {}", stats.docs_marked);

    Ok(())
}
