use crate::dedupe::EdgeLookupMode;
use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum EdgeLookupArg {
    /// Existing behavior: scan matches.redb for connected edges.
    Scan,
    /// Use the adjacency side-index once it has been fully backfilled.
    Auto,
    /// Compare adjacency output against scan output, but return scan output.
    Shadow,
}

impl From<EdgeLookupArg> for EdgeLookupMode {
    fn from(value: EdgeLookupArg) -> Self {
        match value {
            EdgeLookupArg::Scan => EdgeLookupMode::Scan,
            EdgeLookupArg::Auto => EdgeLookupMode::Auto,
            EdgeLookupArg::Shadow => EdgeLookupMode::Shadow,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "incrededup")]
#[command(about = "Performant, disk-based, incremental deduplication using MinHash LSH")]
#[command(version)]
pub struct Args {
    /// Process a PostgreSQL table using DATABASE_URL.
    ///
    /// By default this processes the whole table. Use --scope/--scope-where
    /// when one physical table contains multiple logical corpora.
    #[arg(long)]
    pub postgres: bool,

    /// Name for a logical subset of the PostgreSQL table.
    ///
    /// Requires --scope-where. The name is used for the sidecar directory.
    #[arg(long)]
    pub scope: Option<String>,

    /// Trusted SQL predicate for --scope, for example: corpus = 'news'.
    ///
    /// The predicate is appended to read/update queries as an additional WHERE
    /// condition. It should select a stable logical corpus.
    #[arg(long)]
    pub scope_where: Option<String>,

    /// Legacy dataset UUID or name to process (requires a datasets table and dataset_ids JSONB)
    #[arg(short, long)]
    pub dataset: Option<String>,

    /// Run deduplication from an existing LSH index.
    ///
    /// Point to an lsh.redb file or its containing sidecar directory.
    #[arg(long)]
    pub from_index: Option<PathBuf>,

    /// Output directory for --from-index mode (default: same dir as index)
    #[arg(long)]
    pub output_dir: Option<PathBuf>,

    /// Jaccard similarity threshold (0.0 - 1.0)
    #[arg(short, long, default_value = "0.8")]
    pub threshold: f64,

    /// Maximum size difference ratio to consider pairs
    #[arg(long, default_value = "0.3")]
    pub size_diff: f64,

    /// Database fetch batch size
    #[arg(short, long, default_value = "10000")]
    pub batch_size: i64,

    /// Deprecated and ignored. Sidecars live under --data-dir/<source>/.
    #[arg(long)]
    pub disk_lsh: Option<String>,

    /// MinHash seed for reproducibility
    #[arg(long, default_value = "42")]
    pub seed: u64,

    /// Database table name
    #[arg(long, default_value = "documents")]
    pub table: String,

    /// Number of worker threads (default: number of CPUs)
    #[arg(short, long)]
    pub workers: Option<usize>,

    /// Dry-run supported modes.
    ///
    /// PostgreSQL one-shot modes count documents only. --sync, including the
    /// auto-sync step after --from-index, resolves planned writes without
    /// writing. Other modes ignore this flag.
    #[arg(long)]
    pub dry_run: bool,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Process all documents and rebuild local sidecars from scratch
    #[arg(long)]
    pub all: bool,

    /// Base directory for data files (LSH index, matches, state)
    #[arg(long, default_value = "./data")]
    pub data_dir: String,

    /// Skip writing duplicate results and is_parent updates to the source.
    #[arg(long)]
    pub skip_db_write: bool,

    /// Deprecated. Phase 2 always uses the disk-backed implementation.
    #[arg(long)]
    pub memory: bool,

    /// Start fresh by clearing local sidecars before processing.
    #[arg(long)]
    pub fresh: bool,

    /// Skip database sync after --from-index completes (by default, syncs to DB)
    #[arg(long)]
    pub no_sync: bool,

    /// Run in daemon mode - continuously poll for unprocessed documents.
    ///
    /// With --postgres, polls the configured table. With --sqlite, polls that
    /// SQLite database. Without either, uses the legacy multi-dataset
    /// PostgreSQL schema.
    #[arg(long)]
    pub daemon: bool,

    /// Exit after one daemon pass instead of looping.
    ///
    /// Useful with cron and flock. Requires --daemon.
    #[arg(long)]
    pub run_once: bool,

    /// Polling interval in seconds for daemon mode (default: 5)
    #[arg(long, default_value = "5")]
    pub interval: u64,

    /// Log file path (for daemon mode). Logs to stdout if not specified.
    #[arg(long)]
    pub log_file: Option<PathBuf>,

    /// Minimum content length to index.
    ///
    /// Shorter documents are skipped and marked as parents.
    #[arg(long, default_value = "500")]
    pub min_content_len: i32,

    /// Connected-edge lookup mode for Phase 3: scan, auto, or shadow
    #[arg(long, value_enum, default_value = "scan")]
    pub edge_lookup: EdgeLookupArg,

    /// Sync matches to PostgreSQL with transitivity resolution.
    ///
    /// Point to a sidecar directory containing matches.redb.
    #[arg(long)]
    pub sync: Option<PathBuf>,

    /// Inspect matches.redb file contents.
    ///
    /// Point to a sidecar directory containing matches.redb.
    #[arg(long)]
    pub inspect: Option<PathBuf>,

    /// Build the adjacency side-index for a source directory or matches.redb file
    #[arg(long)]
    pub build_adjacency: Option<PathBuf>,

    /// Limit for --inspect mode (number of sample matches to show)
    #[arg(long, default_value = "20")]
    pub inspect_limit: usize,

    /// Show detailed samples in --inspect mode
    #[arg(long)]
    pub inspect_sample: bool,

    /// Use a SQLite database instead of PostgreSQL.
    ///
    /// Point to a .sqlite or .db file containing a documents table.
    #[arg(long)]
    pub sqlite: Option<PathBuf>,

    /// Detect and optionally handle pathological clusters.
    ///
    /// Point to a sidecar directory containing lsh.redb. Use --cleanup-action
    /// to choose report, mark-parent, or delete.
    #[arg(long)]
    pub cleanup: Option<PathBuf>,

    /// Action to take in cleanup mode: report (dry-run), mark-parent, delete
    #[arg(long, default_value = "report")]
    pub cleanup_action: String,

    /// Minimum bucket size to consider pathological (default: 10000)
    #[arg(long, default_value = "10000")]
    pub cleanup_min_bucket: usize,

    /// Minimum number of LSH bands to consider pathological (default: 14 of 16)
    #[arg(long, default_value = "14")]
    pub cleanup_min_bands: usize,

    /// Keep index memory resident in daemon mode.
    ///
    /// By default, daemon releases memory after --memory-idle-timeout minutes
    /// of no activity.
    #[arg(long)]
    pub keep_in_memory: bool,

    /// Minutes of idle time before releasing memory back to the OS.
    ///
    /// Set to 0 to release immediately after each batch.
    /// Ignored if --keep-in-memory is set
    #[arg(long, default_value = "60")]
    pub memory_idle_timeout: u64,

    /// Seconds to back off after transient search-index corruption is detected
    #[arg(long, default_value = "600")]
    pub search_index_error_backoff_secs: u64,
}
