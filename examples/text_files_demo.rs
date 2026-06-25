//! Demo: deduplicate plain text files from a directory.
//!
//! This example intentionally uses `.txt` files only. If your source material is
//! PDF, DOCX, HTML, email, or another container format, extract text first and
//! then feed the extracted text to `FileSystemSource`, `SqliteSource`,
//! `PostgresSource`, or a custom `DocumentSource`.
//!
//! Run with: cargo run --example text_files_demo

use incrededup::{
    run_dedupe_with_source, sources::filesystem::FileSystemConfig, DedupeConfig, FileSystemSource,
};
use std::fs;
use tempfile::TempDir;

fn repeated_text(topic: &str, suffix: &str) -> String {
    format!(
        "{topic} report with enough text to produce stable MinHash shingles. \
         The document discusses budget allocations, implementation timelines, \
         local review procedures, monitoring requirements, and public reporting. \
         The same core paragraph appears in related files so the duplicate \
         detector has overlapping shingles to compare. \
         Additional detail repeats the topic marker {topic} several times. \
         {topic} {topic} {topic}. {suffix}"
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("incrededup=info")
        .with_target(false)
        .init();

    let temp_dir = TempDir::new()?;
    let input_dir = temp_dir.path().join("texts");
    let data_dir = temp_dir.path().join("sidecars");
    let output_dir = temp_dir.path().join("report");

    fs::create_dir_all(&input_dir)?;
    fs::create_dir_all(&data_dir)?;
    fs::create_dir_all(&output_dir)?;

    fs::write(
        input_dir.join("program_a_original.txt"),
        repeated_text("program-a", "Original copy."),
    )?;
    fs::write(
        input_dir.join("program_a_revision.txt"),
        repeated_text("program-a", "Small revision with a few extra words."),
    )?;
    fs::write(
        input_dir.join("program_b_unique.txt"),
        repeated_text(
            "program-b",
            "Different topic and marker, included as a unique document.",
        ),
    )?;

    let source = FileSystemSource::new(
        FileSystemConfig::new(&input_dir)
            .with_extensions(vec!["txt"])
            .with_recursive(false),
    )
    .with_output_dir(&output_dir);

    let stats = run_dedupe_with_source(
        &source,
        DedupeConfig {
            threshold: 0.7,
            size_diff_threshold: 0.4,
            batch_size: 100,
            data_dir: data_dir.clone(),
            process_all: true,
            min_content_length: 100,
            ..Default::default()
        },
        Some("text_files_demo"),
    )
    .await?;

    let report_path = output_dir.join("duplicates.json");

    println!("Input directory: {}", input_dir.display());
    println!("Sidecar directory: {}", data_dir.display());
    println!("Duplicate report: {}", report_path.display());
    println!("Documents processed: {}", stats.total_documents);
    println!("Raw duplicate edges found: {}", stats.duplicates_found);
    println!("Candidates checked: {}", stats.candidates_checked);

    if report_path.exists() {
        println!("{}", fs::read_to_string(report_path)?);
    }

    Ok(())
}
