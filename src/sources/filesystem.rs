//! Filesystem document source implementation.
//!
//! Reads documents from files on disk. Each file is treated as a document.
//! Useful for deduplicating local file collections without a database.

use super::{DocumentSource, SourceDocument, SourceDupeMatch};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use uuid::Uuid;

/// Configuration for filesystem source
#[derive(Debug, Clone)]
pub struct FileSystemConfig {
    /// Root directory to scan for documents
    pub root_dir: PathBuf,
    /// File extensions to include (e.g., ["txt", "md"]). Empty = all files.
    pub extensions: Vec<String>,
    /// Whether to scan subdirectories recursively
    pub recursive: bool,
    /// Minimum file size in bytes to include
    pub min_size: u64,
    /// Maximum file size in bytes to include (0 = unlimited)
    pub max_size: u64,
}

impl Default for FileSystemConfig {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("."),
            extensions: vec![],
            recursive: true,
            min_size: 0,
            max_size: 0, // unlimited
        }
    }
}

impl FileSystemConfig {
    pub fn new<P: AsRef<Path>>(root_dir: P) -> Self {
        Self {
            root_dir: root_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    pub fn with_extensions(mut self, extensions: Vec<&str>) -> Self {
        self.extensions = extensions.into_iter().map(String::from).collect();
        self
    }

    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn with_min_size(mut self, min_size: u64) -> Self {
        self.min_size = min_size;
        self
    }

    pub fn with_max_size(mut self, max_size: u64) -> Self {
        self.max_size = max_size;
        self
    }
}

/// Filesystem document source.
///
/// Scans a directory for files and treats each file as a document.
/// Documents are identified by a UUID generated from the file path.
pub struct FileSystemSource {
    config: FileSystemConfig,
    /// Cache of file path -> UUID mapping (computed once on first scan)
    file_map: RwLock<Option<FileMap>>,
    /// Output directory for results (optional)
    output_dir: Option<PathBuf>,
}

struct FileMap {
    /// path -> (uuid, file_size)
    #[allow(dead_code)] // Reserved for future use (path lookup by canonical path)
    path_to_uuid: HashMap<PathBuf, (Uuid, u64)>,
    /// uuid -> path (for reverse lookup)
    uuid_to_path: HashMap<Uuid, PathBuf>,
    /// Sorted list of (uuid, path) for pagination
    sorted_entries: Vec<(Uuid, PathBuf)>,
}

impl FileSystemSource {
    /// Create a new filesystem source from config
    pub fn new(config: FileSystemConfig) -> Self {
        Self {
            config,
            file_map: RwLock::new(None),
            output_dir: None,
        }
    }

    /// Create a filesystem source for a directory with default settings
    pub fn from_dir<P: AsRef<Path>>(dir: P) -> Self {
        Self::new(FileSystemConfig::new(dir))
    }

    /// Set output directory for results (duplicates report)
    pub fn with_output_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
        self.output_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Generate a deterministic UUID from a file path
    fn path_to_uuid(path: &Path) -> Uuid {
        let path_string = path.to_string_lossy();
        let mut h2_input = Vec::with_capacity(path_string.len() + 18);
        h2_input.extend_from_slice(b"incrededup-fs-v1:");
        h2_input.extend_from_slice(path_string.as_bytes());

        let high = crate::minhash::calculate_hash_fast(path_string.as_bytes());
        let low = crate::minhash::calculate_hash_fast(&h2_input);
        Uuid::from_u128((u128::from(high) << 64) | u128::from(low))
    }

    /// Scan directory and build file map
    fn scan_directory(&self) -> Result<FileMap> {
        let mut path_to_uuid = HashMap::new();
        let mut uuid_to_path = HashMap::new();

        self.scan_dir_recursive(&self.config.root_dir, &mut path_to_uuid, &mut uuid_to_path)?;

        // Sort entries by UUID for consistent pagination
        let mut sorted_entries: Vec<(Uuid, PathBuf)> =
            uuid_to_path.iter().map(|(u, p)| (*u, p.clone())).collect();
        sorted_entries.sort_by_key(|(u, _)| *u);

        Ok(FileMap {
            path_to_uuid,
            uuid_to_path,
            sorted_entries,
        })
    }

    fn scan_dir_recursive(
        &self,
        dir: &Path,
        path_to_uuid: &mut HashMap<PathBuf, (Uuid, u64)>,
        uuid_to_path: &mut HashMap<Uuid, PathBuf>,
    ) -> Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }

        let entries =
            fs::read_dir(dir).with_context(|| format!("Failed to read directory: {:?}", dir))?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if self.config.recursive {
                    self.scan_dir_recursive(&path, path_to_uuid, uuid_to_path)?;
                }
            } else if path.is_file() {
                // Check extension filter
                if !self.config.extensions.is_empty() {
                    if let Some(ext) = path.extension() {
                        let ext_str = ext.to_string_lossy().to_lowercase();
                        if !self
                            .config
                            .extensions
                            .iter()
                            .any(|e| e.to_lowercase() == ext_str)
                        {
                            continue;
                        }
                    } else {
                        continue; // No extension, skip
                    }
                }

                // Check size filters
                let metadata = fs::metadata(&path)?;
                let size = metadata.len();

                if size < self.config.min_size {
                    continue;
                }
                if self.config.max_size > 0 && size > self.config.max_size {
                    continue;
                }

                let stable_path = path.canonicalize().unwrap_or(path);
                let uuid = Self::path_to_uuid(&stable_path);
                path_to_uuid.insert(stable_path.clone(), (uuid, size));
                uuid_to_path.insert(uuid, stable_path);
            }
        }

        Ok(())
    }

    /// Ensure file map is initialized
    fn ensure_scanned(&self) -> Result<()> {
        let needs_scan = self.file_map.read().unwrap().is_none();
        if needs_scan {
            let map = self.scan_directory()?;
            *self.file_map.write().unwrap() = Some(map);
        }
        Ok(())
    }

    /// Read a file's content
    fn read_file_content(path: &Path) -> Result<String> {
        fs::read_to_string(path)
            .or_else(|_| -> std::io::Result<String> {
                // Try reading as bytes and converting with lossy UTF-8
                let bytes = fs::read(path)?;
                Ok(String::from_utf8_lossy(&bytes).into_owned())
            })
            .with_context(|| format!("Failed to read file: {:?}", path))
    }

    /// Get the path for a document UUID
    pub fn get_path(&self, uuid: &Uuid) -> Option<PathBuf> {
        self.file_map
            .read()
            .unwrap()
            .as_ref()
            .and_then(|m| m.uuid_to_path.get(uuid).cloned())
    }
}

#[async_trait]
impl DocumentSource for FileSystemSource {
    async fn source_name(&self) -> Result<String> {
        Ok(self.config.root_dir.display().to_string())
    }

    async fn count_total(&self) -> Result<i64> {
        self.ensure_scanned()?;
        let map = self.file_map.read().unwrap();
        Ok(map.as_ref().map(|m| m.sorted_entries.len()).unwrap_or(0) as i64)
    }

    async fn fetch_all_after(
        &self,
        last_id: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<SourceDocument>> {
        self.ensure_scanned()?;

        let map_guard = self.file_map.read().unwrap();
        let map = map_guard.as_ref().unwrap();

        // Find starting position
        let start_idx = match last_id {
            Some(id) => {
                // Find the position after `id`
                map.sorted_entries
                    .iter()
                    .position(|(u, _)| *u > id)
                    .unwrap_or(map.sorted_entries.len())
            }
            None => 0,
        };

        let end_idx = (start_idx + limit as usize).min(map.sorted_entries.len());
        let mut docs = Vec::with_capacity(end_idx - start_idx);

        for (uuid, path) in &map.sorted_entries[start_idx..end_idx] {
            let content = Self::read_file_content(path)?;
            let content_len = content.len() as i32;
            let filename = path.file_name().map(|n| n.to_string_lossy().into_owned());

            docs.push(SourceDocument {
                id: *uuid,
                content,
                content_len,
                filename,
            });
        }

        Ok(docs)
    }

    async fn fetch_by_ids(&self, ids: &[Uuid]) -> Result<Vec<SourceDocument>> {
        self.ensure_scanned()?;

        let map_guard = self.file_map.read().unwrap();
        let map = map_guard.as_ref().unwrap();

        let mut docs = Vec::with_capacity(ids.len());

        for id in ids {
            if let Some(path) = map.uuid_to_path.get(id) {
                let content = Self::read_file_content(path)?;
                let content_len = content.len() as i32;
                let filename = path.file_name().map(|n| n.to_string_lossy().into_owned());

                docs.push(SourceDocument {
                    id: *id,
                    content,
                    content_len,
                    filename,
                });
            }
        }

        Ok(docs)
    }

    async fn write_dupes(&self, matches: &[SourceDupeMatch]) -> Result<u64> {
        // Write results to a JSON file if output_dir is set
        if let Some(output_dir) = &self.output_dir {
            fs::create_dir_all(output_dir)?;

            let map_guard = self.file_map.read().unwrap();
            let map = map_guard.as_ref().unwrap();

            // Build human-readable output
            let mut results = Vec::new();
            for m in matches {
                let child_path = map.uuid_to_path.get(&m.child_id);
                let parent_path = map.uuid_to_path.get(&m.parent_id);

                results.push(serde_json::json!({
                    "child_id": m.child_id.to_string(),
                    "child_path": child_path.map(|p| p.display().to_string()),
                    "parent_id": m.parent_id.to_string(),
                    "parent_path": parent_path.map(|p| p.display().to_string()),
                    "jaccard_similarity": m.jaccard_similarity,
                    "size_difference_pct": m.size_difference_pct,
                }));
            }

            let output_path = output_dir.join("duplicates.json");
            let json = serde_json::to_string_pretty(&results)?;
            fs::write(&output_path, json)?;

            tracing::info!(
                "Wrote {} duplicate pairs to {:?}",
                matches.len(),
                output_path
            );
        }

        Ok(matches.len() as u64)
    }

    fn supports_write(&self) -> bool {
        self.output_dir.is_some()
    }

    fn tracks_state(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_files(dir: &TempDir) -> Vec<PathBuf> {
        let files = vec![
            ("doc1.txt", "This is the first document with some content."),
            (
                "doc2.txt",
                "This is the second document with different content.",
            ),
            (
                "doc3.md",
                "# Markdown Document\n\nThis is markdown content.",
            ),
        ];

        let mut paths = Vec::new();
        for (name, content) in files {
            let path = dir.path().join(name);
            fs::write(&path, content).unwrap();
            paths.push(path);
        }
        paths
    }

    #[tokio::test]
    async fn test_filesystem_source_count() {
        let temp_dir = TempDir::new().unwrap();
        create_test_files(&temp_dir);

        let source = FileSystemSource::from_dir(temp_dir.path());
        let count = source.count_total().await.unwrap();

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_filesystem_source_fetch_all() {
        let temp_dir = TempDir::new().unwrap();
        create_test_files(&temp_dir);

        let source = FileSystemSource::from_dir(temp_dir.path());
        let docs = source.fetch_all_after(None, 100).await.unwrap();

        assert_eq!(docs.len(), 3);
        for doc in &docs {
            assert!(!doc.content.is_empty());
            assert!(doc.filename.is_some());
        }
    }

    #[tokio::test]
    async fn test_filesystem_source_pagination() {
        let temp_dir = TempDir::new().unwrap();
        create_test_files(&temp_dir);

        let source = FileSystemSource::from_dir(temp_dir.path());

        // Fetch first 2
        let batch1 = source.fetch_all_after(None, 2).await.unwrap();
        assert_eq!(batch1.len(), 2);

        // Fetch next batch starting after last ID
        let last_id = batch1.last().unwrap().id;
        let batch2 = source.fetch_all_after(Some(last_id), 2).await.unwrap();
        assert_eq!(batch2.len(), 1);

        // No overlap
        assert!(batch1.iter().all(|d| d.id != batch2[0].id));
    }

    #[tokio::test]
    async fn test_filesystem_source_extension_filter() {
        let temp_dir = TempDir::new().unwrap();
        create_test_files(&temp_dir);

        let config = FileSystemConfig::new(temp_dir.path()).with_extensions(vec!["txt"]);
        let source = FileSystemSource::new(config);

        let count = source.count_total().await.unwrap();
        assert_eq!(count, 2); // Only .txt files
    }

    #[tokio::test]
    async fn test_filesystem_source_fetch_by_ids() {
        let temp_dir = TempDir::new().unwrap();
        create_test_files(&temp_dir);

        let source = FileSystemSource::from_dir(temp_dir.path());
        let all_docs = source.fetch_all_after(None, 100).await.unwrap();

        // Fetch just the first document by ID
        let ids = vec![all_docs[0].id];
        let fetched = source.fetch_by_ids(&ids).await.unwrap();

        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].id, all_docs[0].id);
        assert_eq!(fetched[0].content, all_docs[0].content);
    }

    #[tokio::test]
    async fn test_filesystem_source_deterministic_uuid() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.txt");
        fs::write(&path, "content").unwrap();

        // Same path should always produce same UUID
        let uuid1 = FileSystemSource::path_to_uuid(&path);
        let uuid2 = FileSystemSource::path_to_uuid(&path);
        assert_eq!(uuid1, uuid2);
    }
}
