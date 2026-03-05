use anyhow::{Result, Context, bail};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

/// Iceberg table metadata structure (minimal subset)
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields are deserialized but may not all be used yet
struct TableMetadata {
    #[serde(rename = "format-version")]
    format_version: u32,
    #[serde(rename = "table-uuid")]
    table_uuid: String,
    location: String,
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: Option<i64>,
    snapshots: Option<Vec<Snapshot>>,
    #[serde(rename = "schema")]
    schema: Schema,
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields are deserialized but may not all be used yet
pub struct Schema {
    #[serde(rename = "type")]
    schema_type: String,
    fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields are deserialized but may not all be used yet
struct Field {
    id: i32,
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    required: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields are deserialized but may not all be used yet
struct ManifestListEntry {
    #[serde(rename = "manifest-path")]
    manifest_path: String,
    #[serde(rename = "manifest-length")]
    manifest_length: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Reserved for future use
struct ManifestFile {
    #[serde(rename = "manifest-path")]
    manifest_path: String,
    entries: Vec<ManifestEntry>,
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    status: i32, // 0 = EXISTING, 1 = ADDED, 2 = DELETED
    #[serde(rename = "data-file")]
    data_file: DataFile,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields are deserialized but may not all be used yet
struct DataFile {
    #[serde(rename = "file-path")]
    file_path: String,
    #[serde(rename = "file-format")]
    file_format: String,
    #[serde(rename = "record-count")]
    record_count: Option<i64>,
}

pub struct IcebergTable {
    pub name: String,
    pub location: String,
    metadata: Option<TableMetadata>,
}

impl IcebergTable {
    /// Load an Iceberg table by reading its metadata.json file
    /// Falls back to simple directory mode if metadata is not found
    pub fn load(name: &str, location: &str) -> Result<Self> {
        let table_path = Path::new(location);
        
        // Try to find and load metadata.json file
        // Iceberg stores metadata in metadata/ directory with versioned files like:
        // metadata/00000-<uuid>.metadata.json, metadata/00001-<uuid>.metadata.json, etc.
        let metadata_dir = table_path.join("metadata");
        let metadata = if metadata_dir.exists() {
            // Find the highest versioned metadata file
            let mut metadata_files: Vec<PathBuf> = fs::read_dir(&metadata_dir)?
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let path = entry.path();
                    if path.extension()? == "metadata.json" {
                        Some(path)
                    } else {
                        None
                    }
                })
                .collect();

            if !metadata_files.is_empty() {
                // Sort by filename (which contains version number) and take the latest
                metadata_files.sort();
                let latest_metadata = metadata_files.last()
                    .context("Failed to find latest metadata file")?;

                // Read and parse metadata.json
                let metadata_content = fs::read_to_string(latest_metadata)
                    .with_context(|| format!("Failed to read metadata file: {}", latest_metadata.display()))?;
                
                match serde_json::from_str::<TableMetadata>(&metadata_content) {
                    Ok(m) => {
                        println!("Loaded Iceberg table with metadata (format version {})", m.format_version);
                        Some(m)
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to parse metadata.json: {}. Falling back to directory scanning.", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        if metadata.is_none() {
            println!("No Iceberg metadata found. Using directory scanning mode.");
        }

        Ok(Self {
            name: name.to_string(),
            location: location.to_string(),
            metadata,
        })
    }

    /// Get parquet files by reading Iceberg manifests (proper Iceberg way)
    /// Falls back to directory scanning if metadata is not available
    pub fn parquet_files(&self) -> Result<Vec<String>> {
        let table_path = Path::new(&self.location);

        // If we have metadata, use proper Iceberg manifest reading
        if let Some(ref metadata) = self.metadata {
            // Get current snapshot
            let snapshot_id = metadata.current_snapshot_id
                .context("Table has no current snapshot")?;

            // Find the snapshot in the snapshots array
            let snapshot = metadata.snapshots
                .as_ref()
                .and_then(|snapshots| snapshots.iter().find(|s| s.snapshot_id == snapshot_id))
                .context("Current snapshot not found in metadata")?;

            // Read manifest list
            // The manifest-list path is relative to table root
            let manifest_list_path = if snapshot.manifest_list.starts_with('/') {
                PathBuf::from(&snapshot.manifest_list)
            } else {
                table_path.join(&snapshot.manifest_list)
            };

            if !manifest_list_path.exists() {
                // Try alternative: manifest list might be in metadata/ directory
                let alt_path = table_path.join("metadata").join(
                    Path::new(&snapshot.manifest_list).file_name()
                        .context("Invalid manifest list path")?
                );
                if alt_path.exists() {
                    // Check if it's Avro format (snapshot files are often Avro)
                    if alt_path.extension().map(|e| e == "avro").unwrap_or(false) {
                        eprintln!("Warning: Snapshot file is Avro format. Avro support not yet implemented.");
                        eprintln!("Falling back to directory scanning.");
                        return self.fallback_directory_scan(table_path);
                    }
                    return self.read_manifest_list(&alt_path, table_path);
                }
                bail!("Manifest list not found: {}", manifest_list_path.display());
            }

            // Check if manifest list is Avro format
            if manifest_list_path.extension().map(|e| e == "avro").unwrap_or(false) {
                eprintln!("Warning: Snapshot/manifest list is Avro format. Avro support not yet implemented.");
                eprintln!("Falling back to directory scanning.");
                return self.fallback_directory_scan(table_path);
            }

            return self.read_manifest_list(&manifest_list_path, table_path);
        }

        // Fallback: simple directory scanning for tables without metadata
        self.fallback_directory_scan(table_path)
    }

    fn fallback_directory_scan(&self, table_path: &Path) -> Result<Vec<String>> {
        let mut files = Vec::new();
        let data_dir = table_path.join("data");
        if data_dir.exists() {
            for entry in fs::read_dir(&data_dir)? {
                let path = entry?.path();
                if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    files.push(path.to_string_lossy().to_string());
                }
            }
        } else {
            // Also check if parquet files are directly in the table directory
            for entry in fs::read_dir(table_path)? {
                let path = entry?.path();
                if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    files.push(path.to_string_lossy().to_string());
                }
            }
        }
        Ok(files)
    }

    fn read_manifest_list(&self, manifest_list_path: &Path, table_path: &Path) -> Result<Vec<String>> {
        // Check if manifest is Avro format (Iceberg v1/v2 use Avro for manifests)
        if manifest_list_path.extension().map(|e| e == "avro").unwrap_or(false) {
            eprintln!("Warning: Avro manifest files detected. Avro support not yet implemented.");
            eprintln!("Falling back to directory scanning. To enable full Iceberg support, add Avro dependency.");
            return self.fallback_directory_scan(table_path);
        }

        // Try to read as NDJSON (some Iceberg implementations use this)
        let content = match fs::read_to_string(manifest_list_path) {
            Ok(c) => c,
            Err(_) => {
                // If reading as text fails, it's likely binary (Avro)
                eprintln!("Warning: Manifest file appears to be binary (likely Avro). Falling back to directory scanning.");
                return self.fallback_directory_scan(table_path);
            }
        };

        let mut data_files = Vec::new();

        // Parse NDJSON (one JSON object per line)
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let entry: ManifestListEntry = serde_json::from_str(line)
                .context("Failed to parse manifest list entry")?;

            // Read the manifest file
            let manifest_path = if entry.manifest_path.starts_with('/') {
                PathBuf::from(&entry.manifest_path)
            } else {
                table_path.join(&entry.manifest_path)
            };

            if !manifest_path.exists() {
                // Try alternative location (in metadata directory)
                let alt_path = table_path.join("metadata").join(
                    Path::new(&entry.manifest_path).file_name()
                        .context("Invalid manifest path")?
                );
                if alt_path.exists() {
                    // Check if it's Avro format
                    if alt_path.extension().map(|e| e == "avro").unwrap_or(false) {
                        // Skip Avro manifests for now
                        continue;
                    }
                    self.read_manifest(&alt_path, table_path, &mut data_files)?;
                    continue;
                }
                eprintln!("Warning: Manifest file not found: {}", manifest_path.display());
                continue;
            }

            // Check if manifest is Avro format
            if manifest_path.extension().map(|e| e == "avro").unwrap_or(false) {
                // Skip Avro manifests for now
                continue;
            }

            self.read_manifest(&manifest_path, table_path, &mut data_files)?;
        }

        Ok(data_files)
    }

    fn read_manifest(&self, manifest_path: &Path, table_path: &Path, data_files: &mut Vec<String>) -> Result<()> {
        // Manifest files are also NDJSON
        let content = fs::read_to_string(manifest_path)
            .with_context(|| format!("Failed to read manifest: {}", manifest_path.display()))?;

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let entry: ManifestEntry = serde_json::from_str(line)
                .context("Failed to parse manifest entry")?;

            // Only include EXISTING (0) or ADDED (1) files, skip DELETED (2)
            if entry.status == 2 {
                continue;
            }

            // Only include Parquet files
            if entry.data_file.file_format.to_lowercase() != "parquet" {
                continue;
            }

            // Resolve data file path
            let file_path = if entry.data_file.file_path.starts_with('/') {
                entry.data_file.file_path.clone()
            } else {
                table_path.join(&entry.data_file.file_path)
                    .to_string_lossy()
                    .to_string()
            };

            data_files.push(file_path);
        }

        Ok(())
    }

    /// Get the table schema (if metadata is available)
    #[allow(dead_code)] // Public API method, may be used by callers
    pub fn schema(&self) -> Option<&Schema> {
        self.metadata.as_ref().map(|m| &m.schema)
    }

    /// Get format version (if metadata is available)
    #[allow(dead_code)] // Public API method, may be used by callers
    pub fn format_version(&self) -> Option<u32> {
        self.metadata.as_ref().map(|m| m.format_version)
    }
}
