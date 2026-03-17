use anyhow::{Result, bail};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::fmt;

/// Iceberg table metadata structure (minimal subset)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct TableMetadata {
    #[serde(rename = "format-version")]
    pub format_version: Option<u32>,
    pub table_uuid: Option<String>,
    pub location: Option<String>,
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Option<Vec<Snapshot>>,
    pub schema: Option<Schema>,
    #[serde(default)]
    pub schemas: Vec<Schema>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Snapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Schema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Field {
    pub id: Option<i32>,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub required: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ManifestListEntry {
    #[serde(rename = "manifest-path")]
    manifest_path: String,
    #[serde(rename = "manifest-length")]
    manifest_length: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ManifestEntry {
    status: i32, // 0 = EXISTING, 1 = ADDED, 2 = DELETED
    #[serde(rename = "data-file")]
    data_file: DataFile,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct DataFile {
    #[serde(rename = "file-path")]
    pub file_path: String,
    #[serde(rename = "file-format")]
    pub file_format: String,
    #[serde(rename = "record-count")]
    pub record_count: Option<i64>,
}

/// Represents a loaded Iceberg table
pub struct IcebergTable {
    pub name: String,
    pub location: String,
    metadata: Option<TableMetadata>,
}

impl IcebergTable {
    /// Load Iceberg table metadata (falls back to directory scan if missing)
    pub fn load(name: &str, location: &str) -> Result<Self> {
        let table_path = Path::new(location);
        let metadata_dir = table_path.join("metadata");

        let metadata = if metadata_dir.exists() {
            let mut metadata_files: Vec<PathBuf> = fs::read_dir(&metadata_dir)?
                .filter_map(|entry| {
                    let path = entry.ok()?.path();
                    if path.extension()? == "json" {
                        Some(path)
                    } else {
                        None
                    }
                })
                .collect();

            metadata_files.sort();

            if let Some(latest) = metadata_files.last() {
                let content = fs::read_to_string(latest)?;
                match serde_json::from_str::<TableMetadata>(&content) {
                    Ok(m) => {
                        println!(
                            "Loaded Iceberg metadata (format version {:?})",
                            m.format_version
                        );
                        Some(m)
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to parse metadata.json: {}. Falling back to directory scan.",
                            e
                        );
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
            println!("No Iceberg metadata found. Using directory scan fallback.");
        }

        Ok(Self {
            name: name.to_string(),
            location: location.to_string(),
            metadata,
        })
    }

    /// Return the schema fields, if metadata is available
    pub fn schema(&self) -> Option<&[Field]> {
        self.metadata
            .as_ref()
            .and_then(|m| m.schema.as_ref().map(|s| s.fields.as_slice()))
            .or_else(|| {
                self.metadata
                    .as_ref()
                    .and_then(|m| m.schemas.first().map(|s| s.fields.as_slice()))
            })
    }

    /// List Parquet files for the table
    pub fn parquet_files(&self) -> Result<Vec<String>> {
        let table_path = Path::new(&self.location);

        if let Some(ref metadata) = self.metadata {
            if let Some(snapshot_id) = metadata.current_snapshot_id {
                let snapshot = metadata
                    .snapshots
                    .as_ref()
                    .and_then(|s| s.iter().find(|snap| snap.snapshot_id == snapshot_id));

                if let Some(snap) = snapshot {
                    let manifest_list_path = if snap.manifest_list.starts_with('/') {
                        PathBuf::from(&snap.manifest_list)
                    } else {
                        table_path.join(&snap.manifest_list)
                    };

                    if manifest_list_path.exists() {
                        return self.read_manifest_list(&manifest_list_path, table_path);
                    }
                }
            }
        }

        // fallback
        self.fallback_directory_scan(table_path)
    }

    fn fallback_directory_scan(&self, table_path: &Path) -> Result<Vec<String>> {
        let mut files = Vec::new();

        // Check data/ subdir first
        let data_dir = table_path.join("data");
        if data_dir.exists() {
            for entry in fs::read_dir(&data_dir)? {
                let path = entry?.path();
                if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    files.push(path.to_string_lossy().to_string());
                }
            }
        }

        // Also check table root
        for entry in fs::read_dir(table_path)? {
            let path = entry?.path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                files.push(path.to_string_lossy().to_string());
            }
        }

        Ok(files)
    }

    fn read_manifest_list(&self, manifest_list_path: &Path, table_path: &Path) -> Result<Vec<String>> {
        // For simplicity, only handle JSON manifests here
        let content = fs::read_to_string(manifest_list_path)?;
        let mut data_files = Vec::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() { continue; }

            if let Ok(entry) = serde_json::from_str::<ManifestListEntry>(line) {
                let manifest_path = if entry.manifest_path.starts_with('/') {
                    PathBuf::from(&entry.manifest_path)
                } else {
                    table_path.join(&entry.manifest_path)
                };

                if manifest_path.exists() {
                    let manifest_content = fs::read_to_string(&manifest_path)?;
                    for l in manifest_content.lines() {
                        let l = l.trim();
                        if l.is_empty() { continue; }
                        if let Ok(m_entry) = serde_json::from_str::<ManifestEntry>(l) {
                            if m_entry.status != 2 && m_entry.data_file.file_format.to_lowercase() == "parquet" {
                                let full_path = if m_entry.data_file.file_path.starts_with('/') {
                                    m_entry.data_file.file_path.clone()
                                } else {
                                    table_path.join(&m_entry.data_file.file_path).to_string_lossy().to_string()
                                };
                                data_files.push(full_path);
                            }
                        }
                    }
                }
            }
        }

        Ok(data_files)
    }
}
