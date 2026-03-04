use anyhow::Result;
use std::fs;
use std::path::Path;

pub struct IcebergTable {
    pub name: String,
    pub location: String,
}

impl IcebergTable {
    pub fn load(name: &str, location: &str) -> Result<Self> {
        Ok(Self { name: name.to_string(), location: location.to_string() })
    }

    pub fn parquet_files(&self) -> Result<Vec<String>> {
        let mut files = Vec::new();
        let data_dir = Path::new(&self.location).join("data");
        for entry in fs::read_dir(&data_dir)? {
            let path = entry?.path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                files.push(path.to_string_lossy().to_string());
            }
        }
        Ok(files)
    }
}