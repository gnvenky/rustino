use anyhow::Result;
use async_trait::async_trait;

/// Connector trait for different data sources (similar to Trino's connector SPI)
/// This allows Rustino to support multiple data sources: Iceberg, Parquet, CSV, etc.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Get the connector name
    fn name(&self) -> &str;

    /// List tables available in this connector
    async fn list_tables(&self) -> Result<Vec<String>>;

    /// Get table schema
    async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema>;

    /// Read data from a table (returns file paths or data directly)
    async fn read_table(&self, table_name: &str) -> Result<Vec<String>>;
}

/// Table schema information
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Iceberg connector implementation
pub struct IcebergConnector {
    pub catalog_path: String,
}

#[async_trait]
impl Connector for IcebergConnector {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        // Scan catalog directory for tables
        use std::fs;
        use std::path::Path;
        
        let catalog = Path::new(&self.catalog_path);
        let mut tables = Vec::new();
        
        if catalog.exists() {
            for entry in fs::read_dir(catalog)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    // Check if it's an Iceberg table (has metadata directory)
                    if path.join("metadata").exists() {
                        tables.push(path.file_name().unwrap().to_string_lossy().to_string());
                    }
                }
            }
        }
        
        Ok(tables)
    }
    
async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema> {
    use crate::iceberg::IcebergTable;

    let table_path = format!("{}/{}", self.catalog_path, table_name);
    let iceberg = IcebergTable::load(table_name, &table_path)?;

    if let Some(fields) = iceberg.schema() {
        let columns = fields.iter().map(|f| ColumnInfo {
            name: f.name.clone(),
            data_type: f.field_type.clone(),
            nullable: !f.required.unwrap_or(false),
        }).collect();

        Ok(TableSchema {
            name: table_name.to_string(),
            columns,
        })
    } else {
        anyhow::bail!("Table schema not available")
    }
}


    async fn read_table(&self, table_name: &str) -> Result<Vec<String>> {
        use crate::iceberg::IcebergTable;
        
        let table_path = format!("{}/{}", self.catalog_path, table_name);
        let iceberg = IcebergTable::load(table_name, &table_path)?;
        iceberg.parquet_files()
    }
}
