use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use std::path::Path;

pub struct Engine {
    pub ctx: SessionContext,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Register multiple parquet files as one logical table
    pub async fn register_parquet_files(
        &mut self,
        table_name: &str,
        files: Vec<String>,
    ) -> datafusion::error::Result<()> {
        // Register the *directory* containing the parquet files so DataFusion
        // will treat all files in that directory as one logical table.
        if let Some(first) = files.first() {
            let path = Path::new(first);
            let dir = if path.is_dir() {
                path
            } else {
                path.parent().unwrap_or(path)
            };

            self.ctx
                .register_parquet(
                    table_name,
                    dir.to_string_lossy().as_ref(),
                    ParquetReadOptions::default(),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn query(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        // Delegating to DataFusion to execute the SQL query
        let df = self.ctx.sql(sql).await?;
        df.collect().await
    }
}