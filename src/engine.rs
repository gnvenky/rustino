use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;

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
        self.ctx
            .register_parquet(table_name, &files, ParquetReadOptions::default())
            .await?;
        Ok(())
    }

    pub async fn query(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        df.collect().await
    }
}