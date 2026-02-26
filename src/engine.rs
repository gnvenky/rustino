use datafusion::prelude::*;
use arrow::record_batch::RecordBatch;

pub struct Engine {
    ctx: SessionContext,
}

impl Engine {
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        Self { ctx }
    }

    pub async fn execute_sql(
        &self,
        sql: &str,
    ) -> datafusion::error::Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        df.collect().await
    }

    pub async fn register_parquet(
        &self,
        table_name: &str,
        path: &str,
    ) -> datafusion::error::Result<()> {
        self.ctx
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await?;
        Ok(())
    }
}
