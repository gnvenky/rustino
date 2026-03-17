mod engine;
mod worker;
mod iceberg;
mod coordinator;
mod connector;
mod planner;
mod task;

use engine::Engine;
use worker::Worker;
use iceberg::IcebergTable;
use coordinator::Coordinator;
use datafusion::arrow::util::pretty::print_batches;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut engine = Engine::new();

    // Load Iceberg table
    let iceberg = IcebergTable::load(
        "example_table",
        "/tmp/warehouse/default/example_table"
    )?;

    // Get parquet files
    let files = iceberg.parquet_files()?;

    // Register all parquet files as one table
    engine.register_parquet_files(&iceberg.name, files.clone()).await?;

    // Query all data
    let sql = &format!("SELECT * FROM {}", iceberg.name);
    let batches = engine.query(sql).await?;

    // Print query results to stdout
    if !batches.is_empty() {
        println!("Query results:");
        print_batches(&batches)?;
    } else {
        println!("Query returned no rows");
    }

    // Execute via worker/coordinator (distributed execution)
    use std::sync::Arc;
    use datafusion::prelude::*;
    
    let worker1 = Arc::new(Worker::new("worker-1"));
    let worker2 = Arc::new(Worker::new("worker-2"));
    let workers = vec![worker1, worker2];
    
    // Create a new context for coordinator (or share the engine's context)
    let coordinator_ctx = SessionContext::new();
    // Register the same table in coordinator's context
    if let Some(first) = files.first() {
        let path = std::path::Path::new(first);
        let dir = if path.is_dir() {
            path
        } else {
            path.parent().unwrap_or(path)
        };
        coordinator_ctx
            .register_parquet(&iceberg.name, dir.to_string_lossy().as_ref(), ParquetReadOptions::default())
            .await?;
    }
    
    let coordinator = Coordinator::new(workers, coordinator_ctx);
    
    // Execute query through coordinator (distributed)
    let distributed_batches: Vec<datafusion::arrow::record_batch::RecordBatch> = coordinator.execute_query(sql).await?;
    
    if !distributed_batches.is_empty() {
        println!("\nDistributed query results:");
        print_batches(&distributed_batches)?;
    }

    Ok(())
}
