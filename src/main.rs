mod engine;
mod worker;
mod iceberg;
mod coordinator;

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

    // Execute via worker/coordinator
    let worker = Worker::new("worker-1");
    let coordinator = Coordinator::new(worker);

    coordinator.execute_query(batches).await;

    Ok(())
}