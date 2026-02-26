mod engine;
mod worker;
mod coordinator;

use engine::Engine;
use worker::Worker;
use coordinator::Coordinator;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let engine = Engine::new();

    engine
        .register_parquet("sales", "src/data/sales.parquet")
        .await?;

    let worker = Worker::new(engine);
    let coordinator = Coordinator::new(worker);

    coordinator
        .execute_query("SELECT * FROM sales LIMIT 5")
        .await;

    Ok(())
}
