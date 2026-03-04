use crate::worker::Worker;
use datafusion::arrow::record_batch::RecordBatch;

pub struct Coordinator {
    worker: Worker,
}

impl Coordinator {
    pub fn new(worker: Worker) -> Self {
        Self { worker }
    }

    pub async fn execute_query(&self, batches: Vec<RecordBatch>) {
        self.worker.execute(batches).await;
    }
}