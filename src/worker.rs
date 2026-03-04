use datafusion::arrow::record_batch::RecordBatch;

pub struct Worker {
    pub id: String,
}

impl Worker {
    pub fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }

    pub async fn execute(&self, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
        println!("Worker {} processing {} batches", self.id, batches.len());
        batches
    }
}