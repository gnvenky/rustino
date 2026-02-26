use crate::worker::Worker;

pub struct Coordinator {
    worker: Worker,
}

impl Coordinator {
    pub fn new(worker: Worker) -> Self {
        Self { worker }
    }

    pub async fn execute_query(&self, sql: &str) {
        self.worker.run_query(sql).await;
    }
}
