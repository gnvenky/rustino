use crate::engine::Engine;

pub struct Worker {
    engine: Engine,
}

impl Worker {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }

    pub async fn run_query(&self, sql: &str) {
        let results = self.engine.execute_sql(sql).await.unwrap();

        for batch in results {
            println!("{:?}", batch);
        }
    }
}
