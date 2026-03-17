use crate::worker::Worker;
use crate::task::{Task, Stage};
use crate::planner::QueryPlanner;
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use anyhow::Result;
use std::sync::Arc;

/// Coordinator for distributed query execution (similar to Trino's Coordinator)
pub struct Coordinator {
    workers: Vec<Arc<Worker>>,
    planner: Arc<QueryPlanner>,
    ctx: Arc<SessionContext>,
}

impl Coordinator {
    pub fn new(workers: Vec<Arc<Worker>>, ctx: SessionContext) -> Self {
        let planner = Arc::new(QueryPlanner::new(ctx.clone()));
        Self {
            workers,
            planner,
            ctx: Arc::new(ctx),
        }
    }

    /// Execute a query with distributed execution
    pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        println!("Coordinator: Planning query...");
        
        // Plan the query
        let plan = self.planner.plan(sql).await?;
        
        // Create stages for distributed execution
        let stages = self.create_stages(&plan)?;
        
        println!("Coordinator: Created {} stages", stages.len());
        
        // Execute stages in order (respecting dependencies)
        let mut results = Vec::new();
        for stage in stages {
            println!("Coordinator: Executing stage {}", stage.id);
            let stage_results = self.execute_stage(stage).await?;
            results.extend(stage_results);
        }
        
        Ok(results)
    }

    /// Create execution stages from logical plan
    fn create_stages(&self, plan: &datafusion::logical_expr::LogicalPlan) -> Result<Vec<Stage>> {
        // In a full implementation, this would analyze the plan and create
        // stages with proper dependencies. For now, create a single stage.
        let mut stage = Stage::new("stage-0");
        
        // Create tasks for this stage
        // In distributed execution, tasks would be split across workers
        let task = Task::new("query-1", "stage-0", &format!("{:?}", plan));
        stage.add_task(task);
        
        Ok(vec![stage])
    }

    /// Execute a stage by distributing tasks to workers
    async fn execute_stage(&self, stage: Stage) -> Result<Vec<RecordBatch>> {
        let mut all_results = Vec::new();
        
        // Distribute tasks to workers
        for (idx, task) in stage.tasks.iter().enumerate() {
            let worker = &self.workers[idx % self.workers.len()];
            println!("Coordinator: Assigning task {} to worker {}", task.id, worker.id);
            
            // Execute task on worker
            let results = worker.execute_task(task.clone()).await?;
            all_results.extend(results);
        }
        
        Ok(all_results)
    }
}
