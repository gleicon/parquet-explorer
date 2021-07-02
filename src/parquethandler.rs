
use std::time::Instant;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;

#[derive(Clone)]
pub struct ParquetHandler {
    pub pm: crate::filemanager::ParquetFileManager,
    pub ctx: datafusion::prelude::ExecutionContext, // shortcut
}

impl ParquetHandler {
    pub async fn describe(&mut self){
        for (k, _v) in self.pm.files.iter() {
            let qq = format!("SHOW COLUMNS FROM {}", k.clone());
            self.clone().query(qq).await.unwrap()
        }
    }

    pub async fn query(&mut self, query: String) -> datafusion::error::Result<()> {
        let start = Instant::now();
    
        println!("query: {}", query);
        // cargo run -- -d -f test_data/taxi_2019_04.parquet -q "SELECT count(*) FROM parquet_tables"
        match self.ctx.sql(&query) {
            Ok(df) => {
                let results: Vec<RecordBatch> = df.collect().await.unwrap();
                print_batches(&results).unwrap();
                let duration = start.elapsed();
                println!("Time elapsed: {:?}\n{:?} rows", duration, results.len());
                return Ok(())
            },
            Err(e) => return Err(e),
        }
    }
    
    pub fn new(pm: crate::filemanager::ParquetFileManager) -> Self {
        let s = Self {
            pm: pm.clone(),
            ctx: pm.execution_context,
        };
        return s
    }
}
