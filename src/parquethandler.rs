use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use std::fs;
use std::time::Instant;

#[derive(Clone)]
pub struct ParquetHandler {
    pub pm: crate::filemanager::ParquetFileManager,
    pub ctx: datafusion::prelude::ExecutionContext, // shortcut
}

impl ParquetHandler {
    pub async fn describe(&mut self) {
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
                return Ok(());
            }
            Err(e) => return Err(e),
        }
    }

    pub async fn query_to_parquet(
        &mut self,
        query: String,
        filepath: String,
        singlepartition: bool,
    ) -> datafusion::error::Result<()> {
        if singlepartition {
            self.write_single_file_parquet(query, filepath, None).await
        } else {
            self.write_to_parquet(query, filepath).await
        }
    }

    pub fn new(pm: crate::filemanager::ParquetFileManager) -> Self {
        let s = Self {
            pm: pm.clone(),
            ctx: pm.execution_context,
        };
        return s;
    }

    pub async fn write_to_parquet(
        &mut self,
        query: String,
        filepath: String,
    ) -> datafusion::error::Result<()> {
        let logical_plan = self.ctx.create_logical_plan(&query)?;
        let logical_plan = self.ctx.optimize(&logical_plan)?;
        let physical_plan = self.ctx.create_physical_plan(&logical_plan)?;
        self.ctx.write_parquet(physical_plan, filepath, None).await
    }

    pub async fn write_single_file_parquet(
        &self,
        query: String,
        filepath: String,
        writer_properties: Option<WriterProperties>,
    ) -> datafusion::error::Result<()> {
        let logical_plan = self.ctx.create_logical_plan(&query)?;
        let logical_plan = self.ctx.optimize(&logical_plan)?;
        let physical_plan = self.ctx.create_physical_plan(&logical_plan)?;
        // create directory to contain the Parquet files (one per partition)
        let file = fs::File::create(filepath)?;
        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            physical_plan.schema(),
            writer_properties.clone(),
        )?;
        // writes all partitions to the same file
        for i in 0..physical_plan.output_partitioning().partition_count() {
            let stream = physical_plan.execute(i).await?;
            stream
                .map(|batch| writer.write(&batch?))
                .try_collect()
                .await
                .map_err(datafusion::error::DataFusionError::from)?;
        }
        writer
            .close()
            .map_err(datafusion::error::DataFusionError::from)
            .map(|_| ())
    }
}
