use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use clap::{AppSettings, Clap};
use std::time::Instant;

use datafusion;
use parquet::file::reader::{FileReader};

use std::path::Path;

#[derive(Clap)]
#[clap(version = "0.0.1", author = "gleicon <gleicon@gmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(short = 'f', long = "file")]
    file: String,

    #[clap(short = 'd', long = "describe")]
    describe: bool,

    #[clap(short = 'q', long = "query")]
    query: Option<String>,
}

#[tokio::main]
pub async fn main() {
    let opts: Opts = Opts::parse();
    let path = Path::new(&opts.file);

    let tablename = path.file_stem().unwrap().to_str().unwrap();

    let execution_config = datafusion::prelude::ExecutionConfig::new().with_information_schema(true);

    //let mut ctx = datafusion::prelude::ExecutionContext::new();
    let mut ctx = datafusion::prelude::ExecutionContext::with_config(execution_config);

    ctx.register_parquet(&tablename.clone(), &opts.file).unwrap();
    println!("tablename: {:?}", tablename);

    if opts.describe {
        let qq = format!("SHOW COLUMNS FROM {}", tablename);
        query_parquet(ctx.clone(), qq).await.unwrap()
    }

    match opts.query {
        Some(q) => {
            match query_parquet(ctx.clone(), q.clone()).await {
                Ok(_a) => (),
                Err(e) => println!("Error running query {:?}: {:?}", q, e),
            }
        },
        None => println!("Empty query"),
    }
}


async fn query_parquet(mut ctx: datafusion::prelude::ExecutionContext, query: String) -> datafusion::error::Result<()> {
    let start = Instant::now();

    println!("query: {}", query);
    // cargo run -- -d -f test_data/taxi_2019_04.parquet -q "SELECT count(*) FROM parquet_tables"
    match ctx.sql(&query) {
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
