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

    let tablename = path.file_stem().unwrap();

    let mut ctx = datafusion::prelude::ExecutionContext::new();
    ctx.register_parquet(&tablename.to_str().unwrap(), &opts.file).unwrap();
    println!("tablename: {:?}", tablename);

    if opts.describe {
        // describe_parquet(reader);
        println!("describe");
    }

    match opts.query {
        Some(q) => {
            match query_parquet(ctx, q.clone()).await {
                Ok(_a) => (),
                Err(e) => println!("Error running query {:?}: {:?}", q, e),
            }
        },
        None => println!("Empty query"),
    }
}

fn describe_parquet(reader: parquet::file::reader::SerializedFileReader<std::fs::File>) {
    let parquet_metadata = reader.metadata();
    let row_group_reader = reader.get_row_group(0).unwrap();

    println!("num_row_groups: {}", parquet_metadata.num_row_groups());
    println!("row_group_reader: {}", row_group_reader.num_columns());
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
