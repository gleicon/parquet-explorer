use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use clap::{AppSettings, Clap};
use std::time::Instant;

use datafusion;
use parquet::file::reader::{FileReader, SerializedFileReader};

use std::{fs::File, path::Path};

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

    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file).unwrap();

        if opts.describe {
            describe_parquet(reader);
        }
    }

    match opts.query {
        Some(q) => query_parquet(opts.file, q).await.unwrap(),
        None => println!("Empty query"),
    }
}

fn describe_parquet(reader: parquet::file::reader::SerializedFileReader<std::fs::File>) {
    let parquet_metadata = reader.metadata();
    let row_group_reader = reader.get_row_group(0).unwrap();

    println!("num_row_groups: {}", parquet_metadata.num_row_groups());
    println!("row_group_reader: {}", row_group_reader.num_columns());
}

async fn query_parquet(path: String, query: String) -> datafusion::error::Result<()> {
    let start = Instant::now();

    println!("query: {}", query);
    // cargo run -- -d -f test_data/taxi_2019_04.parquet -q "SELECT count(*) FROM parquet_tables"
    let mut ctx = datafusion::prelude::ExecutionContext::new();

    ctx.register_parquet("parquet_tables", &path).unwrap();

    // create a plan to run a SQL query
    let df = ctx.sql(&query).unwrap();
    let results: Vec<RecordBatch> = df.collect().await.unwrap();
    print_batches(&results).unwrap();
    let duration = start.elapsed();
    println!("Time elapsed: {:?}\n{:?} rows", duration, results.len());
    Ok(())
}
