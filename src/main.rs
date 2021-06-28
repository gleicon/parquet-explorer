use clap::{AppSettings, Clap};
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

fn describe_parquet(reader: parquet::file::reader::SerializedFileReader<std::fs::File>) {
    let parquet_metadata = reader.metadata();
    let row_group_reader = reader.get_row_group(0).unwrap();
    println!("num_row_groups: {}", parquet_metadata.num_row_groups());
    println!("row_group_reader: {}", row_group_reader.num_columns());
}

fn main() {
    let opts: Opts = Opts::parse();
    let path = Path::new(&opts.file);

    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file).unwrap();
        if opts.describe {
            describe_parquet(reader);
        }
    }

    match opts.query {
        Some(q) => println!("query: {}", q),
        None => println!("Empty query"),
    }
}
