use clap::{AppSettings, Clap};


mod filemanager;
mod parquethandler;

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
    let pm = filemanager::ParquetFileManager::new(opts.file.clone());
    let parquet_handler = parquethandler::ParquetHandler::new(pm);

    if opts.describe {
        parquet_handler.clone().describe().await;
    }

    match opts.query {
        Some(q) => {
            parquet_handler.clone().query(q).await.unwrap();
        },
        None => println!("Empty query"), 
    }
}