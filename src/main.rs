use clap::{AppSettings, Clap};
use linefeed::{Interface, ReadResult};
use std::iter::FromIterator;
use std::path::Path;

mod filemanager;
mod parquethandler;
mod repl;

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

    #[clap(short = 'n', long = "newfile")]
    newfile: bool,

    #[clap(short = 'o', long = "outfile")]
    outfile: Option<String>,

    #[clap(short = 's', long = "singlepartition")]
    singlepartition: bool,

    #[clap(short = 'i', long = "interactive")]
    interactive: bool,
}

#[tokio::main]
pub async fn main() {
    let opts: Opts = Opts::parse();
    let pm = filemanager::ParquetFileManager::new(opts.file.clone());
    let parquet_handler = parquethandler::ParquetHandler::new(pm);

    if opts.describe {
        parquet_handler.clone().describe().await;
    }

    if opts.interactive {
        println!("Interactive mode");
        let reader = Interface::new("parquet-explorer").unwrap();
        let path = Path::new(&opts.file);
        let tablename = path.file_stem().unwrap().to_str().unwrap();
        println!("data source: {}", tablename);

        let prompt = format!("table: {:?}> ", tablename);

        reader.set_prompt(&prompt).unwrap();

        while let ReadResult::Input(input) = reader.read_line().unwrap() {
            let res = Vec::from_iter(
                input
                    .to_lowercase()
                    .trim()
                    .split("something")
                    .map(String::from),
            );
            let cmd = &res[0];

            match cmd.as_str() {
                "describe" => parquet_handler.clone().describe().await,
                "select" => parquet_handler
                    .clone()
                    .query(input.clone().to_string())
                    .await
                    .unwrap(),
                "" => (),
                "quit" => return,
                "exit" => return,
                _ => println!("Invalid command {:?}", input),
            };
        }
        return;
    }
    match opts.query {
        Some(q) => {
            if !opts.newfile {
                parquet_handler.clone().query(q).await.unwrap();
            } else {
                parquet_handler
                    .clone()
                    .query_to_parquet(q.clone(), opts.outfile.unwrap(), opts.singlepartition)
                    .await
                    .unwrap();
            }
        }
        None => println!("Empty query"),
    }
}
