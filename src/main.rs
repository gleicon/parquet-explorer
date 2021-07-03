use clap::{AppSettings, Clap};

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
    let mut repl = repl::Repl::new(parquet_handler.clone());

    if opts.describe {
        parquet_handler.clone().describe().await;
    }

    if opts.interactive {
        repl.start_interactive_mode().await;
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
