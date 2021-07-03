use linefeed::{Interface, ReadResult};
use std::iter::FromIterator;
use std::path::Path;

pub struct Repl {
    pub parquet_handler: crate::parquethandler::ParquetHandler,
    pub root_path: String,
}

impl Repl {
    pub async fn start_interactive_mode(&mut self) {
        println!("Interactive mode");
        let reader = Interface::new("parquet-explorer").unwrap();
        let path = Path::new(&self.root_path);
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
                "describe" => self.parquet_handler.clone().describe().await,
                "select" => self
                    .parquet_handler
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
    pub fn new(ph: crate::parquethandler::ParquetHandler) -> Self {
        let s = Self {
            root_path: ph.pm.root_path.clone(),
            parquet_handler: ph,
        };
        return s;
    }
}
