### Parquet file explorer

Command line parquet file explorer. Built with Rust, Clap, Arrow and Dataflow.

### Build

$ cargo build --release

### Local testing through cargo

$ cargo run -- -f <file.parquet> -d

### Options
	- -d (--describe) show table structure
	- -f <filename> (--file <filename>) parquet file 
	- -q <query> (--query <query>)  "SELECT * FROM tablename", tablename is the filename w/o extension


