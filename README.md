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
	- -n (--newfile) signals that a new file will be create, requires -o
	- -o <outputfile> (--output <outputfile>) goes along with -n to dump the query result to a new parquet file
	- -s (--singlefile) tells parquet-explorer to write a single parquet file instead of a partitioned parquet (default behaviour)

#### SQL Dialect
Datafusion provides a PostgreSQL compatible SQL dialect. The tablename will follow the parquet filename (if the parquet is called original_parquet you should query against that). The information schema is enabled so "SHOW COLUMNS FROM original_parquet" will print columns from the ```original_parquet``` file.

#### Query a parquet file using SQL
```parquet-explorer -f original_parquet -q "SELECT * from original_parquet LIMIT 10"``` will get the top 10 rows from original_parquet


#### Filtering parquet files using SQL
```parquet-explorer -f original_parquet -q "SELECT * from original_parquet LIMIT 10" -n -o destination_parquet``` will query original_parquet, take the top 10 lines and create a new parquet file with them.




