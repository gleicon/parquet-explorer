use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, path::Path};

fn main() {
    let path = Path::new("test_data/taxi_2019_04.parquet");
    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_metadata = reader.metadata();
        let row_group_reader = reader.get_row_group(0).unwrap();

        println!("num_row_groups: {}", parquet_metadata.num_row_groups());
        println!("row_group_reader: {}", row_group_reader.num_columns());
    }
}
