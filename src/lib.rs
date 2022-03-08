use bstr::BString;
use libtaos::{Field, Timestamp, TimestampPrecision};
use rand::{distributions::Alphanumeric, thread_rng, Rng};

pub mod avro;
pub mod my_parquet;

pub fn generate_data(data_types: &Vec<&str>, size: u32) -> (Vec<Vec<Field>>, Vec<Vec<Field>>) {
    let mut rows: Vec<Vec<Field>> = vec![];
    let mut cols: Vec<Vec<Field>> = vec![];
    for _ in 0..data_types.len() {
        let col = vec![];
        cols.push(col);
    }
    for _ in 0..size {
        let mut row = vec![];
        let mut index = 0;
        for data_type in data_types {
            match *data_type {
                "tinyint" => {
                    let tinyint = rand::thread_rng().gen();
                    row.push(Field::TinyInt(tinyint));
                    cols[index].push(Field::TinyInt(tinyint));
                }
                "utinyint" => {
                    let utinyint = rand::thread_rng().gen();
                    row.push(Field::UTinyInt(utinyint));
                    cols[index].push(Field::UTinyInt(utinyint));
                }
                "smallint" => {
                    let smallint = rand::thread_rng().gen();
                    row.push(Field::SmallInt(smallint));
                    cols[index].push(Field::SmallInt(smallint));
                }
                "usmallint" => {
                    let usmallint = rand::thread_rng().gen();
                    row.push(Field::USmallInt(usmallint));
                    cols[index].push(Field::USmallInt(usmallint));
                }
                "int" => {
                    let int = rand::thread_rng().gen();
                    row.push(Field::Int(int));
                    cols[index].push(Field::Int(int));
                }
                "uint" => {
                    let uint = rand::thread_rng().gen();
                    row.push(Field::UInt(uint));
                    cols[index].push(Field::UInt(uint));
                }
                "bigint" => {
                    let bigint = rand::thread_rng().gen();
                    row.push(Field::BigInt(bigint));
                    cols[index].push(Field::BigInt(bigint));
                }
                "ubigint" => {
                    let ubigint = rand::thread_rng().gen();
                    row.push(Field::UBigInt(ubigint));
                    cols[index].push(Field::UBigInt(ubigint));
                }
                "float" => {
                    let float = rand::thread_rng().gen();
                    row.push(Field::Float(float));
                    cols[index].push(Field::Float(float));
                }
                "double" => {
                    let double = rand::thread_rng().gen();
                    row.push(Field::Double(double));
                    cols[index].push(Field::Double(double));
                }
                "timestamp" => {
                    let timestamp = Field::Timestamp(Timestamp::new(
                        rand::thread_rng().gen(),
                        TimestampPrecision::Milli,
                    ));
                    row.push(timestamp.clone());
                    cols[index].push(timestamp.clone());
                }
                "bool" => {
                    let bool = rand::thread_rng().gen();
                    row.push(Field::Bool(bool));
                    cols[index].push(Field::Bool(bool));
                }
                "binary" => {
                    let rand_string: String = thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .map(char::from)
                        .collect::<String>();
                    row.push(Field::Binary(BString::from(rand_string.clone())));
                    cols[index].push(Field::Binary(BString::from(rand_string.clone())));
                }
                "nchar" => {
                    let rand_string: String = thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .map(char::from)
                        .collect::<String>();
                    row.push(Field::NChar(rand_string.clone()));
                    cols[index].push(Field::NChar(rand_string.clone()));
                }
                _ => panic!("unknown data type"),
            }
            index += 1;
        }
        rows.push(row);
    }
    (rows, cols)
}
