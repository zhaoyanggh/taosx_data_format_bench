use bstr::BString;
use libtaos::{Field, Timestamp, TimestampPrecision};
use rand::{distributions::Alphanumeric, thread_rng, Rng};

pub mod avro;
pub mod my_parquet;

pub fn generate_data(data_types: &Vec<&str>, size: u32) -> Vec<Vec<Field>> {
    let mut rows: Vec<Vec<Field>> = vec![];
    for _ in 0..size {
        let mut row = vec![];
        for data_type in data_types {
            match *data_type {
                "tinyint" => row.push(Field::TinyInt(rand::thread_rng().gen())),
                "utinyint" => row.push(Field::UTinyInt(rand::thread_rng().gen())),
                "smallint" => row.push(Field::SmallInt(rand::thread_rng().gen())),
                "usmallint" => row.push(Field::USmallInt(rand::thread_rng().gen())),
                "int" => row.push(Field::Int(rand::thread_rng().gen())),
                "uint" => row.push(Field::UInt(rand::thread_rng().gen())),
                "bigint" => row.push(Field::BigInt(rand::thread_rng().gen())),
                "ubigint" => row.push(Field::UBigInt(rand::thread_rng().gen())),
                "float" => row.push(Field::Float(rand::thread_rng().gen())),
                "double" => row.push(Field::Double(rand::thread_rng().gen())),
                "timestamp" => {
                    row.push(Field::Timestamp(Timestamp::new(
                        rand::thread_rng().gen(),
                        TimestampPrecision::Milli,
                    )));
                }
                "bool" => {
                    row.push(Field::Bool(rand::thread_rng().gen()));
                }
                "binary" => {
                    let rand_string: String = thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .map(char::from)
                        .collect::<String>();
                    row.push(Field::Binary(BString::from(rand_string)));
                }
                "nchar" => {
                    let rand_string: String = thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .map(char::from)
                        .collect::<String>();
                    row.push(Field::NChar(rand_string));
                }
                _ => panic!("unknown data type"),
            }
        }
        rows.push(row);
    }
    rows
}
