use std::{
    fs::{self, File},
    io::{BufReader, Write},
    path::Path,
};

use avro_rs::{
    types::{Record, Value},
    Codec, Reader, Schema, Writer,
};
use libtaos::Field;
use serde_json::{self, json, Map};
pub fn generate_avro_schema(data_types: &Vec<&str>) -> Schema {
    let mut raw_json_schema = Map::new();
    raw_json_schema.insert(
        "type".to_string(),
        serde_json::Value::String("record".to_string()),
    );
    raw_json_schema.insert(
        "name".to_string(),
        serde_json::Value::String("m1".to_string()),
    );
    let mut field_json_array: Vec<serde_json::Value> = vec![];
    for data_type in data_types {
        let column = json!({ "name": *data_type, "type": match *data_type {
            "tinyint" | "utinyint" | "smallint" | "usmallint" | "int" => "int",
            "uint" | "bigint" | "timestamp" | "ubigint" => "long",
            "bool" => "boolean",
            "float" => "float",
            "double" => "double",
            "binary" => "bytes",
            "nchar" => "string",
            _ => unreachable!("unexpected data type, please contact the author to fix!"),
        }});
        field_json_array.push(column);
    }
    raw_json_schema.insert(
        "fields".to_string(),
        serde_json::Value::Array(field_json_array),
    );

    Schema::parse_str(serde_json::to_string(&raw_json_schema).unwrap().as_str()).unwrap()
}

pub fn avro_serialize(data_types: &Vec<&str>, rows: &Vec<Vec<Field>>, compression: Codec) {
    let schema = generate_avro_schema(data_types);
    let mut writer = Writer::with_codec(&schema, Vec::new(), compression);
    for row in rows {
        let mut record = Record::new(writer.schema()).unwrap();
        let mut index = 0;
        for field in row {
            let field = (*field).clone();
            match field {
                libtaos::Field::Null => todo!(),
                libtaos::Field::Bool(v) => record.put(data_types[index], v),
                libtaos::Field::TinyInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::SmallInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::Int(v) => record.put(data_types[index], v),
                libtaos::Field::BigInt(v) => record.put(data_types[index], v),
                libtaos::Field::Float(v) => record.put(data_types[index], v),
                libtaos::Field::Double(v) => record.put(data_types[index], v),
                libtaos::Field::Binary(v) => record.put(data_types[index], (&*v).to_vec()),
                libtaos::Field::Timestamp(v) => {
                    record.put(data_types[index], (v).as_raw_timestamp())
                }
                libtaos::Field::NChar(v) => record.put(data_types[index], (v).clone()),
                libtaos::Field::UTinyInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::USmallInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::UInt(v) => record.put(data_types[index], v as i64),
                libtaos::Field::UBigInt(v) => record.put(data_types[index], v as i64),
            };
            index += 1;
        }
        writer.append(record).unwrap();
    }
}

pub fn avro_serialized_write(
    filename: &str,
    data_types: &Vec<&str>,
    rows: &Vec<Vec<Field>>,
    compression: Codec,
) {
    fs::remove_file(filename).unwrap();
    let path = Path::new(filename);
    let mut file = fs::File::create(&path).unwrap();
    let schema = generate_avro_schema(data_types);
    let mut writer = Writer::with_codec(&schema, Vec::new(), compression);
    for row in rows {
        let mut record = Record::new(writer.schema()).unwrap();
        let mut index = 0;
        for field in row {
            let field = (*field).clone();
            match field {
                libtaos::Field::Null => todo!(),
                libtaos::Field::Bool(v) => record.put(data_types[index], v),
                libtaos::Field::TinyInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::SmallInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::Int(v) => record.put(data_types[index], v),
                libtaos::Field::BigInt(v) => record.put(data_types[index], v),
                libtaos::Field::Float(v) => record.put(data_types[index], v),
                libtaos::Field::Double(v) => record.put(data_types[index], v),
                libtaos::Field::Binary(v) => record.put(data_types[index], (&*v).to_vec()),
                libtaos::Field::Timestamp(v) => {
                    record.put(data_types[index], (v).as_raw_timestamp())
                }
                libtaos::Field::NChar(v) => record.put(data_types[index], (v).clone()),
                libtaos::Field::UTinyInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::USmallInt(v) => record.put(data_types[index], v as i32),
                libtaos::Field::UInt(v) => record.put(data_types[index], v as i64),
                libtaos::Field::UBigInt(v) => record.put(data_types[index], v as i64),
            };
            index += 1;
        }
        writer.append(record).unwrap();
    }
    let input = writer.into_inner().unwrap();
    file.write(&input).unwrap();
}

pub fn avro_read(filename: &str) -> u32 {
    let mut count = 0;
    let f = File::open(filename).unwrap();
    let buffered_reader = BufReader::new(f);
    let r = Reader::new(buffered_reader);
    for x in r.unwrap() {
        count += 1;
        match x.unwrap() {
            Value::Record(r) => {
                for _ in r.into_iter() {
                    // println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }
    }
    count
}
