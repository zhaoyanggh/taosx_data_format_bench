use libtaos::Field;
use parquet::{
    basic::{
        Compression, ConvertedType, LogicalType, Repetition, TimeUnit, TimestampType,
        Type as PhysicalType,
    },
    column::writer::ColumnWriter,
    file::{
        properties::WriterProperties,
        serialized_reader::SerializedFileReader,
        writer::{FileWriter, InMemoryWriteableCursor, SerializedFileWriter},
    },
    schema::types::Type,
};
use std::{
    fs::{self},
    path::Path,
    sync::Arc,
    vec,
};

pub fn generate_parquet_schema(data_types: &Vec<&str>) -> Arc<Type> {
    let mut fields = vec![];

    for data_type in data_types {
        match *data_type {
            "tinyint" => fields.push(Arc::new(
                Type::primitive_type_builder("tinyint", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_8)
                    .build()
                    .unwrap(),
            )),
            "utinyint" => fields.push(Arc::new(
                Type::primitive_type_builder("utinyint", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_8)
                    .build()
                    .unwrap(),
            )),
            "smallint" => fields.push(Arc::new(
                Type::primitive_type_builder("smallint", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::INT_16)
                    .build()
                    .unwrap(),
            )),
            "usmallint" => fields.push(Arc::new(
                Type::primitive_type_builder("usmallint", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_16)
                    .build()
                    .unwrap(),
            )),
            "int" => fields.push(Arc::new(
                Type::primitive_type_builder("int", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "uint" => fields.push(Arc::new(
                Type::primitive_type_builder("uint", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_32)
                    .build()
                    .unwrap(),
            )),
            "bigint" => fields.push(Arc::new(
                Type::primitive_type_builder("bigint", PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "timestamp" => fields.push(Arc::new(
                Type::primitive_type_builder("timestamp", PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::TIMESTAMP(TimestampType {
                        is_adjusted_to_u_t_c: false,
                        unit: TimeUnit::MILLIS(Default::default()),
                    })))
                    .build()
                    .unwrap(),
            )),
            "ubigint" => fields.push(Arc::new(
                Type::primitive_type_builder("ubigint", PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ConvertedType::UINT_64)
                    .build()
                    .unwrap(),
            )),
            "float" => fields.push(Arc::new(
                Type::primitive_type_builder("float", PhysicalType::FLOAT)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "double" => fields.push(Arc::new(
                Type::primitive_type_builder("double", PhysicalType::DOUBLE)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            "binary" => fields.push(Arc::new(
                Type::primitive_type_builder("binary", PhysicalType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_id(8)
                    .build()
                    .unwrap(),
            )),
            "nchar" => fields.push(Arc::new(
                Type::primitive_type_builder("nchar", PhysicalType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_logical_type(Some(LogicalType::STRING(Default::default())))
                    .with_id(8)
                    .build()
                    .unwrap(),
            )),
            "bool" => fields.push(Arc::new(
                Type::primitive_type_builder("bool", PhysicalType::BOOLEAN)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )),
            _ => unreachable!("unexpected data type, please contact the author to fix!"),
        }
    }

    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(&mut fields)
            .build()
            .unwrap(),
    )
}

pub fn parquet_serialize(data_types: &Vec<&str>, cols: &Vec<Vec<Field>>, compression: Compression) {
    let cursor = InMemoryWriteableCursor::default();
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(compression)
            .build(),
    );
    let schema = generate_parquet_schema(data_types);
    let mut writer = SerializedFileWriter::new(cursor, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    for col in cols {
        let data_writer = row_group_writer.next_column().unwrap();
        if let Some(mut writer) = data_writer {
            let mut def_level = vec![];
            match writer {
                ColumnWriter::BoolColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        values.push(*field.to_owned().as_bool().unwrap());
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        match *field {
                            Field::TinyInt(v) => {
                                values.push(v as i32);
                            }
                            Field::SmallInt(v) => {
                                values.push(v as i32);
                            }
                            Field::Int(v) => {
                                values.push(v as i32);
                            }
                            Field::UTinyInt(v) => {
                                values.push(v as i32);
                            }
                            Field::USmallInt(v) => {
                                values.push(v as i32);
                            }
                            Field::UInt(v) => {
                                values.push(v as i32);
                            }
                            _ => unreachable!(),
                        }
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        let field = (*field).clone();
                        match field {
                            Field::BigInt(v) => values.push(v),
                            Field::Timestamp(v) => values.push(v.as_raw_timestamp()),
                            Field::UBigInt(v) => values.push(v as i64),
                            _ => unreachable!(),
                        }
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::FloatColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        values.push(*field.as_float().unwrap());
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        values.push(*field.as_double().unwrap())
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        let field = (*field).clone();
                        match field {
                            Field::Binary(v) => {
                                values.push(parquet::data_type::ByteArray::from(v.to_vec()))
                            }
                            Field::NChar(v) => {
                                values.push(parquet::data_type::ByteArray::from(v.as_str()))
                            }
                            _ => unreachable!(),
                        }
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                _ => unreachable!(),
            }
            row_group_writer.close_column(writer).unwrap();
        }
    }
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}

pub fn parquet_serialized_write(
    filename: &str,
    data_types: &Vec<&str>,
    cols: &Vec<Vec<Field>>,
    compression: Compression,
) {
    fs::remove_file(filename).unwrap();
    let path = Path::new(filename);
    let file = fs::File::create(&path).unwrap();
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(compression)
            .build(),
    );
    let schema = generate_parquet_schema(data_types);
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    for col in cols {
        let data_writer = row_group_writer.next_column().unwrap();
        if let Some(mut writer) = data_writer {
            let mut def_level = vec![];
            match writer {
                ColumnWriter::BoolColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        values.push(*field.to_owned().as_bool().unwrap());
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        match *field {
                            Field::TinyInt(v) => {
                                values.push(v as i32);
                            }
                            Field::SmallInt(v) => {
                                values.push(v as i32);
                            }
                            Field::Int(v) => {
                                values.push(v as i32);
                            }
                            Field::UTinyInt(v) => {
                                values.push(v as i32);
                            }
                            Field::USmallInt(v) => {
                                values.push(v as i32);
                            }
                            Field::UInt(v) => {
                                values.push(v as i32);
                            }
                            _ => unreachable!(),
                        }
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        let field = (*field).clone();
                        match field {
                            Field::BigInt(v) => values.push(v),
                            Field::Timestamp(v) => values.push(v.as_raw_timestamp()),
                            Field::UBigInt(v) => values.push(v as i64),
                            _ => unreachable!(),
                        }
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::FloatColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        values.push(*field.as_float().unwrap());
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        values.push(*field.as_double().unwrap())
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    let mut values = vec![];
                    for field in col {
                        def_level.push(1);
                        let field = (*field).clone();
                        match field {
                            Field::Binary(v) => {
                                values.push(parquet::data_type::ByteArray::from(v.to_vec()))
                            }
                            Field::NChar(v) => {
                                values.push(parquet::data_type::ByteArray::from(v.as_str()))
                            }
                            _ => unreachable!(),
                        }
                    }
                    typed
                        .write_batch(&values[..], Some(&def_level), None)
                        .unwrap();
                }
                _ => unreachable!(),
            }
            row_group_writer.close_column(writer).unwrap();
        }
    }
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}

pub fn parquet_read(filename: &str) -> u32 {
    let mut count = 0;
    let parquet_reader = SerializedFileReader::try_from(filename).unwrap();
    for _ in parquet_reader {
        count += 1;
        // println!("{:?}", row);
    }
    count
}
