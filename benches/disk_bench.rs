use avro_rs::Codec;
use criterion::{
    criterion_group, criterion_main,
    measurement::{Measurement, ValueFormatter},
    BenchmarkId, Criterion, Throughput,
};
use filesize::PathExt;
use std::path::Path;
use taosx_data_format_bench::{
    avro::{avro_read, avro_serialized_write},
    generate_data,
    my_parquet::{parquet_read, parquet_serialized_write},
};

extern crate lazy_static;
lazy_static::lazy_static! {
    pub static ref DATATYPES: Vec<&'static str> = vec![
        "timestamp",
        "tinyint",
        "utinyint",
        "smallint",
        "usmallint",
        "int",
        "uint",
        "bigint",
        "ubigint",
        "float",
        "double",
        "bool",
        "binary",
        "nchar",
    ];
}

pub const DATASIZE: u32 = 10000;

fn get_file_size(filename: &str) -> u64 {
    let path = Path::new(filename);
    // let metadata = match path.symlink_metadata() {
    //     Ok(v) => v,
    //     Err(_) => return 0,
    // };
    let metadata = path.symlink_metadata().unwrap();
    path.size_on_disk_fast(&metadata).unwrap()
}

pub struct Compressability;
impl Measurement for Compressability {
    type Intermediate = u64;

    type Value = u64;

    fn start(&self) -> Self::Intermediate {
        0
    }

    fn end(&self, _: Self::Intermediate) -> Self::Value {
        get_file_size("sample")
    }

    fn add(&self, v1: &Self::Value, v2: &Self::Value) -> Self::Value {
        v1 + v2
    }

    fn zero(&self) -> Self::Value {
        0
    }

    fn to_f64(&self, value: &Self::Value) -> f64 {
        *value as f64
    }

    fn formatter(&self) -> &dyn criterion::measurement::ValueFormatter {
        &CompressabilityFormatter
    }
}

struct CompressabilityFormatter;
impl ValueFormatter for CompressabilityFormatter {
    fn format_value(&self, value: f64) -> String {
        format!("{:.4} bytes", value)
    }

    fn format_throughput(&self, throughput: &criterion::Throughput, value: f64) -> String {
        match throughput {
            criterion::Throughput::Bytes(b) => format!("{:.4} bpb", value / *b as f64),
            criterion::Throughput::Elements(b) => format!("{:.4} bytes/{}", value, b),
        }
    }

    fn scale_values(&self, _: f64, _: &mut [f64]) -> &'static str {
        "bytes"
    }

    fn scale_throughputs(
        &self,
        _: f64,
        throughput: &criterion::Throughput,
        values: &mut [f64],
    ) -> &'static str {
        match throughput {
            Throughput::Bytes(n) => {
                for val in values {
                    *val /= *n as f64;
                }
                "bpb"
            }
            Throughput::Elements(n) => {
                for val in values {
                    *val /= *n as f64;
                }
                "b/e"
            }
        }
    }

    fn scale_for_machines(&self, _: &mut [f64]) -> &'static str {
        "bytes"
    }
}

pub fn bench_write(c: &mut Criterion<Compressability>) {
    let mut group = c.benchmark_group("Persistence");
    group.sample_size(10);
    let mut i = 1;
    let mut step = 1;
    while i <= DATASIZE {
        let (rows, cols) = generate_data(&DATATYPES, i);
        group.bench_with_input(BenchmarkId::new("Parquet-SNAPPY", i), &i, |b, _| {
            b.iter(|| {
                parquet_serialized_write(
                    "sample",
                    &DATATYPES,
                    &cols,
                    parquet::basic::Compression::SNAPPY,
                );
                assert_eq!(parquet_read("sample"), i);
            })
        });

        group.bench_with_input(BenchmarkId::new("Parquet-BROTLI", i), &i, |b, _| {
            b.iter(|| {
                parquet_serialized_write(
                    "sample",
                    &DATATYPES,
                    &cols,
                    parquet::basic::Compression::BROTLI,
                );
                assert_eq!(parquet_read("sample"), i);
            })
        });

        group.bench_with_input(BenchmarkId::new("Parquet-GZIP", i), &i, |b, _| {
            b.iter(|| {
                parquet_serialized_write(
                    "sample",
                    &DATATYPES,
                    &cols,
                    parquet::basic::Compression::GZIP,
                );
                assert_eq!(parquet_read("sample"), i);
            })
        });

        group.bench_with_input(BenchmarkId::new("Parquet-LZ4", i), &i, |b, _| {
            b.iter(|| {
                parquet_serialized_write(
                    "sample",
                    &DATATYPES,
                    &cols,
                    parquet::basic::Compression::LZ4,
                );
                assert_eq!(parquet_read("sample"), i);
            })
        });

        // The codec type LZO is not supported yet

        // group.bench_with_input(BenchmarkId::new("Parquet-LZO", i), &i, |b, _| {
        //     b.iter(|| {
        //         parquet_serialized_write(
        //             "sample",
        //             &DATATYPES,
        //             &cols,
        //             parquet::basic::Compression::LZO,
        //         );
        //         assert_eq!(parquet_read("sample"), i);
        //     })
        // });

        group.bench_with_input(BenchmarkId::new("Parquet-ZSTD", i), &i, |b, _| {
            b.iter(|| {
                parquet_serialized_write(
                    "sample",
                    &DATATYPES,
                    &cols,
                    parquet::basic::Compression::ZSTD,
                );
                assert_eq!(parquet_read("sample"), i);
            })
        });

        group.bench_with_input(BenchmarkId::new("Parquet-UNCOMPRESSED", i), &i, |b, _| {
            b.iter(|| {
                parquet_serialized_write(
                    "sample",
                    &DATATYPES,
                    &cols,
                    parquet::basic::Compression::UNCOMPRESSED,
                );
                assert_eq!(parquet_read("sample"), i);
            })
        });

        group.bench_with_input(BenchmarkId::new("Avro-Deflate", i), &i, |b, _| {
            b.iter(|| {
                avro_serialized_write("sample", &DATATYPES, &rows, Codec::Deflate);
                assert_eq!(avro_read("sample"), i);
            })
        });

        group.bench_with_input(BenchmarkId::new("Avro-Null", i), &i, |b, _| {
            b.iter(|| {
                avro_serialized_write("sample", &DATATYPES, &rows, Codec::Null);
                assert_eq!(avro_read("sample"), i);
            })
        });
        i = step * 1000;
        step += 1;
    }
    group.finish();
}

criterion_group! {
    name = bench_compressability;
    config = Criterion::default().with_measurement(Compressability);
    targets = bench_write
}
criterion_main!(bench_compressability);
