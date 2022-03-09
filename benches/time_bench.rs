use avro_rs::Codec;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use taosx_data_format_bench::{
    avro::{avro_read, avro_serialize, avro_serialized_write},
    generate_data,
    my_parquet::{parquet_read, parquet_serialize, parquet_serialized_write},
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

pub fn bench_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("Serialize");
    let mut i = 1;
    let step = 10;
    while i <= DATASIZE {
        let (rows, cols) = generate_data(&DATATYPES, i);
        group.bench_with_input(BenchmarkId::new("Parquet", i), &i, |b, _| {
            b.iter(|| parquet_serialize(&DATATYPES, &cols, parquet::basic::Compression::SNAPPY))
        });
        group.bench_with_input(BenchmarkId::new("Avro", i), &i, |b, _| {
            b.iter(|| avro_serialize(&DATATYPES, &rows, Codec::Deflate))
        });
        i *= step;
    }
    group.finish();
}

pub fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("Write");
    let mut i = 1;
    let step = 10;
    while i <= DATASIZE {
        let (rows, cols) = generate_data(&DATATYPES, i);
        group.bench_with_input(BenchmarkId::new("Parquet", i), &i, |b, _| {
            b.iter(|| {
                parquet_serialized_write(
                    "sample.parquet",
                    &DATATYPES,
                    &cols,
                    parquet::basic::Compression::SNAPPY,
                );
            })
        });
        parquet_read("sample.parquet");
        group.bench_with_input(BenchmarkId::new("Avro", i), &i, |b, _| {
            b.iter(|| avro_serialized_write("sample.avro", &DATATYPES, &rows, Codec::Deflate))
        });
        avro_read("sample.avro");
        i *= step;
    }
    group.finish();
}

criterion_group!(benches, bench_write);
criterion_main!(benches);
