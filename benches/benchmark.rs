use avro_rs::Codec;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use taosx_data_format_bench::{
    avro::{avro_read, avro_serialize, avro_serialized_write},
    generate_data,
    my_parquet::{parquet_read, parquet_serialize, parquet_serialized_write},
};

extern crate lazy_static;
lazy_static::lazy_static! {
    static ref DATATYPES: Vec<&'static str> = vec![
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

const DATASIZE: u32 = 10000;

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

pub fn write_benchmark(c: &mut Criterion) {
    let (rows, cols) = generate_data(&DATATYPES, DATASIZE);
    c.bench_function("parquet write", |b| {
        b.iter(|| {
            parquet_serialized_write(
                "sample.parquet",
                &DATATYPES,
                &rows,
                parquet::basic::Compression::SNAPPY,
            )
        })
    });
    parquet_read("sample.parquet");
    c.bench_function("avro write", |b| {
        b.iter(|| avro_serialized_write("sample.avro", &DATATYPES, &rows, Codec::Null))
    });
    avro_read("sample.avro");
}

// criterion_group!(benches, serialize_benchmark, write_benchmark);
criterion_group!(benches, bench_serialize);
criterion_main!(benches);
