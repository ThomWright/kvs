use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kvs::{EngineType, KvStore, KvsEngine, SledKvsEngine};
use rand;
use rand::distributions::Standard;
use rand::Rng;
use tempfile::TempDir;

fn write(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");

    for engine_type in &[EngineType::Kvs, EngineType::Sled] {
        group.bench_with_input(
            BenchmarkId::from_parameter(engine_type),
            engine_type,
            |b, &engine_type| {
                b.iter_batched(
                    || {
                        let temp_dir =
                            TempDir::new().expect("unable to create temporary working directory");
                        let store: Box<dyn KvsEngine> = match engine_type {
                            EngineType::Kvs => Box::new(
                                KvStore::open(temp_dir.path()).expect("unable to open KvStore"),
                            ),
                            EngineType::Sled => Box::new(
                                SledKvsEngine::open(temp_dir.path())
                                    .expect("unable to open SledKvsEngine"),
                            ),
                        };

                        let key = gen_random_string();
                        let value = gen_random_string();
                        (store, key.clone(), value.clone())
                    },
                    |(mut store, key, value)| {
                        store.set(key, value).unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");

    for engine_type in &[EngineType::Kvs, EngineType::Sled] {
        group.bench_with_input(
            BenchmarkId::from_parameter(engine_type),
            engine_type,
            |b, &engine_type| {
                b.iter_batched(
                    || {
                        let temp_dir =
                            TempDir::new().expect("unable to create temporary working directory");
                        let mut store: Box<dyn KvsEngine> = match engine_type {
                            EngineType::Kvs => Box::new(
                                KvStore::open(temp_dir.path()).expect("unable to open KvStore"),
                            ),
                            EngineType::Sled => Box::new(
                                SledKvsEngine::open(temp_dir.path())
                                    .expect("unable to open SledKvsEngine"),
                            ),
                        };

                        let key = gen_random_string();
                        let value = gen_random_string();

                        store.set(key.clone(), value).unwrap();

                        (store, key.clone())
                    },
                    |(mut store, key)| {
                        store.get(key).unwrap();
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn gen_random_string() -> String {
    let mut rng = rand::thread_rng();
    let length = rng.gen_range(1, 100_001);

    rng.sample_iter::<char, _>(&Standard)
        .take(length)
        .collect::<String>()
}

criterion_group!(benches, write, read);
criterion_main!(benches);
