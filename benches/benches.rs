use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kvs::{EngineType, KvStore, KvsEngine, SledKvsEngine};
use rand;
use rand::distributions::Standard;
use rand::Rng;
use tempfile::TempDir;

enum Engine {
    Kvs(KvStore),
    Sled(SledKvsEngine),
}

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
                        let store: Engine = match engine_type {
                            EngineType::Kvs => Engine::Kvs(
                                KvStore::open(temp_dir.path()).expect("unable to open KvStore"),
                            ),
                            EngineType::Sled => Engine::Sled(
                                SledKvsEngine::open(temp_dir.path())
                                    .expect("unable to open SledKvsEngine"),
                            ),
                        };

                        let key = gen_random_string();
                        let value = gen_random_string();
                        (store, key.clone(), value.clone())
                    },
                    |(store, key, value)| {
                        match store {
                            Engine::Kvs(ref s) => s.set(key, value).unwrap(),
                            Engine::Sled(ref s) => s.set(key, value).unwrap(),
                        };
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
                        let store: Engine = match engine_type {
                            EngineType::Kvs => Engine::Kvs(
                                KvStore::open(temp_dir.path()).expect("unable to open KvStore"),
                            ),
                            EngineType::Sled => Engine::Sled(
                                SledKvsEngine::open(temp_dir.path())
                                    .expect("unable to open SledKvsEngine"),
                            ),
                        };

                        let key = gen_random_string();
                        let value = gen_random_string();

                        match store {
                            Engine::Kvs(ref s) => s.set(key.clone(), value).unwrap(),
                            Engine::Sled(ref s) => s.set(key.clone(), value).unwrap(),
                        };

                        (store, key.clone())
                    },
                    |(store, key)| {
                        match store {
                            Engine::Kvs(s) => s.get(key).unwrap(),
                            Engine::Sled(s) => s.get(key).unwrap(),
                        };
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
