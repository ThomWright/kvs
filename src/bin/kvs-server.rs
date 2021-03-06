extern crate clap;
#[macro_use]
extern crate slog;
extern crate slog_term;

use clap::{crate_version, App, Arg};
use kvs::{
    existing_engine,
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    EngineType, KvStore, KvsServer, SledKvsEngine,
};
use num_cpus;
use slog::Drain;
use std::convert::TryInto;
use std::env;

fn main() -> kvs::Result<()> {
    if let Err(e) = run_kvs() {
        // Print the Display message for any error.
        // Simply returning the error will print the Debug version, which is not as nice.
        eprintln!("{}", e);
        std::process::exit(1)
    }
    Ok(())
}

fn run_kvs() -> kvs::Result<()> {
    let version = env!("CARGO_PKG_VERSION");

    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    let log = slog::Logger::root(drain, o!("version" => version));

    let matches = App::new(&[env!("CARGO_PKG_NAME"), "-server"].concat())
        .version(crate_version!())
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(version)
        .arg(
            Arg::with_name("addr")
                .help("An IP address, either v4 or v6, and a port number, with the format IP:PORT")
                .long("addr")
                .takes_value(true)
                .value_name("ADDR")
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            Arg::with_name("engine")
                .help("Storage engine to use")
                .long("engine")
                .takes_value(true)
                .possible_values(&["kvs", "sled"])
                .value_name("ENGINE"),
        )
        .get_matches();

    let addr = matches.value_of("addr").unwrap();
    let engine_arg = matches.value_of("engine").map(|e| match e {
        "kvs" => EngineType::Kvs,
        "sled" => EngineType::Sled,
        _ => panic!("Unexpected engine type argument"),
    });

    // TODO: move into KvsServer?
    let engine_type = match (engine_arg, existing_engine(&env::current_dir()?)) {
        (None, None) => Ok(EngineType::Kvs),
        (Some(engine_arg), None) => Ok(engine_arg),
        (None, Some(current_engine)) => Ok(current_engine),
        (Some(engine_arg), Some(current_engine)) => {
            if engine_arg == current_engine {
                Ok(current_engine)
            } else {
                Err(KvsServerError::EngineMismatch {})
            }
        }
    }?;

    info!(log, "Starting kvs server"; "addr" => addr, "engine" => engine_type);

    let curr_dir = std::env::current_dir()?;
    let pool = SharedQueueThreadPool::new(
        num_cpus::get()
            .try_into()
            .expect("Can't convert from usize to u32"),
    )?;
    match engine_type {
        EngineType::Kvs => {
            let server = KvsServer::new(log, KvStore::open(&curr_dir)?, pool)?;
            server.run(addr)?;
            Ok(())
        }

        EngineType::Sled => {
            let server = KvsServer::new(log, SledKvsEngine::open(&curr_dir)?, pool)?;
            server.run(addr)?;
            Ok(())
        }
    }
}

#[derive(Debug, failure::Fail)]
enum KvsServerError {
    #[fail(display = "Chosen engine does not match existing data")]
    EngineMismatch {},
}
