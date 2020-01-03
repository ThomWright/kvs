//! The `kvs` crate is a simple key-value store, made by following the
//! [Practical Networked Applications in Rust](https://github.com/pingcap/talent-plan/tree/master/rust) course.

#![deny(
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::module_name_repetitions, missing_docs)]

#[macro_use]
extern crate slog;

mod engines;
mod errors;
mod network;
pub mod thread_pool;

pub use self::network::KvsClient;
pub use self::engines::SledKvsEngine;
pub use self::engines::KvStore;
pub use self::engines::KvsEngine;
pub use self::errors::Result;
pub use self::network::{existing_engine, EngineType, KvsServer};
