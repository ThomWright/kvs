//! The `kvs` crate is a simple key-value store, made by following the
//! [Practical Networked Applications in Rust](https://github.com/pingcap/talent-plan/tree/master/rust) course.

#![deny(
    missing_debug_implementations,
    missing_copy_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::module_name_repetitions)]

#[macro_use]
extern crate slog;

mod bytes;
mod client;
mod engine;
mod errors;
mod file;
mod network_data;
mod server;
mod sled;
mod store;

pub use client::KvsClient;
pub use client::Error as ClientError;
pub use engine::KvsEngine;
pub use errors::{KvsError, Result};
pub use server::{EngineType, KvsServer};
pub use store::KvStore;
pub use sled::SledKvsEngine;
