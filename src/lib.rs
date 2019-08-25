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

use failure;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::result;

/// Implementation of the key-value store.
///
/// # Examples
///
/// Setting and retrieving a value for the key `key`.
///
/// ```
/// use kvs::KvStore;
///
/// let mut store = KvStore::new();
///
/// let key = "key".to_owned();
///
/// store.set(key.clone(), "value".to_owned());
///
/// let saved_val = store.get(key.clone());
/// ```
#[derive(Debug)]
pub struct KvStore {
    writer: BufWriter<File>,

    index: HashMap<String, String>,
}

// type LogPointer = u64;

impl KvStore {
    /// Create a new KvStore using a log file in the given directory.
    pub fn open(path: &std::path::Path) -> Result<KvStore> {
        let mut file_path = path.to_path_buf();
        file_path.push("log.json");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;

        let buffered_writer = BufWriter::new(file.try_clone()?);

        let iter = serde_json::Deserializer::from_reader(BufReader::new(file.try_clone()?))
            .into_iter::<Command>();

        let mut index = HashMap::new();
        for item in iter {
            match item? {
                Command::Set { key, value } => {
                    index.insert(key, value);
                }
                Command::Rm { key } => {
                    index.remove(&key);
                }
            }
        }

        Ok(KvStore {
            writer: buffered_writer,
            index: index,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        #![allow(missing_docs)]

        Ok(self.index.get(&key).map(|s| s.to_string()))
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        #![allow(missing_docs)]

        let ser_command = serde_json::to_vec(&Command::Set {
            key: key.clone(),
            value: value.clone(),
        })?;

        self.writer.write(&ser_command)?;

        self.index.insert(key, value);

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        #![allow(missing_docs)]

        if let None = self.get(key.clone())? {
            return Err(KvsError::KeyNotFound {})?;
        }

        let ser_command = serde_json::to_vec(&Command::Rm { key: key.clone() })?;

        self.writer.write(&ser_command)?;

        self.index.remove(&key);

        Ok(())
    }
}

/// Operations which can be performed on the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    #[allow(missing_docs)]
    Set { key: String, value: String },
    #[allow(missing_docs)]
    Rm { key: String },
}

/// Convenience Result type.
pub type Result<T> = result::Result<T, failure::Error>;

#[derive(Debug, failure::Fail)]
enum KvsError {
    #[fail(display = "Key not found")]
    KeyNotFound {},
}
