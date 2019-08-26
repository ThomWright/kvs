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
use std::convert::TryInto;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
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
    log: File,
    index: HashMap<String, LogPointer>,
}

type LogPointer = u64;

impl KvStore {
    /// Create a new KvStore using a log file in the given directory.
    pub fn open(path: &std::path::Path) -> Result<KvStore> {
        let mut file_path = path.to_path_buf();
        file_path.push("log.json");

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)?;

        let buffered_reader = BufReader::new(file.try_clone()?);
        let deserializer = serde_json::Deserializer::from_reader(buffered_reader);
        let mut commands = deserializer.into_iter::<Command>();

        let mut index = HashMap::<String, LogPointer>::new();

        let mut file_offset = 0;
        while let Some(command) = commands.next() {
            match command? {
                Command::Set { key, value: _ } => {
                    index.insert(key, file_offset);
                }
                Command::Rm { key } => {
                    index.remove(&key);
                }
            }

            file_offset = commands.byte_offset().try_into()?;
        }

        Ok(KvStore {
            log: file,
            index: index,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        #![allow(missing_docs)]

        match self.index.get(&key) {
            None => return Ok(None),

            Some(&seek_pos) => {
                let mut buffered_reader = BufReader::new(self.log.try_clone()?);
                buffered_reader.seek(SeekFrom::Start(seek_pos))?;

                let mut deserializer = serde_json::Deserializer::from_reader(buffered_reader);
                let command = Command::deserialize(&mut deserializer)?;

                match command {
                    Command::Set { key: ckey, value } => {
                        if ckey == key {
                            Ok(Some(value))
                        } else {
                            Err(KvsError::InvalidKeyFound {})?
                        }
                    }
                    Command::Rm { key: _ } => Err(KvsError::InvalidCommandFound {})?,
                }
            }
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        #![allow(missing_docs)]

        let ser_command = serde_json::to_vec(&Command::Set {
            key: key.clone(),
            value: value.clone(),
        })?;

        let mut buffered_writer = BufWriter::new(self.log.try_clone()?);
        let write_pos = buffered_writer.seek(SeekFrom::End(0))?;

        buffered_writer.write(&ser_command)?;
        buffered_writer.flush()?;

        self.index.insert(key, write_pos);

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        #![allow(missing_docs)]

        if let None = self.get(key.clone())? {
            return Err(KvsError::KeyNotFound {})?;
        }

        let ser_command = serde_json::to_vec(&Command::Rm { key: key.clone() })?;

        let mut buffered_writer = BufWriter::new(self.log.try_clone()?);

        buffered_writer.write(&ser_command)?;
        buffered_writer.flush()?;

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

    #[fail(display = "Invalid command found in log")]
    InvalidCommandFound {},

    #[fail(display = "Invalid key found in log")]
    InvalidKeyFound {},
}
