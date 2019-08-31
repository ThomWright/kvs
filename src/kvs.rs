use crate::bytes::Bytes;
use crate::KvsError;
use crate::Result;
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
    index: HashMap<String, ValueInfo>,
    uncompacted: Bytes,
}

#[derive(Debug, Clone, Copy)]
struct ValueInfo {
    /// Position of value in file
    file_offset: Bytes,

    /// Size of serialised command in file
    size: Bytes,
}

impl KvStore {
    /// Create a new KvStore using a log file in the given directory.
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<KvStore> {
        let mut file_path = path.into();
        file_path.push(".kvs");

        std::fs::create_dir_all(&file_path)?;

        file_path.push("log.json");

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)?;

        let buffered_reader = BufReader::new(file.try_clone()?);
        let deserializer = serde_json::Deserializer::from_reader(buffered_reader);
        let mut commands = deserializer.into_iter::<Command>();

        let mut index = HashMap::<String, ValueInfo>::new();

        let mut file_offset = Bytes(0);
        let mut uncompacted = Bytes(0);
        while let Some(command) = commands.next() {
            let next_file_offset: Bytes = commands.byte_offset().try_into()?;
            let cmd_size = next_file_offset - file_offset;

            let Command { key, value } = command?;

            if let Some(ValueInfo {
                size: prev_cmd_size,
                ..
            }) = index.get(&key)
            {
                uncompacted += prev_cmd_size;
            }

            match value {
                Some(_) => {
                    index.insert(
                        key,
                        ValueInfo {
                            file_offset,
                            size: cmd_size,
                        },
                    );
                }
                None => {
                    uncompacted += cmd_size;
                    index.remove(&key);
                }
            }

            file_offset = next_file_offset;
        }

        Ok(KvStore {
            log: file,
            index,
            uncompacted,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        #![allow(missing_docs)]

        match self.index.get(&key) {
            None => Ok(None),

            Some(&ValueInfo { file_offset, .. }) => {
                let mut buffered_reader = BufReader::new(self.log.try_clone()?);
                buffered_reader.seek(SeekFrom::Start(file_offset.0))?;

                let mut deserializer = serde_json::Deserializer::from_reader(buffered_reader);
                let command = Command::deserialize(&mut deserializer)?;

                let Command { key: ckey, value } = command;

                if ckey == key {
                    match value {
                        Some(_) => Ok(value),
                        None => Err(KvsError::UnexpectedCommand {})?,
                    }
                } else {
                    Err(KvsError::UnexpectedKey {})?
                }
            }
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        #![allow(missing_docs)]

        let ser_command = serde_json::to_vec(&Command {
            key: key.clone(),
            value: Some(value.clone()),
        })?;

        let mut buffered_writer = BufWriter::new(self.log.try_clone()?);
        let write_pos = buffered_writer.seek(SeekFrom::End(0))?;

        buffered_writer.write_all(&ser_command)?;
        buffered_writer.flush()?;

        if let Some(ValueInfo { size, .. }) = self.index.get(&key) {
            self.uncompacted += size;
        }
        self.index.insert(
            key,
            ValueInfo {
                file_offset: Bytes(write_pos),
                size: Bytes(ser_command.len().try_into()?),
            },
        );

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        #![allow(missing_docs)]

        match self.index.get(&key) {
            None => Err(KvsError::KeyNotFound {})?,

            Some(ValueInfo { size: prev_cmd_size, .. }) => {
                let ser_command = serde_json::to_vec(&Command {
                    key: key.clone(),
                    value: None,
                })?;

                let mut buffered_writer = BufWriter::new(self.log.try_clone()?);

                buffered_writer.write_all(&ser_command)?;
                buffered_writer.flush()?;

                let cmd_len: Bytes = ser_command.len().try_into()?;
                self.uncompacted = self.uncompacted + prev_cmd_size + cmd_len;

                self.index.remove(&key);

                Ok(())
            }
        }
    }
}

/// Operations which can be performed on the database.
/// A 'remove' command has `value` equal to `None`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Command {
    #[serde(rename = "k")]
    key: String,

    #[serde(rename = "v")]
    value: Option<String>,
}
