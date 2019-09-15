use crate::bytes::Bytes;
use crate::file;
use crate::file::get_log_file_ids;
use crate::file::KvsWriter;
use crate::KvsError;
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;

/// Implementation of a simple, persistent key-value store.
///
/// The data is stored in multiple files in a single directory.
/// Only the latest log file is actively written to.
///
/// New files are created when compaction occurs.
///
/// # Examples
///
/// Setting and retrieving a value for the key `key`.
///
/// ```
/// let mut store = kvs::KvStore::open(".")?;
///
/// let key = "key".to_owned();
///
/// store.set(key.clone(), "value".to_owned());
///
/// let saved_val = store.get(key.clone());
/// # Ok::<(), failure::Error>(())
/// ```
#[derive(Debug)]
pub struct KvStore {
    /// Path of directory containing log files
    path: PathBuf,
    writer: KvsWriter,

    readers: Readers,
    index: Index,
    uncompacted: Bytes,
}

type Readers = HashMap<file::Id, BufReader<File>>;
type Index = HashMap<String, ValueInfo>;

#[derive(Debug, Clone, Copy)]
struct ValueInfo {
    /// Position of value in file
    file_offset: Bytes,

    /// Size of serialised command in file
    size: Bytes,

    /// Identifier for file the value is stored in
    file_id: file::Id,
}

impl KvStore {
    /// Create a new KvStore, using the given `path` directory.
    /// The log files will be stored in a directory named `.kvs` inside `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path_dir = path.into();
        if !path_dir.is_dir() {
            Err(KvsError::NotADirectory {})?
        }
        let kvs_dir = path_dir.join(".kvs");

        fs::create_dir_all(&kvs_dir)?;

        let mut file_ids = get_log_file_ids(&kvs_dir)?;
        file_ids.sort_unstable();

        let mut readers = HashMap::new();
        let mut index = HashMap::<String, ValueInfo>::new();
        let mut uncompacted = Bytes(0);

        for id in &file_ids {
            let mut buffered_reader = file::new_reader(&kvs_dir, *id)?;

            uncompacted += load_file_into_index(*id, &mut buffered_reader, &mut index)?;

            readers.insert(*id, buffered_reader);
        }

        let write_file_id = file_ids.last().unwrap_or(&0) + 1;
        let writer = KvsWriter::new(&kvs_dir, write_file_id)?;
        readers.insert(write_file_id, file::new_reader(&kvs_dir, write_file_id)?);

        Ok(KvStore {
            path: kvs_dir,
            writer,
            readers,

            index,
            uncompacted,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        #![allow(missing_docs)]

        match self.index.get(&key) {
            None => Ok(None),

            Some(&ValueInfo {
                file_offset,
                file_id,
                ..
            }) => {
                let reader = self
                    .readers
                    .get_mut(&file_id)
                    .expect("Reader not found for file ID");
                reader.seek(SeekFrom::Start(file_offset.0))?;

                let mut deserializer = serde_json::Deserializer::from_reader(reader);
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

        let write_pos = self.writer.write_log(&ser_command)?;

        if let Some(ValueInfo { size, .. }) = self.index.get(&key) {
            self.uncompacted += size;
        }
        self.index.insert(
            key,
            ValueInfo {
                file_offset: Bytes(write_pos),
                size: Bytes(ser_command.len().try_into()?),
                file_id: self.writer.id,
            },
        );

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        #![allow(missing_docs)]

        match self.index.get(&key) {
            None => Err(KvsError::KeyNotFound {})?,

            Some(ValueInfo {
                size: prev_cmd_size,
                ..
            }) => {
                let ser_command = serde_json::to_vec(&Command {
                    key: key.clone(),
                    value: None,
                })?;

                self.writer.write_log(&ser_command)?;

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

fn load_file_into_index(
    file_id: file::Id,
    buffered_reader: &mut BufReader<File>,
    index: &mut Index,
) -> Result<Bytes> {
    let deserializer = serde_json::Deserializer::from_reader(buffered_reader);
    let mut commands = deserializer.into_iter::<Command>();

    let mut uncompacted = Bytes(0);
    let mut file_offset = Bytes(0);
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
                        file_id,
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

    Ok(uncompacted)
}
