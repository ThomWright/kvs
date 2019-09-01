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
use std::path::PathBuf;

/* TODO: use multiple files
 * - ordered
 * - write to latest
 * - other read-only
 * - to compact (with latest file X)
 *   - create new file (X+1), to write compacted logs into
 *   - create another new file (X+2), all normal writes go here
 *   - write compacted logs to X+1, duplication is fine
 *   - remove all files <=X
 */

/// Implementation of a simple, persistent key-value store.
///
/// The data is stored in multiple files in a single directory.
/// Only the latest log file is actively written to.
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
    /// Path of directory containing log files
    path: PathBuf,
    writer: BufWriter<File>,
    readers: HashMap<FileId, BufReader<File>>,
    index: HashMap<String, ValueInfo>,
    uncompacted: Bytes,
}

#[derive(Debug, Clone, Copy)]
struct ValueInfo {
    /// Position of value in file
    file_offset: Bytes,

    /// Size of serialised command in file
    size: Bytes,

    /// Identifier for file the value is stored in
    file_id: FileId,
}

type FileId = u64;

impl KvStore {
    /// Create a new KvStore, using the given `path` directory.
    /// The log files will be stored in a directory named `.kvs` inside `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut kvs_dir = path.into();
        kvs_dir.push(".kvs");

        std::fs::create_dir_all(&kvs_dir)?;

        let mut index = HashMap::<String, ValueInfo>::new();

        let mut file_path = kvs_dir.clone();
        file_path.push("1.log");

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)?;

        let mut buffered_reader = BufReader::new(file.try_clone()?);
        let deserializer = serde_json::Deserializer::from_reader(&mut buffered_reader);
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
                            file_id: 1,
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

        let buffered_writer = BufWriter::new(file.try_clone()?);

        let mut readers = HashMap::new();
        readers.insert(1, buffered_reader);

        Ok(KvStore {
            path: kvs_dir,
            writer: buffered_writer,
            readers,

            index,
            uncompacted,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        #![allow(missing_docs)]

        match self.index.get(&key) {
            None => Ok(None),

            Some(&ValueInfo { file_offset, .. }) => {
                let reader = self
                    .readers
                    .get_mut(&1)
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

        let write_pos = self.writer.seek(SeekFrom::End(0))?;

        self.writer.write_all(&ser_command)?;
        self.writer.flush()?;

        if let Some(ValueInfo { size, .. }) = self.index.get(&key) {
            self.uncompacted += size;
        }
        self.index.insert(
            key,
            ValueInfo {
                file_offset: Bytes(write_pos),
                size: Bytes(ser_command.len().try_into()?),
                file_id: 1,
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

                self.writer.write_all(&ser_command)?;
                self.writer.flush()?;

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
