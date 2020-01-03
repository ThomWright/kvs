use crate::bytes::Bytes;
use crate::engine::KvsEngine;
use crate::errors::KvsError;
use crate::file;
use crate::file::{get_log_file_ids, KvsWriter};
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub const KVS_DIR: &str = ".kvs";
const MAX_UNCOMPACTED: Bytes = Bytes(1024 * 1024);

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
/// # use crate::kvs::KvsEngine;
/// let mut store = kvs::KvStore::open(".")?;
///
/// let key = "key".to_owned();
///
/// store.set(key.clone(), "value".to_owned());
///
/// let saved_val = store.get(key.clone());
/// # Ok::<(), failure::Error>(())
/// ```
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct KvStore {
    store: Arc<Mutex<InternalKvStore>>,
}

impl KvStore {
    /// Create a new KvStore, using the given `path` directory.
    /// The log files will be stored in a directory named `.kvs` inside `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let store = InternalKvStore::open(path)?;
        Ok(KvStore {
            store: Arc::new(Mutex::new(store)),
        })
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
struct InternalKvStore {
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
    /// Identifier for file the value is stored in
    file_id: file::Id,

    /// Position of value in file
    file_offset: Bytes,

    /// Size of serialised command in file
    size: Bytes,
}

impl InternalKvStore {
    fn open(path: impl Into<PathBuf>) -> Result<InternalKvStore> {
        let path_dir = path.into();
        if !path_dir.is_dir() {
            return Err(KvsError::NotADirectory.into());
        }
        let kvs_dir = path_dir.join(KVS_DIR);

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

        Ok(InternalKvStore {
            path: kvs_dir,
            writer,
            readers,

            index,
            uncompacted,
        })
    }

    fn compact(&mut self) -> Result<()> {
        // create new file to write compacted logs into
        let compaction_file_id = self.writer.id + 1;
        let mut compacted_log_writer = {
            let writer = KvsWriter::new(&self.path, compaction_file_id)?;
            self.readers.insert(
                compaction_file_id,
                file::new_reader(&self.path, compaction_file_id)?,
            );
            writer
        };

        // create new file to write new logs into
        let new_log_writer = {
            let file_id = self.writer.id + 2;
            let writer = KvsWriter::new(&self.path, file_id)?;
            self.readers
                .insert(file_id, file::new_reader(&self.path, file_id)?);
            writer
        };

        // switch writer
        self.uncompacted = Bytes(0);
        self.writer = new_log_writer;

        for val_info in self.index.values_mut() {
            if val_info.file_id == self.writer.id {
                // we're only compacting logs in old files
                continue;
            }

            // copy from src file to compacted log file
            let reader = self
                .readers
                .get_mut(&val_info.file_id)
                .expect("Reader not found for file ID");
            reader.seek(SeekFrom::Start(val_info.file_offset.0))?;

            let new_offset = compacted_log_writer.offset;

            let bytes_copied =
                std::io::copy(&mut reader.take(val_info.size.0), &mut compacted_log_writer)?;

            // update index
            *val_info = ValueInfo {
                file_id: compaction_file_id,
                file_offset: Bytes(new_offset),
                size: Bytes(bytes_copied),
            }
        }
        self.writer.flush()?;

        // remove all unused files
        let file_ids_to_rm: Vec<_> = self
            .readers
            .keys()
            .filter(|&&id| id < compaction_file_id)
            .cloned()
            .collect();
        for id in file_ids_to_rm {
            self.readers.remove(&id);
            file::remove(&self.path, id)?;
        }

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        let mut store = self.store.lock().unwrap();

        if let Some(&ValueInfo {
            file_offset,
            file_id,
            size,
        }) = store.index.get(&key)
        {
            let reader = store
                .readers
                .get_mut(&file_id)
                .expect("Reader not found for file ID");
            reader.seek(SeekFrom::Start(file_offset.0))?;

            let Command { value, .. } = serde_json::from_reader(reader.take(size.0))?;
            match value {
                None => Err(KvsError::UnexpectedCommand.into()),
                Some(_) => Ok(value),
            }
        } else {
            Ok(None)
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let mut store = self.store.lock().unwrap();

        let write_pos = store.writer.offset;

        serde_json::to_writer(
            &mut store.writer,
            &Command {
                key: key.clone(),
                value: Some(value.clone()),
            },
        )?;
        store.writer.flush()?;

        let cmd_len = store.writer.offset - write_pos;

        if let Some(&ValueInfo { size, .. }) = store.index.get(&key) {
            store.uncompacted += size;
        }

        let writer_id = store.writer.id;
        store.index.insert(
            key,
            ValueInfo {
                file_offset: Bytes(write_pos),
                size: Bytes(cmd_len),
                file_id: writer_id,
            },
        );

        if store.uncompacted > MAX_UNCOMPACTED {
            store.compact()?
        }

        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut store = self.store.lock().unwrap();

        match store.index.get(&key) {
            None => Err(KvsError::KeyNotFound.into()),

            Some(&ValueInfo {
                size: prev_cmd_size,
                ..
            }) => {
                let write_pos = store.writer.offset;

                serde_json::to_writer(
                    &mut store.writer,
                    &Command {
                        key: key.clone(),
                        value: None,
                    },
                )?;
                store.writer.flush()?;

                let cmd_len = store.writer.offset - write_pos;
                store.uncompacted = store.uncompacted + prev_cmd_size + Bytes(cmd_len);

                store.index.remove(&key);

                if store.uncompacted > MAX_UNCOMPACTED {
                    store.compact()?
                }

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
    reader: &mut BufReader<File>,
    index: &mut Index,
) -> Result<Bytes> {
    let deserializer = serde_json::Deserializer::from_reader(reader);
    let mut commands = deserializer.into_iter::<Command>();

    let mut uncompacted = Bytes(0);
    let mut file_offset = Bytes(0);
    while let Some(command) = commands.next() {
        let next_file_offset: Bytes = commands.byte_offset().try_into()?;
        let cmd_size = next_file_offset - file_offset;

        let Command { key, value } = command?;

        // value is being overwritten
        if let Some(ValueInfo {
            size: prev_cmd_size,
            ..
        }) = index.get(&key)
        {
            uncompacted += prev_cmd_size;
        }

        match value {
            // Set
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
            // Rm
            None => {
                uncompacted += cmd_size;
                index.remove(&key);
            }
        }

        file_offset = next_file_offset;
    }

    Ok(uncompacted)
}
