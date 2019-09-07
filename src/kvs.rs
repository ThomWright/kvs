use crate::bytes::Bytes;
use crate::KvsError;
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
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
    writer: BufWriter<File>,
    write_file_id: FileId,
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
        let path_dir = path.into();
        if !path_dir.is_dir() {
            Err(KvsError::NotADirectory {})?
        }
        let kvs_dir = path_dir.join(".kvs");

        fs::create_dir_all(&kvs_dir)?;

        let mut file_ids = fs::read_dir(&kvs_dir)?
            .flat_map(|f| f)
            .map(|file| file.path())
            .filter(|path| path.extension() == Some(&OsString::from("log")))
            .flat_map(|path| path.file_stem().and_then(OsStr::to_str).map(String::from))
            .map(|file_stem| {
                Ok(file_stem
                    .parse::<FileId>()
                    .map_err(|_| KvsError::UnexpectedFileName {})?)
            })
            .collect::<Result<Vec<FileId>>>()?;

        file_ids.sort_unstable();

        let mut readers = HashMap::new();
        let mut index = HashMap::<String, ValueInfo>::new();
        let mut uncompacted = Bytes(0);

        for id in &file_ids {
            let file_path = kvs_dir.join(format!("{}.log", id));
            let mut buffered_reader =
                BufReader::new(OpenOptions::new().read(true).open(&file_path)?);

            uncompacted += load_file_into_index(*id, &mut buffered_reader, &mut index)?;

            readers.insert(*id, buffered_reader);
        }

        let write_file_id = file_ids.last().unwrap_or(&0) + 1;
        let file_path = kvs_dir.join(format!("{}.log", write_file_id));

        let buffered_writer = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&file_path)?,
        );

        readers.insert(
            write_file_id,
            BufReader::new(OpenOptions::new().read(true).open(&file_path)?),
        );

        Ok(KvStore {
            path: kvs_dir,
            writer: buffered_writer,
            write_file_id,
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
                file_id: self.write_file_id,
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

fn load_file_into_index(
    file_id: FileId,
    buffered_reader: &mut BufReader<File>,
    index: &mut HashMap<String, ValueInfo>,
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
