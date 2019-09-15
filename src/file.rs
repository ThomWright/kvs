use crate::KvsError;
use crate::Result;
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

/// Identifies a log file
pub type Id = u64;

fn format_name(id: Id) -> String {
    format!("{}.log", id)
}

pub fn get_log_file_ids(kvs_dir: &PathBuf) -> Result<Vec<Id>> {
    fs::read_dir(&kvs_dir)?
        .flat_map(|f| f)
        .map(|file| file.path())
        .filter(|path| path.extension() == Some(&OsString::from("log")))
        .flat_map(|path| path.file_stem().and_then(OsStr::to_str).map(String::from))
        .map(|file_stem| {
            Ok(file_stem
                .parse::<Id>()
                .map_err(|_| KvsError::UnexpectedFileName {})?)
        })
        .collect::<Result<Vec<Id>>>()
}

pub fn new_reader(dir: &PathBuf, id: Id) -> Result<BufReader<File>> {
    let file_path = dir.join(format_name(id));
    Ok(BufReader::new(
        OpenOptions::new().read(true).open(&file_path)?,
    ))
}

#[derive(Debug)]
pub struct KvsWriter {
    pub id: Id,
    offset: u64,
    writer: BufWriter<File>,
}

impl KvsWriter {
    pub fn new(dir: &PathBuf, file_id: Id) -> Result<KvsWriter> {
        let file_path = dir.join(format_name(file_id));

        let writer = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(&file_path)?,
        );

        Ok(KvsWriter {
            id: file_id,
            offset: 0,
            writer,
        })
    }

    /// Writes to the log file, returning the position the log was written to.
    pub fn write_log(&mut self, buf: &[u8]) -> Result<u64> {
        self.offset = self.writer.seek(SeekFrom::End(0))?;
        let write_pos = self.offset;

        let written = self.writer.write(&buf)?;
        self.offset += written as u64;

        self.writer.flush()?;

        Ok(write_pos)
    }
}
