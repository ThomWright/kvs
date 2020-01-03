use super::KvsEngine;
use crate::errors::KvsError;
use crate::Result;
use sled::Db;
use std::fs;
use std::path::PathBuf;
use std::str;
use std::sync::{Arc, Mutex};

pub const SLED_DIR: &str = ".sled";

/// Implementation of a simple, persistent key-value store using `sled`.
#[derive(Debug, Clone)]
#[allow(clippy::module_name_repetitions)]
pub struct SledKvsEngine {
    db: Arc<Mutex<Db>>,
}

impl SledKvsEngine {
    /// Create a new sled store inside the given `path` directory.
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let path_dir = path.into();
        if !path_dir.is_dir() {
            return Err(KvsError::NotADirectory.into());
        }
        let sled_dir = path_dir.join(SLED_DIR);

        fs::create_dir_all(&sled_dir)?;

        let db = Db::open(sled_dir)?;

        Ok(SledKvsEngine {
            db: Arc::new(Mutex::new(db)),
        })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let store = self.db.lock().unwrap();

        match store.get(key)? {
            None => Ok(None),
            Some(buf) => Ok(Some(String::from_utf8(buf.to_vec())?)),
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let store = self.db.lock().unwrap();

        store.insert(key, value.into_bytes())?;
        store.flush()?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let store = self.db.lock().unwrap();

        match store.remove(key)? {
            None => Err(KvsError::KeyNotFound.into()),
            Some(_) => {
                store.flush()?;
                Ok(())
            }
        }
    }
}
