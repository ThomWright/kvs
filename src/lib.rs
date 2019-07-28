//! The `kvs` crate is a simple key-value store, made by following the
//! [Practical Networked Applications in Rust](https://github.com/pingcap/talent-plan/tree/master/rust) course.

#![deny(missing_docs)]

use std::collections::HashMap;

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
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        #![allow(missing_docs)]

        KvStore {
            store: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        #![allow(missing_docs)]

        self.store.get(&key).cloned()
    }
    pub fn set(&mut self, key: String, value: String) {
        #![allow(missing_docs)]

        self.store.insert(key, value);
    }
    pub fn remove(&mut self, key: String) {
        #![allow(missing_docs)]

        self.store.remove(&key);
    }
}
