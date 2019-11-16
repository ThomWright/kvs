use crate::Result;

/// Interface for a simple key-value store.
#[allow(clippy::module_name_repetitions)]
pub trait KvsEngine {
    /// Get the value for the given key, if it exists.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Set the value for the given key, overwriting the previous value if it existed.
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Remove the value for the given key. Will error if the key does not exist.
    fn remove(&mut self, key: String) -> Result<()>;
}
