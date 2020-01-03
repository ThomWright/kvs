use crate::Result;

/// Interface for a simple key-value store.
#[allow(clippy::module_name_repetitions)]
pub trait KvsEngine: Clone + Send + 'static {
    /// Get the value for the given key, if it exists.
    fn set(&self, key: String, value: String) -> Result<()>;
    /// Set the value for the given key, overwriting the previous value if it existed.
    fn get(&self, key: String) -> Result<Option<String>>;
    /// Remove the value for the given key. Will error if the key does not exist.
    fn remove(&self, key: String) -> Result<()>;
}
