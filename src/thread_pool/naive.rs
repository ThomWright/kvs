use super::ThreadPool;
use crate::Result;
use std::thread;

/// Not really a pool, spawns a thread for every job.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, Copy)]
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> Result<Self> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
