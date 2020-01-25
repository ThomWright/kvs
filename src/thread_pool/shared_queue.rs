use super::{ThreadPool, ThreadPoolMessage};
use crate::Result;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::sync::Arc;
use std::thread;

#[derive(Debug)]
struct PoolData {
    sender: Sender<ThreadPoolMessage>,
    receiver: Receiver<ThreadPoolMessage>,
    num_threads: u32,
}

/// A simple home-grown threadpool using `crossbeam`'s unbounded channel for distributing work.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct SharedQueueThreadPool {
    data: Arc<PoolData>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(num_threads: u32) -> Result<Self> {
        let (s, r) = unbounded::<ThreadPoolMessage>();

        let pool = Arc::new(PoolData {
            sender: s,
            receiver: r,
            num_threads,
        });

        for _ in 0..num_threads {
            spawn(pool.clone());
        }

        Ok(SharedQueueThreadPool { data: pool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.data
            .sender
            .send(ThreadPoolMessage::RunJob(Box::new(job)))
            .unwrap_or_else(|_| println!("Unable to spawn job: channel disconnected"));
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.data.num_threads {
            self.data
                .sender
                .send(ThreadPoolMessage::Shutdown)
                .unwrap_or(());
        }
    }
}

fn spawn(pool: Arc<PoolData>) {
    let receiver = pool.receiver.clone();
    thread::spawn(move || {
        let _sentinel = Sentinel { pool };
        loop {
            match receiver.recv() {
                Ok(msg) => match msg {
                    ThreadPoolMessage::RunJob(job) => job(),
                    ThreadPoolMessage::Shutdown => return,
                },
                Err(_) => {}
            }
        }
    });
}

struct Sentinel {
    pool: Arc<PoolData>,
}
impl Drop for Sentinel {
    fn drop(&mut self) {
        if thread::panicking() {
            spawn(self.pool.clone());
        }
    }
}
