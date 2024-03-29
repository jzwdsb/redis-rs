//! # thread_pool
//! A thread pool implementation in Rust.
//!
//! ## Usage
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! thread_pool = "0.1.0"
//!
//! ```
//!
//! And this to your crate root:
//!
//! ```rust
//! extern crate thread_pool;
//! ```
//!
//! ## Example
//!
//! ```rust
//! extern crate thread_pool;
//!
//! use thread_pool::ThreadPool;
//!
//! fn main() {
//!     let pool = ThreadPool::new(4);
//!
//!    for i in 0..4 {
//!       pool.execute(move || {
//!          println!("Hello from thread {}", i);
//!      });
//!   }
//! }
//!
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![allow(dead_code)]

use crossbeam_channel::{Receiver, Sender};
use std::thread;

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
}

struct Job {
    id: usize,
    func: Box<dyn FnOnce() + Send + 'static>,
}

enum Message {
    NewJob(Job),
    Terminate,
}

/// A thread pool implementation in Rust.
#[derive(Debug)]
pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Sender<Message>,
}

impl Worker {
    fn new(id: usize, receiver: Receiver<Message>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    (job.func)();
                }
                Message::Terminate => {
                    break;
                }
            }
        });

        Worker { id, thread }
    }

    fn id(&self) -> usize {
        self.id
    }

    fn join(self) {
        self.thread.join().unwrap();
    }
}

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, receiver.clone()));
        }

        ThreadPool { workers, sender }
    }

    /// Execute a function in a thread.
    ///
    /// # Panics
    ///
    /// The `execute` function will panic if the thread pool has been shutdown.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Job {
            id: 0,
            func: Box::new(f),
        };

        self.sender.send(Message::NewJob(job)).unwrap();
    }

    /// Join all threads in the thread pool.
    /// after calling this function, the thread pool will no longer accept new jobs;
    /// it will wait for all active jobs to finish and then terminate the worker threads.
    pub fn join(self) {
        for _ in 0..self.workers.len() {
            self.sender.send(Message::Terminate).unwrap();
        }

        for worker in self.workers {
            worker.thread.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    };

    #[test]
    fn test_thread_pool() {
        let pool = ThreadPool::new(4);

        for i in 0..4 {
            pool.execute(move || {
                println!("Hello from thread {}", i);
            });
        }

        println!("Shutting down.");

        let value = Arc::new(AtomicU64::new(0));

        for _ in 0..10 {
            let value = Arc::clone(&value);

            pool.execute(move || {
                value.fetch_add(1, Ordering::SeqCst);
            });
        }
        pool.join();

        assert_eq!(value.load(Ordering::SeqCst), 10);
        println!("value = {}", value.load(Ordering::SeqCst));
    }
}
