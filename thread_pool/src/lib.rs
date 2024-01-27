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
//
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

use crossbeam_channel::{Receiver, Sender};
use std::thread;

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

    /// Shutdown the thread pool.
    /// after calling this function, the thread pool will no longer accept new jobs;
    /// it will wait for all active jobs to finish and then terminate the worker threads.
    pub fn shutdown(self) {
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

    use std::sync::{Arc, Mutex};

    #[test]
    fn test_thread_pool() {
        let pool = ThreadPool::new(4);

        for i in 0..4 {
            pool.execute(move || {
                println!("Hello from thread {}", i);
            });
        }

        println!("Shutting down.");

        let value = Arc::new(Mutex::new(0));

        for _ in 0..10 {
            let value = Arc::clone(&value);

            pool.execute(move || {
                let mut num = value.lock().unwrap();

                *num += 1;
            });
        }

        println!("value = {}", *value.lock().unwrap());
    }
}
