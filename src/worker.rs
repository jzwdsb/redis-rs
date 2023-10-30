//! implementing the worker thread model
//! the overall architecture is as follows
//! - the main thread is responsible for managing the connections and
//!   dispatch readable events to the worker threads
//! - the main thread dispatch the incoming requests to the worker threads
//! - one of the worker threads parses the incoming bytes into commands
//! - then send the commands to the server thread via a channel
//! - then the server thread execute the commands and send the response back to the worker threads via a channel
//! - one of the worker threads will serialize the response into bytes and send it back to the client
//! 
//! 

use mio::{Events, Poll, Token};
use std::{
    collections::HashMap,
    sync::{
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
};

use crate::{cmd, connection::AsyncConnection, RedisErr};

// thess message type is used by the worker to communicate with the server
pub struct Message {
    stream: Arc<AsyncConnection>,
}

unsafe impl Sync for Message {}

// the worker is responsible for
// - handling a subset connections
// - parse the incoming bytes into commands
// - send the command to the server thread
// - recv the response from the server thread
// - send the response back to the client
// - close the connection

// the worker respesents a single running thread
// the running context is stored in the worker
// since we need to share the worker between the main thread and the worker thread
// the worker needs to be thread safe, so we need to wrap it in an Arc<Mutex<Worker>>
pub struct Worker {
    id: usize,
    poll: Poll,
    sender: Sender<Message>, // send message to the server
    recv: Receiver<Message>, // recv message from the server
    events: Events,          // the events that this worker is interested in
    sockets: Arc<Mutex<HashMap<Token, Arc<AsyncConnection>>>>, // the sockets that this worker is handling
}

impl Worker {
    pub fn new(
        id: usize,
        sender: Sender<Message>,
        recv: Receiver<Message>,
    ) -> Result<Worker, RedisErr> {
        let poll = Poll::new()?;
        Ok(Worker {
            id,
            poll,
            sender,
            recv,
            events: Events::with_capacity(1024),
            sockets: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // called by main thread to start the worker
    pub fn run(&mut self) {}

    fn poll(&mut self) {}

    // called by main thread to add new connections to the worker
    // need to be concurrent safe

    pub fn number_of_connections(&self) -> usize {
        self.sockets.lock().unwrap().len()
    }
}

// the worker pool is used to manage all the workers
// the main thread interact with the worker pool instead of the workers directly
// it is responsible for
// - creating new workers
// - dispatching new connections to the workers
// - load balancing among workers (dispatch new connections to the least busy worker)
pub struct WorkerPool {
    workers: Vec<Arc<Mutex<Worker>>>,
}

impl WorkerPool {
    // create a new worker pool with the given size
    pub fn new(_size: usize) -> Result<WorkerPool, RedisErr> {
        todo!("create a new worker pool")
    }

    // dispatch the new event to the worker
    // after we handle the read event, we should tell the server to invert the interest of the socket to write
    pub fn dispatch_read_event(&mut self, message: Arc<AsyncConnection>) {
        // find the least busy worker

        // send the message to the worker
    }

    // the write event is initiated by the database server
    // the server send the response back to the worker

    pub fn dispatch_write_event(&mut self, message: Arc<AsyncConnection>) {
        // find the least busy worker

        // send the message to the worker
    }

    pub fn run(&mut self) -> Result<(Sender<Message>, Receiver<Message>), RedisErr> {
        todo!("run the worker pool")
    }
}
