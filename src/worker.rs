//! implementing the worker thread model
//! the overall architecture is as follows
//! - the main thread is responsible for accepting new connections
//! - the main thread will dispatch the new connections to the worker threads
//! - the worker threads will handle the connections
//! - the worker threads will parse the incoming bytes into commands
//! - the worker threads will send the commands to the server thread via a channel
//! - the server thread will execute the commands
//! - the server thread will send the response back to the worker threads via a channel
//! - the worker threads will serialize the response into bytes and send it back to the client
//!


use mio::{Token, Poll, Events};
use std::{sync::{
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    }, collections::HashMap};

use crate::{cmd, RedisErr, connection::AsyncConnection};

// thess message type is used by the worker to communicate with the server
pub struct Message {}

unsafe impl Sync for Message {}

// the worker is responsible for
// - handling a subset connections
// - parse the incoming bytes into commands
// - send the command to the server thread
// - recv the response from the server thread
// - send the response back to the client
// - close the connection
pub struct Worker {
    id: usize,
    poll: Poll,
    sender: Sender<Message>, // send message to the server
    recv: Receiver<Message>, // recv message from the server
    events: Events, // the events that this worker is interested in
    sockets: Arc<Mutex<HashMap<Token,Arc<AsyncConnection>>>>, // the sockets that this worker is handling
}

impl Worker {
    pub fn new(id: usize, sender: Sender<Message>, recv: Receiver<Message>) -> Result<Worker, RedisErr> {
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
    pub fn run(&mut self) {
        todo!("worker run")
    }

    pub fn add_event(&mut self, socket: Token) {
        todo!("add new connections to the worker thread")
    }

    pub fn number_of_connections(&self) -> usize {
        todo!("return the number of connections that this worker is handling")
    }
}

// the worker pool is used to manage all the workers
// the main thread interact with the worker pool instead of the workers directly
// it is responsible for
// - creating new workers
// - dispatching new connections to the workers
// - load balancing among workers (dispatch new connections to the least busy worker)
pub struct WorkerPool {
    
}

impl WorkerPool {
    // create a new worker pool with the given size
    pub fn new(_size: usize) -> Result<WorkerPool, RedisErr> {
        todo!("create a new worker pool")
    }

    pub fn add_socket(&mut self, socket: AsyncConnection) {
        todo!("add new connections to the worker pool")
    }

    pub fn run (&mut self) -> Result<(Sender<Message>, Receiver<Message>), RedisErr> {
        todo!("run the worker pool")
    }

}
