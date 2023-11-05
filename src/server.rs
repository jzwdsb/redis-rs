//! Redis Server implementation
//! use mio to achieve non-blocking IO, multiplexing and event driven
//! an event loop is used to handle all the IO events

use crate::cmd::{Command, Parser};
use crate::connection::{AsyncConnection, FrameReader, FrameWriter};
use crate::db::{DBHandler, Database};
use crate::frame::Frame;
use crate::worker;
use crate::RedisErr;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::net::TcpListener;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

pub struct ServerBuilder {
    addr: String,
    port: u16,
    max_client: usize,

    worker_thread_num: usize,
}

impl ServerBuilder {
    pub fn new(/* ... */) -> Self {
        Self {
            addr: "0.0.0.0".to_string(),
            port: 6379,
            max_client: 1024,
            worker_thread_num: 1,
        }
    }

    pub fn addr(mut self, addr: &str) -> Self {
        self.addr = addr.to_string();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn max_client(mut self, max_client: usize) -> Self {
        self.max_client = max_client;
        self
    }

    pub fn worker_thread_num(mut self, handler_num: usize) -> Self {
        self.worker_thread_num = handler_num;
        self
    }

    pub async fn build(self) -> Result<Server, RedisErr> {
        Server::new(
            &self.addr,
            self.port,
            self.max_client,
            self.worker_thread_num,
        ).await
    }
}

// Server is the main struct of the server
// data retrieval and storage are done through the server
// this process can divide into different layers
// 1. transport layer: handle the connection
// 2. protocol layer: parse the command from the stream
// 3. command layer: execute the command
// 4. storage layer: store the data
// 5. response layer: represent the response
// 6. protocol layer: serialize the response
// 7. transport layer: write the response to the stream
// the data flow is like this:
// client -> transport -> protocol -> request  -> storage
//                                                 |
//                                                 v
// client <- transport <- protocol <- response <- storage
pub struct Server {
    db: Database,
    shutdown: bool,
    listener: TcpListener,
    sockets: HashMap<usize, Arc<AsyncConnection>>,
    wait_duration: Duration,
    command_parser: Parser,
    worker_pool: worker::WorkerPool,
    db_handler: DBHandler,
}

impl Server {
    pub async fn new(
        addr: &str,
        port: u16,
        max_client: usize,
        worker_thread_num: usize,
    ) -> Result<Self, RedisErr> {
        let addr: std::net::SocketAddr = format!("{}:{}", addr, port).parse()?;
        let db = Database::new();
        let mut listener = tokio::net::TcpListener::bind(addr).await?;
        

        Ok(Self {
            db: db,
            shutdown: false,
            listener: listener,
            sockets: HashMap::new(),
            wait_duration: Duration::from_millis(100),
            command_parser: crate::cmd::Parser::new(),
            worker_pool: worker::WorkerPool::new(worker_thread_num)?,
            db_handler: DBHandler::new(),
        })
    }

    pub async  fn run(&mut self) -> Result<(), RedisErr> {
        // start the worker pool
        self.worker_pool.run().unwrap();
    
        // start the server pool
        

        // waitting for new connections
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    // handle new connections
                    // dispatch it to the worker
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn shutdown(&mut self) {
        self.shutdown = true;
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    struct TestStream {
        pub data: Vec<u8>,
        pub closed: bool,
    }

    impl From<Vec<u8>> for TestStream {
        fn from(data: Vec<u8>) -> Self {
            Self {
                data: data,
                closed: false,
            }
        }
    }

    impl std::io::Read for TestStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.closed == false {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Stream is closed",
                ));
            }
            let len = std::cmp::min(buf.len(), self.data.len());
            buf[..len].copy_from_slice(&self.data[..len]);
            self.data = self.data[len..].to_vec();
            Ok(len)
        }
    }

    impl std::io::Write for TestStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.data.extend_from_slice(buf);
            self.closed = true;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
