//! Redis Server implementation
//! use mio to achieve non-blocking IO, multiplexing and event driven
//! an event loop is used to handle all the IO events

use crate::db::Database;
use crate::RedisErr;
use crate::{connection::AsyncConnection, handler::Handler};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use tokio::sync::Semaphore;

pub struct ServerBuilder {
    addr: String,
    port: u16,
    max_client: usize,
}

impl ServerBuilder {
    pub fn new(/* ... */) -> Self {
        Self {
            addr: "0.0.0.0".to_string(),
            port: 6379,
            max_client: 1024,
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

    pub async fn build(self) -> Result<Server, RedisErr> {
        Server::new(&self.addr, self.port, self.max_client).await
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
    listener: TcpListener,
    sockets: HashMap<usize, Arc<AsyncConnection>>,
    limit_connections: Arc<Semaphore>,
    wait_duration: Duration,
}

impl Server {
    pub async fn new(addr: &str, port: u16, max_client: usize) -> Result<Self, RedisErr> {
        let addr: std::net::SocketAddr = format!("{}:{}", addr, port).parse()?;
        let db = Database::new();
        let listener = tokio::net::TcpListener::bind(addr).await?;

        Ok(Self {
            db: db,
            listener: listener,
            sockets: HashMap::new(),
            limit_connections: Arc::new(Semaphore::new(max_client)),
            wait_duration: Duration::from_millis(100),
        })
    }

    pub async fn run(&mut self) -> Result<(), RedisErr> {
        // waitting for new connections
        loop {
            let premit = match self.limit_connections.clone().acquire_owned().await {
                Ok(premit) => premit,
                Err(e) => {
                    error!("Error acquiring premit: {}", e);
                    continue;
                }
            };
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    trace!("Accepting connection from: {}", addr);
                    let handler = Handler::new(stream);
                    tokio::spawn(async move {
                        if let Err(err) = handler.run().await {
                            error!("Error handling connection: {}", err);
                        }
                        drop(premit)
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }
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
