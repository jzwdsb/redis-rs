//! Redis Server implementation
//! use mio to achieve non-blocking IO, multiplexing and event driven
//! an event loop is used to handle all the IO events

use crate::db::DBDropGuard;
use crate::handler::Handler;
use crate::Arg;
use crate::Result;

use std::sync::Arc;
use std::time::Duration;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore};

pub struct ServerBuilder {
    addr: String,
    port: u16,
    max_client: usize,
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self {
            addr: "127.0.0.1".to_string(),
            port: 6379,
            max_client: 1024,
        }
    }

    pub fn new_with_arg(args: Arg) -> Self {
        Self {
            addr: args.get_host(),
            port: args.get_port(),
            max_client: args.get_max_clients(),
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

    pub async fn build(self) -> Result<Server> {
        Server::new(&self.addr, self.port, self.max_client).await
    }
} // impl ServerBuilder

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
    db: DBDropGuard,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>, // limit the max connections
    shutdown: Arc<Notify>,
    wait_duration: Duration,
}

impl Server {
    pub async fn new(addr: &str, port: u16, max_client: usize) -> Result<Self> {
        let addr: std::net::SocketAddr = format!("{}:{}", addr, port).parse()?;
        let db = DBDropGuard::new();
        let listener = tokio::net::TcpListener::bind(addr).await?;

        Ok(Self {
            db: db,
            listener: listener,
            limit_connections: Arc::new(Semaphore::new(max_client)),
            shutdown: Arc::new(Notify::new()),
            wait_duration: Duration::from_millis(100),
        })
    }

    // run the server
    pub async fn run(self) -> Result<()> {
        // waitting for new connections
        loop {
            let premit = match self.limit_connections.clone().acquire_owned().await {
                Ok(premit) => premit,
                Err(e) => {
                    error!("Error acquiring premit: {}", e);
                    continue;
                }
            };
            tokio::select! {
                Ok((stream, addr)) = self.listener.accept() => {
                    trace!("Accepting connection from: {}", addr);

                    let mut handler = Handler::new(stream, self.db.db(), self.shutdown.clone());
                    tokio::spawn(async move {
                        if let Err(err) = handler.run().await {
                            error!("Error handling connection: {}", err);
                        }
                        drop(premit)
                    });
                }
                Err(e) = self.listener.accept() => {
                    error!("Error accepting connection: {}", e);
                }
                // Ctrl-C to shutdown
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl-C received, shutting down");
                    self.shutdown.notify_waiters();
                    self.limit_connections.acquire_owned().await.expect("already closed").forget();
                    return Ok(())
                }
                // may notified by a shutdown command from worker threads
                _ = self.shutdown.notified() => {
                    info!("Shutdown complete");
                    return Ok(())
                }
            }
        }
    }
} // impl Server

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;

    struct TestStream {
        pub data: Bytes,
        pub closed: bool,
    }

    impl From<Bytes> for TestStream {
        fn from(data: Bytes) -> Self {
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
            self.data = self.data.split_off(len);
            Ok(len)
        }
    }

    impl std::io::Write for TestStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.closed {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Stream is closed",
                ));
            }
            self.data = Bytes::copy_from_slice(buf);
            self.closed = true;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
