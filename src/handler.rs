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

use tokio::net::TcpStream;

use crate::connection::AsyncConnection;

// the worker is responsible for
// - handling a subset connections
// - parse the incoming bytes into commands
// - execute the commands
// - send the response back to the client
// - close the connection

// the worker respesents a single running thread
// the running context is stored in the worker
// since we need to share the worker between the main thread and the worker thread
// the worker needs to be thread safe, so we need to wrap it in an Arc<Mutex<Worker>>
pub struct Handler {
    conn: AsyncConnection
}

impl Handler {
    pub fn new(
        stream: TcpStream
    ) -> Handler {
        Handler {
            conn: AsyncConnection::new(stream),
        }
    }

    pub async fn run(&self) -> crate::Result<()> {
        Ok(())
    }
}

