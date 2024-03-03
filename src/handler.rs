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
use crate::{cmd, connection::AsyncConnection, db::DB};

use std::sync::Arc;

use log::trace;
use tokio::{net::TcpStream, sync::Notify};

pub struct Handler {
    db: DB,
    conn: AsyncConnection,
    shutdown: Arc<Notify>,
}

impl Handler {
    pub fn new(stream: TcpStream, db: DB, shutdown: Arc<Notify>) -> Handler {
        Handler {
            db,
            conn: AsyncConnection::new(stream),
            shutdown,
        }
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        let parser = cmd::Parser::new();
        loop {
            tokio::select! {
                frame = self.conn.read_frame() => {
                    let frame = frame?;

                    let cmd = parser.parse(frame);
                    if cmd.is_err() {
                        self.conn.write_frame(crate::frame::Frame::Error(cmd.unwrap_err().to_string())).await?;
                        continue;
                    }
                    let cmd = cmd.unwrap();
                    trace!("parsed command {:?}", cmd);
                    // normally, the apply function would return a frame
                    // and we should write that frame to the client
                    // but subscribe would block the thread and never return
                    // until the connection is unsubscribed
                    let resp = cmd.apply(&mut self.db, &mut self.conn, self.shutdown.clone()).await;
                    trace!("command response {:?}", resp);
                    self.conn.write_frame(resp).await?;
                }
                _ = self.shutdown.notified() => {
                    return Ok(())
                }
            }
        }
    }
}
