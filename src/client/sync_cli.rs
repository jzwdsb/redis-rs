//! This file includes the implementation of a Redis client SDK in a synchronous way.
//!
//! When using this SDK, the I/O operations of this SDK will block the current thread from running.
//! This SDK is still a work in progress.
//!
//! TODO: add documentation test for this module



use std::net::TcpStream;
// use mio::net::TcpStream;
use std::{error::Error, net::SocketAddr};

use crate::connection::{FrameReader, FrameWriter, SyncConnection};
use crate::frame::Frame;

pub struct SyncConn {
    conn: SyncConnection,
}

impl SyncConn {
    fn new(conn: SyncConnection) -> Self {
        Self { conn }
    }

    pub fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let req = build_cmd_frame("GET", &[key]);
        self.conn.write_frame(req)?;

        let resp = self.conn.read_frame()?;
        match resp {
            Frame::SimpleString(s) => Ok(s.as_bytes().to_vec()),
            Frame::BulkString(s) => Ok(s),
            _ => Err("Invalid response".into()),
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<(), Box<dyn Error>> {
        let req = build_cmd_frame("SET", &[key, value]);
        self.conn.write_frame(req)?;

        let resp = self.conn.read_frame()?;
        match resp {
            Frame::SimpleString(s) => {
                if s == "OK" {
                    Ok(())
                } else {
                    Err("Invalid response".into())
                }
            }
            _ => Err("Invalid response".into()),
        }
    }
}

pub struct BlockClient {
    conn: SyncConn,
}

impl BlockClient {
    fn new(conn: SyncConn) -> Self {
        Self { conn: conn }
    }

    pub fn open(addr: &str) -> Result<Self, Box<dyn Error>> {
        let conn = TcpStream::connect(addr.parse::<SocketAddr>()?)?;
        let stream = Box::new(conn);
        let conn = SyncConnection::new(1, stream);
        Ok(Self::new(SyncConn::new(conn)))
    }

    pub fn get_connection(&mut self) -> &mut SyncConn {
        &mut self.conn
    }
}

fn build_cmd_frame(cmd: &str, args: &[&str]) -> Frame {
    let mut frame = vec![];
    frame.push(Frame::BulkString(cmd.as_bytes().to_vec()));
    for arg in args {
        frame.push(Frame::BulkString(arg.as_bytes().to_vec()));
    }
    Frame::Array(frame)
}
