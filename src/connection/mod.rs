use std::fmt::Debug;
use std::io::{Read, Write};

use mio::event::Source;

use crate::frame::Frame;
use crate::RedisErr;

mod connection;

pub trait AsyncConnectionLike: Source + Read + Write + Debug {}
impl AsyncConnectionLike for mio::net::TcpStream {}

pub trait SyncConnectionLike: Read + Write + Debug {}
impl SyncConnectionLike for std::net::TcpStream {}

pub trait FrameReader {
    fn read_frame(&mut self) -> Result<Frame, RedisErr>;
}

pub trait FrameWriter {
    fn write_frame(&mut self, frame: Frame) -> Result<(), RedisErr>;
}

pub use connection::AsyncConnection;
pub use connection::SyncConnection;