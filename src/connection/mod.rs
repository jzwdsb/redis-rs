use std::fmt::Debug;
use std::io::{Read, Write};

mod connection;

pub trait SyncConnectionLike: Read + Write + Debug {}
impl SyncConnectionLike for std::net::TcpStream {}

pub use connection::AsyncConnection;
pub use connection::SyncConnection;
