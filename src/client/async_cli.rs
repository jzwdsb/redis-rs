


use std::error::Error;

use tokio::net::TcpStream;

use crate::frame::Frame;


// use tokio as asynchronous runtime
struct AsyncConnection {
    stream: TcpStream,
}


// TODO: implement AsyncConnection
impl AsyncConnection {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    async fn read_frame(&mut self) -> Result<Frame, Box<dyn Error>> {
        todo!("read_frame")
    }

    async fn write_frame(&mut self, frame: Frame) -> Result<(), Box<dyn Error>> {
        todo!("write_frame")
    }
}




pub struct AsyncClient {
    conn: AsyncConnection,
}


impl AsyncClient {
    fn new(conn: AsyncConnection) -> Self {
        Self { conn }
    }

    fn open(addr: &str) -> Result<Self, Box<dyn Error>> {
        todo!("open")
    }

    async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        todo!("get")
    }

    async fn set(&mut self, key: &str, value: &str) -> Result<(), Box<dyn Error>> {
        todo!("set")
    }    
}