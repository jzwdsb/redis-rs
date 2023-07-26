//! helper functions in this crate

use std::io::{ErrorKind, Read, Write, Error};
use mio::net::TcpStream;
type Bytes = Vec<u8>;



pub(crate) fn read_request(stream: &mut TcpStream) -> Result<Bytes, Error> {
    let mut buffer = [0; 1024];
    let mut req_bytes = Vec::new();
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                return Err(Error::new(ErrorKind::ConnectionAborted, "connection aborted"));
            }
            Ok(n) => {
                req_bytes.extend_from_slice(&buffer[0..n]);
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(req_bytes)
}

pub(crate) fn write_response(stream: &mut TcpStream, response: &[u8]) -> Result<usize, Error> {
    let mut response = response;
    let mut cnt = 0;
    loop {
        match stream.write(&response) {
            Ok(len)=> {
                response = &response[len..]; 
                cnt += len;
                if response.len() == 0 {
                    break;
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(cnt)
}