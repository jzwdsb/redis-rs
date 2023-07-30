//! helper functions in this crate

use mio::net::TcpStream;
use std::io::{Error, ErrorKind, Read, Write};
type Bytes = Vec<u8>;

#[inline]
pub fn bytes_to_printable_string(bytes: &Bytes) -> String {
    let str = String::from_utf8_lossy(bytes);
    let escaped = str.replace("\r", "\\r").replace("\n", "\\n");

    escaped
}

pub fn read_request(stream: &mut TcpStream) -> Result<Bytes, Error> {
    let mut buffer = [0; 1024];
    let mut req_bytes = Vec::new();
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::ConnectionAborted,
                    "connection aborted",
                ));
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

pub fn write_response(stream: &mut TcpStream, response: &[u8]) -> Result<usize, Error> {
    let mut response = response;
    let mut cnt = 0;
    loop {
        match stream.write(&response) {
            Ok(len) => {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_str() {
        let bytes = "*2\r\n$3\r\nGET\r\n$5\r\nHello\r\n".as_bytes().to_vec();
        let str = bytes_to_printable_string(&bytes);
        println!("{}", str);
        assert_eq!(str, "*2\\r\\n$3\\r\\nGET\\r\\n$5\\r\\nHello\\r\\n");
    }
}
