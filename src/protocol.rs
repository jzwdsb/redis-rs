// parse the redis protocol


use std::io::{Read, Write};
use crate::err::Err;

type Bytes = Vec<u8>;

pub trait ProtocolDecoder {
    fn from_stream(&self, stream: &mut impl Read) -> Result<Protocol, Err>;
}

pub trait ProtocolEncoder {
    fn to_stream(&self, stream: &mut impl Write) -> Result<(), Err>;
}


// RESP protocol
#[derive(Debug, PartialEq)]
pub enum Protocol {
    SimpleString(String),  // non binary safe string,format: `+OK\r\n`
    Errors(String),        // Error message returned by server,format: `-Error message\r\n`
    Integers(i64),         // Integers: format `:1000\r\n`
    BulkStrings(Bytes),    // Binary safe Strings `$6\r\nfoobar\r\n`
    Arrays(Vec<Protocol>), // array of RESP elements `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
}

impl Protocol {
    pub fn from_stream(stream: &mut impl Read) -> Result<Protocol, Err> {
        let frist_byte = read_byte(stream);
        match frist_byte {
            Ok(c) => match c {
                b'+' => {
                    let rest_bytes = read_from_stream_until(stream, &[b'\r', b'\n'])?;
                    let rest_bytes = String::from_utf8(rest_bytes).unwrap();
                    Ok(Protocol::SimpleString(rest_bytes))
                }
                b'-' => {
                    let rest_bytes = read_from_stream_until(stream, &[b'\r', b'\n'])?;
                    let rest_bytes = String::from_utf8(rest_bytes).unwrap();
                    Ok(Protocol::Errors(rest_bytes))
                }
                b':' => {
                    let rest_bytes = read_from_stream_until(stream, &[b'\r', b'\n'])?;
                    let rest_bytes = String::from_utf8(rest_bytes).unwrap();
                    let rest_bytes = rest_bytes.parse::<i64>().unwrap();
                    Ok(Protocol::Integers(rest_bytes))
                }
                b'$' => {
                    let rest_bytes = read_from_stream_until(stream, &[b'\r', b'\n']).unwrap();
                    let rest_bytes = String::from_utf8(rest_bytes).unwrap();
                    let rest_bytes_cnt = rest_bytes.parse::<i64>().unwrap();
                    let rest_bytes = read_from_stream_until(stream, &[b'\r', b'\n']).unwrap();
                    if rest_bytes.len() as i64 != rest_bytes_cnt {
                        return Err(Err::SyntaxError);
                    }
                    Ok(Protocol::BulkStrings(rest_bytes))
                }
                b'*' => {
                    let rest_bytes = read_from_stream_until(stream, &[b'\r', b'\n'])?;
                    let rest_bytes = String::from_utf8(rest_bytes).unwrap();
                    let rest_bytes = rest_bytes.parse::<i64>().unwrap();
                    let mut protocols = Vec::new();
                    for _ in 0..rest_bytes {
                        let protocol = Protocol::from_stream(stream)?;
                        protocols.push(protocol);
                    }
                    Ok(Protocol::Arrays(protocols))
                }
                _ => {
                    return Err(Err::SyntaxError);
                }
            },
            Err(err) => {
                return Err(err);
            }
        }
    }

    // RESP protocol
    // server response is Simple Strings, the first byte of the reply is "+" followed by the string itself
    // `+OK\r\n`
    //
    pub fn serialize_response(&self) -> Vec<u8> {
        match self {
            Protocol::SimpleString(s) => {
                let mut result = String::new();
                result.push('+');
                result.push_str(s);
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Protocol::Errors(s) => {
                let mut result = String::new();
                result.push('-');
                result.push_str(s);
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Protocol::Integers(i) => {
                let mut result = String::new();
                result.push(':');
                result.push_str(&i.to_string());
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Protocol::BulkStrings(s) => {
                let mut result = String::new();
                result.push('$');
                result.push_str(&s.len().to_string());
                result.push('\r');
                result.push('\n');
                result.push_str(&String::from_utf8(s.clone()).unwrap());
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Protocol::Arrays(v) => {
                let mut result = String::new();
                result.push('*');
                result.push_str(&v.len().to_string());
                result.push('\r');
                result.push('\n');
                for protocol in v {
                    result.push_str(&String::from_utf8(protocol.serialize_response()).unwrap());
                }
                result.into_bytes()
            }
        }
    }
}

fn read_byte(stream: &mut impl Read) -> Result<u8, Err> {
    let mut buffer = [0; 1];
    match stream.read_exact(&mut buffer) {
        Ok(_) => Ok(buffer[0]),
        Err(e) => Err(Err::IOError(e.to_string())),
    }
}

fn read_from_stream_until(stream: &mut impl Read, delimiter: &[u8]) -> Result<Vec<u8>, Err> {
    let mut result = Vec::new();
    let mut buffer = [0; 1];
    let mut delimiter_index = 0;
    loop {
        match stream.read_exact(&mut buffer) {
            Ok(_) => {
                if buffer[0] == delimiter[delimiter_index] {
                    delimiter_index += 1;
                    if delimiter_index == delimiter.len() {
                        break;
                    }
                } else {
                    delimiter_index = 0;
                }
                result.push(buffer[0]);
            }
            Err(e) => {
                return Err(Err::IOError(e.to_string()));
            }
        }
    }

    result.truncate(result.len() - delimiter.len() + 1);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_byte() {
        let mut stream = "hello world".as_bytes();
        let byte = read_byte(&mut stream);
        assert_eq!(byte.is_ok(), true);
        assert_eq!(byte.unwrap(), b'h');
    }

    #[test]
    fn test_read_from_stream_until() {
        let mut stream = "hello world\r\n".as_bytes();
        let bytes = read_from_stream_until(&mut stream, &[b'\r', b'\n']);
        assert_eq!(bytes.is_ok(), true);
        assert_eq!(bytes.unwrap(), "hello world".as_bytes());
    }

    #[test]
    fn test_parse_request() {
        let mut stream = "$7\r\nSET a b\r\n".as_bytes();
        let command = Protocol::from_stream(&mut stream);
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Protocol::BulkStrings("SET a b".as_bytes().to_vec())
        );

        let mut stream = "+OK\r\n".as_bytes();
        let command = Protocol::from_stream(&mut stream);
        assert_eq!(command.is_ok(), true);
        assert_eq!(command.unwrap(), Protocol::SimpleString("OK".to_string()));

        let mut stream = "-ERR unknown command 'foobar'\r\n".as_bytes();
        let command = Protocol::from_stream(&mut stream);
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Protocol::Errors("ERR unknown command 'foobar'".to_string())
        );

        let mut stream = ":1000\r\n".as_bytes();
        let command = Protocol::from_stream(&mut stream);
        assert_eq!(command.is_ok(), true);
        assert_eq!(command.unwrap(), Protocol::Integers(1000));

        let mut stream = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let command = Protocol::from_stream(&mut stream);
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Protocol::Arrays(vec![
                Protocol::BulkStrings("hello".as_bytes().to_vec()),
                Protocol::BulkStrings("world".as_bytes().to_vec())
            ])
        );
    }
}
