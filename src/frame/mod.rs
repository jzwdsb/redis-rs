//! this module is used to parse the RESP protocol
//! RESP is defined as a protocol in the Redis documentation:
//! https://redis.io/docs/reference/protocol-spec/

use crate::RedisErr;

use std::fmt::Display;

type Bytes = Vec<u8>;

// RESP protocol definition
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Nil,                  // nil bulk string: `$-1\r\n`
    SimpleString(String), // non binary safe string,format: `+OK\r\n`
    Error(String),        // Error message returned by server,format: `-Error message\r\n`
    Integer(i64),         // Integers: format `:1000\r\n`
    BulkString(Bytes),    // Binary safe Strings `$6\r\nfoobar\r\n`
    Array(Vec<Frame>),    // array of RESP elements `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
}

const CRLF: &[u8] = b"\r\n";

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Nil => write!(f, "$-1\\r\\n"),
            Frame::SimpleString(s) => write!(f, "+{}\\r\\n", s),
            Frame::Error(s) => write!(f, "-{}\\r\\n", s),
            Frame::Integer(i) => write!(f, ":{}\\r\\n", i),
            Frame::BulkString(b) => {
                write!(f, "${}\\r\\n{}\\r\\n", b.len(), String::from_utf8_lossy(b))
            }
            Frame::Array(a) => {
                let mut s = String::new();
                s.push_str(&format!("*{}\\r\\n", a.len()));
                for frame in a {
                    s.push_str(&format!("{}", frame));
                }
                write!(f, "{}", s)
            }
        }
    }
}

impl Frame {
    pub fn from_bytes(data: &[u8]) -> Result<Frame, RedisErr> {
        if data.len() == 0 {
            return Err(RedisErr::FrameIncomplete);
        }

        let frist_byte = data[0];
        match frist_byte {
            // SimpleString +OK\r\n
            b'+' => {
                let mut data = &data[1..];
                if !data.ends_with(CRLF) {
                    return Err(RedisErr::FrameIncomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let simple_string = String::from_utf8(data.to_vec()).unwrap();
                Ok(Frame::SimpleString(simple_string))
            }
            // Error -Error message\r\n
            b'-' => {
                let mut data = &data[1..];
                if !data.ends_with(CRLF) {
                    return Err(RedisErr::FrameIncomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let error_string = String::from_utf8(data.to_vec()).unwrap();
                Ok(Frame::Error(error_string))
            }
            // Number :1000\r\n
            b':' => {
                let mut data = &data[1..];
                if !data.ends_with(CRLF) {
                    return Err(RedisErr::FrameIncomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let num = String::from_utf8(data.to_vec())?.parse()?;

                Ok(Frame::Integer(num))
            }
            // BulkString, binary safe, $6\r\nfoobar\r\n
            b'$' => {
                let mut data = &data[1..];
                // find \r\n
                let index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(RedisErr::FrameIncomplete);
                }
                let index = index.unwrap();
                let num = String::from_utf8(data[..index].to_vec())?.parse()?;
                data = &data[index + 2..];
                // check if end with \r\n
                // find next \r\n
                let index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(RedisErr::FrameIncomplete);
                }
                let index = index.unwrap();
                if index != num {
                    return Err(RedisErr::FrameMalformed);
                }

                // remove \r\n
                let bulk_string = data[..num].to_vec();
                Ok(Frame::BulkString(bulk_string))
            }
            // Arrays *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            b'*' => {
                let mut data = &data[1..];
                // find first \r\n and parse the number
                let index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(RedisErr::FrameIncomplete);
                }
                let index = index.unwrap();

                let num = String::from_utf8(data[..index].to_vec())?.parse()?;
                data = &data[index + 2..];
                let mut result = Vec::new();
                for _ in 0..num {
                    // remove \r\n
                    let frame = Frame::from_bytes(&data)?;
                    data = &data[frame.len()..];
                    result.push(frame)
                }
                Ok(Frame::Array(result))
            }
            // inline command, such as `set key value`
            // separated by space
            b if b.is_ascii_alphanumeric() => {
                let mut data = data;
                let index = index_of(data, CRLF);
                if let Some(idx) = index {
                    data = &data[..idx];
                }

                let s = String::from_utf8(data.to_vec())?;
                let mut result = Vec::new();
                for item in s.split(' ') {
                    // check simple string or integer
                    if item.len() == 0 {
                        continue;
                    }
                    match item.as_bytes()[0] {
                        n if n.is_ascii_digit() => {
                            let num = item.parse()?;
                            result.push(Frame::Integer(num));
                        }
                        c if c.is_ascii_alphabetic() => {
                            result.push(Frame::SimpleString(item.to_string()));
                        }
                        _ => {
                            return Err(RedisErr::FrameMalformed);
                        }
                    }
                }

                return Ok(Frame::Array(result));
            }
            _ => Err(RedisErr::FrameMalformed),
        }
    }

    // return the length after serialize
    pub fn len(&self) -> usize {
        match self {
            Frame::Nil => 5,
            // string len + '+' + '\r' + '\n'
            Frame::SimpleString(s) => s.len() + 3,
            // string len + '-' + '\r' + '\n'
            Frame::Error(s) => s.len() + 3,
            // string len + ':' + '\r' + '\n'
            Frame::Integer(i) => i.to_string().len() + 3,
            // string len + '$' + '\r' + '\n' + string + '\r' + '\n'
            Frame::BulkString(s) => s.len() + 5 + s.len().to_string().len(),
            // frame len + '*' + '\r' + '\n' + frame + '\r' + '\n'
            Frame::Array(v) => {
                let mut result = 0;
                for protocol in v {
                    result += protocol.len();
                }
                result + 3 + v.len().to_string().len()
            }
        }
    }

    // RESP protocol
    // server response is Simple Strings, the first byte of the reply is "+" followed by the string itself
    // `+OK\r\n`
    //
    pub fn serialize(self) -> Bytes {
        match self {
            Frame::Nil => {
                let mut result = Vec::<u8>::new();
                result.push(b'$');
                result.extend_from_slice(b"-1");
                result.extend_from_slice(b"\r\n");
                result
            }
            Frame::SimpleString(s) => {
                let mut result = Vec::<u8>::new();
                result.push(b'+');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            }
            Frame::Error(s) => {
                let mut result = Vec::<u8>::new();
                result.push(b'-');
                result.extend_from_slice(s.as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            }
            Frame::Integer(i) => {
                let mut result = Vec::<u8>::new();
                result.push(b':');
                result.extend_from_slice(i.to_string().as_bytes());
                result.extend_from_slice(b"\r\n");
                result
            }
            Frame::BulkString(s) => {
                let mut result = Vec::<u8>::new();
                result.push(b'$');
                result.extend(s.len().to_string().as_bytes());
                result.extend_from_slice(b"\r\n");
                result.extend_from_slice(&s);
                result.extend_from_slice(b"\r\n");
                result
            }
            Frame::Array(v) => {
                let mut result = Vec::<u8>::new();
                result.push(b'*');
                result.extend_from_slice(&v.len().to_string().into_bytes());
                result.extend_from_slice(b"\r\n");
                for protocol in v {
                    result.extend(protocol.serialize());
                }
                result
            }
        }
    }
}

#[inline]
fn index_of(data: &[u8], target: &[u8]) -> Option<usize> {
    for window in data.windows(target.len()) {
        if window == target {
            return Some(window.as_ptr() as usize - data.as_ptr() as usize);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_request() {
        let data = "$7\r\nSET a b\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Frame::BulkString("SET a b".as_bytes().to_vec())
        );

        let data = "+OK\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(command.unwrap(), Frame::SimpleString("OK".to_string()));

        let data = "-ERR unknown command 'foobar'\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Frame::Error("ERR unknown command 'foobar'".to_string())
        );

        let data = ":1000\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(command.unwrap(), Frame::Integer(1000));

        let data = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Frame::Array(vec![
                Frame::BulkString("hello".as_bytes().to_vec()),
                Frame::BulkString("world".as_bytes().to_vec())
            ])
        );

        // inline command
        let data = "SET a b 1".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Frame::Array(vec![
                Frame::SimpleString("SET".to_string()),
                Frame::SimpleString("a".to_string()),
                Frame::SimpleString("b".to_string()),
                Frame::Integer(1),
            ])
        );

        // bad case
        let data = "$7\r\nSET a ba\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), false);
        assert_eq!(command.unwrap_err(), RedisErr::FrameMalformed);
    }
}
