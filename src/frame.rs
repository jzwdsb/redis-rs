//! this module is used to parse the RESP protocol
//! RESP is defined as a protocol in the Redis documentation:
//! https://redis.io/docs/reference/protocol-spec/

use std::fmt::Display;

type Bytes = Vec<u8>;


#[derive(Debug, PartialEq, Clone)]
pub enum FrameError {
    Incomplete,
    Malformed,
}

impl Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Incomplete => write!(f, "Incomplete"),
            FrameError::Malformed => write!(f, "Malformed"),
        }
    }
}

impl From<std::string::FromUtf8Error> for FrameError {
    fn from(_: std::string::FromUtf8Error) -> Self {
        FrameError::Malformed
    }
}

impl From<std::num::ParseIntError> for FrameError {
    fn from(_: std::num::ParseIntError) -> Self {
        FrameError::Malformed
    }
}

impl FrameError {
    fn is_incomplete(&self) -> bool {
        match self {
            FrameError::Incomplete => true,
            _ => false,
        }
    }
}

// RESP protocol definition
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    SimpleString(String), // non binary safe string,format: `+OK\r\n`
    Errors(String),       // Error message returned by server,format: `-Error message\r\n`
    Integers(i64),        // Integers: format `:1000\r\n`
    BulkStrings(Bytes),   // Binary safe Strings `$6\r\nfoobar\r\n`
    Arrays(Vec<Frame>),   // array of RESP elements `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
}

const CRLF: &[u8] = b"\r\n";

impl Frame {
    pub fn from_bytes(data: &[u8]) -> Result<Frame, FrameError> {
        if data.len() == 0 {
            return Err(FrameError::Incomplete);
        }
        // may have data copy
        let frist_byte = data[0];
        let mut data = &data[1..];
        match frist_byte {
            // SimpleString +OK\r\n
            b'+' => {
                if !data.ends_with(CRLF) {
                    return Err(FrameError::Incomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let simple_string = String::from_utf8(data.to_vec()).unwrap();
                Ok(Frame::SimpleString(simple_string))
            }
            // Error -Error message\r\n
            b'-' => {
                if !data.ends_with(CRLF) {
                    return Err(FrameError::Incomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let error_string = String::from_utf8(data.to_vec()).unwrap();
                Ok(Frame::Errors(error_string))
            }
            // Number :1000\r\n
            b':' => {
                if !data.ends_with(CRLF) {
                    return Err(FrameError::Incomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let num = String::from_utf8(data.to_vec())?.parse()?;

                Ok(Frame::Integers(num))
            }
            // BulkString, binary safe, $6\r\nfoobar\r\n
            b'$' => {
                // find \r\n
                let index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(FrameError::Incomplete);
                }
                let index = index.unwrap();
                let num = String::from_utf8(data[..index].to_vec())?.parse()?;
                data = &data[index + 2..];
                // check if end with \r\n
                // find next \r\n
                let index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(FrameError::Incomplete);
                }
                let index = index.unwrap();
                if index != num {
                    return Err(FrameError::Malformed);
                }

                // remove \r\n
                let bulk_string = data[..num].to_vec();
                Ok(Frame::BulkStrings(bulk_string))
            }
            // Arrays *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            b'*' => {
                // find first \r\n and parse the number
                let index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(FrameError::Incomplete);
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
                Ok(Frame::Arrays(result))
            }
            _ => {
                return Err(FrameError::Malformed);
            }
        }
    }

    // return the length after serialize
    fn len(&self) -> usize {
        match self {
            // string len + '+' + '\r' + '\n'
            Frame::SimpleString(s) => s.len() + 3,
            // string len + '-' + '\r' + '\n'
            Frame::Errors(s) => s.len() + 3,
            // string len + ':' + '\r' + '\n'
            Frame::Integers(i) => i.to_string().len() + 3,
            // string len + '$' + '\r' + '\n' + string + '\r' + '\n'
            Frame::BulkStrings(s) => s.len() + 5 + s.len().to_string().len(),
            // frame len + '*' + '\r' + '\n' + frame + '\r' + '\n'
            Frame::Arrays(v) => {
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
            Frame::SimpleString(s) => {
                let mut result = String::new();
                result.push('+');
                result.push_str(&s);
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Frame::Errors(s) => {
                let mut result = String::new();
                result.push('-');
                result.push_str(&s);
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Frame::Integers(i) => {
                let mut result = String::new();
                result.push(':');
                result.push_str(&i.to_string());
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Frame::BulkStrings(s) => {
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
            Frame::Arrays(v) => {
                let mut result = String::new();
                result.push('*');
                result.push_str(&v.len().to_string());
                result.push('\r');
                result.push('\n');
                for protocol in v {
                    result.push_str(&String::from_utf8(protocol.serialize()).unwrap());
                }
                result.into_bytes()
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
            Frame::BulkStrings("SET a b".as_bytes().to_vec())
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
            Frame::Errors("ERR unknown command 'foobar'".to_string())
        );

        let data = ":1000\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(command.unwrap(), Frame::Integers(1000));

        let data = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Frame::Arrays(vec![
                Frame::BulkStrings("hello".as_bytes().to_vec()),
                Frame::BulkStrings("world".as_bytes().to_vec())
            ])
        );

        // bad case
        let data = "$7\r\nSET a ba\r\n".as_bytes();
        let command = Frame::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), false);
        assert_eq!(command.unwrap_err(), FrameError::Malformed);
    }
}
