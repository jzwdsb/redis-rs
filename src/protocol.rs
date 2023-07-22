// parse the redis protocol

type Bytes = Vec<u8>;

#[derive(Debug, PartialEq, Clone)]
pub enum ProtocolError {
    Incomplete,
    Malformed,
}

impl From<std::string::FromUtf8Error> for ProtocolError {
    fn from(_: std::string::FromUtf8Error) -> Self {
        ProtocolError::Malformed
    }
}

impl From<std::num::ParseIntError> for ProtocolError {
    fn from(_: std::num::ParseIntError) -> Self {
        ProtocolError::Malformed
    }
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


const CRLF: &[u8] = b"\r\n";

impl Protocol {
    pub fn from_bytes(data: &Bytes) -> Result<Protocol, ProtocolError> {
        if data.len() == 0 {
            return Err(ProtocolError::Incomplete);
        }
        // may have data copy
        let frist_byte = data[0];
        let mut data = &data[1..];
        match frist_byte {
            // +OK\r\n
            b'+' | b'-' => {
                if !data.ends_with(b"\r\n") {
                    return Err(ProtocolError::Incomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let simple_string = String::from_utf8(data.to_vec()).unwrap();
                Ok(Protocol::SimpleString(simple_string))
            }
            // :1000\r\n
            b':' => {
                if !data.ends_with(b"\r\n") {
                    return Err(ProtocolError::Incomplete);
                }
                // remove \r\n
                data = &data[..data.len() - 2];
                let num = String::from_utf8(data.to_vec())?.parse()?;

                Ok(Protocol::Integers(num))
            }
            // $6\r\nfoobar\r\n
            b'$' => {
                // find \r\n
                let mut index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(ProtocolError::Incomplete);
                }
                let index = index.unwrap();
                let num = String::from_utf8(data[..index].to_vec())?.parse()?;
                data = &data[index + 2..];
                // check if end with \r\n
                if data.len() != num + 2 {
                    return Err(ProtocolError::Incomplete);
                }
                if data[num] != b'\r' || data[num + 1] != b'\n' {
                    return Err(ProtocolError::Incomplete);
                }
                let bulk_string = data[..num].to_vec();
                Ok(Protocol::BulkStrings(bulk_string))
            }
            // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            b'*' => {
                // find \r\n
                let mut index = index_of(data, CRLF);
                if index.is_none() {
                    return Err(ProtocolError::Incomplete);
                }
                let index = index.unwrap();

                let num = String::from_utf8(data[..index].to_vec())?.parse()?;
                let mut result = Vec::new();
                for _ in 0..num {
                    data = &data[index + 2..];
                    let mut index = index_of(data, CRLF);
                    if index.is_none() {
                        return Err(ProtocolError::Incomplete);
                    }
                    let index = index.unwrap();
                    result.push(Protocol::from_bytes(&data[..index].to_vec())?);
                }
                Ok(Protocol::Arrays(result))
            }
            _ => {
                return Err(ProtocolError::Malformed);
            }
        }
    }

    // RESP protocol
    // server response is Simple Strings, the first byte of the reply is "+" followed by the string itself
    // `+OK\r\n`
    //
    pub fn serialize(self) -> Bytes {
        match self {
            Protocol::SimpleString(s) => {
                let mut result = String::new();
                result.push('+');
                result.push_str(&s);
                result.push('\r');
                result.push('\n');
                result.into_bytes()
            }
            Protocol::Errors(s) => {
                let mut result = String::new();
                result.push('-');
                result.push_str(&s);
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
                    result.push_str(&String::from_utf8(protocol.serialize()).unwrap());
                }
                result.into_bytes()
            }
        }
    }
}

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
        let mut data = "$7\r\nSET a b\r\n".as_bytes();
        let command = Protocol::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Protocol::BulkStrings("SET a b".as_bytes().to_vec())
        );

        let mut data = "+OK\r\n".as_bytes();
        let command = Protocol::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(command.unwrap(), Protocol::SimpleString("OK".to_string()));

        let mut data = "-ERR unknown command 'foobar'\r\n".as_bytes();
        let command = Protocol::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Protocol::Errors("ERR unknown command 'foobar'".to_string())
        );

        let mut data = ":1000\r\n".as_bytes();
        let command = Protocol::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(command.unwrap(), Protocol::Integers(1000));

        let mut data = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let command = Protocol::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), true);
        assert_eq!(
            command.unwrap(),
            Protocol::Arrays(vec![
                Protocol::BulkStrings("hello".as_bytes().to_vec()),
                Protocol::BulkStrings("world".as_bytes().to_vec())
            ])
        );

        // bad case
        let mut data = "$7\r\nSET a ba\r\n".as_bytes();
        let command = Protocol::from_bytes(&data.to_vec());
        assert_eq!(command.is_ok(), false);
        assert_eq!(command.unwrap_err(), ProtocolError::Malformed);
    }

}
