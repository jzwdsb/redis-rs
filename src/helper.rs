//! helper functions in this crate

type Bytes = Vec<u8>;

#[inline]
#[allow(dead_code)]
pub fn bytes_to_printable_string(bytes: &Bytes) -> String {
    let str = String::from_utf8_lossy(bytes);
    let escaped = str.replace("\r", "\\r").replace("\n", "\\n");

    escaped
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
