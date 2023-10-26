//! https://rdb.fnordig.de/file_format.html#high-level-algorithm-to-parse-rdb
//! follow official document to implement rdb parser and writer

use std::{collections::HashMap, io::Write};

use crate::value::Value;

pub struct RDB {
    dbs: Vec<HashMap<String, Value>>,
    expire_table: HashMap<String, u64>,
    version: u32,
    aux_fields: Vec<(String, String)>,
    file: std::fs::File,
}

impl RDB {
    pub fn new() -> Self {
        Self {
            version: 0,
            aux_fields: Vec::new(),
            dbs: Vec::new(),
            expire_table: HashMap::new(),
            file: std::fs::File::create("dump.rdb").unwrap(),
        }
    }
    pub fn load(&mut self, path: &str) {}

    pub fn write(&mut self, path: &str) {
        let file = std::fs::File::create(path).unwrap();
        let mut writer: Box<dyn Write> = Box::new(file);
        self.write_header(&mut writer);
        self.write_content(&mut writer);
        self.write_footer(&mut writer);
    }

    /*
    ----------------------------#
    52 45 44 49 53              # Magic String "REDIS"
    30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3
    ----------------------------
    FA                          # Auxiliary field
    $string-encoded-key         # May contain arbitrary metadata
    $string-encoded-value       # such as Redis version, creation time, used memory, ...
    ----------------------------
    FE 00                       # Indicates database selector. db number = 00
    FB                          # Indicates a resizedb field
    $length-encoded-int         # Size of the corresponding hash table
    $length-encoded-int         # Size of the corresponding expire hash table
    */

    fn write_header(&mut self, writer: &mut Box<dyn Write>) {
        writer.write(b"REDIS").unwrap();
        writer.write(b"0003").unwrap();
        writer.write(b"FA").unwrap();
        for (key, value) in self.aux_fields.iter() {
            writer.write(key.as_bytes()).unwrap();
            writer.write(value.as_bytes()).unwrap();
        }
    }

    /*
    ----------------------------# Key-Value pair starts
    FD $unsigned-int            # "expiry time in seconds", followed by 4 byte unsigned int
    $value-type                 # 1 byte flag indicating the type of value
    $string-encoded-key         # The key, encoded as a redis string
    $encoded-value              # The value, encoding depends on $value-type
    ----------------------------
    */

    fn write_content(&mut self, writer: &mut Box<dyn Write>) {
        for (idx, value) in self.dbs.iter().enumerate() {
            Self::write_db(idx, value, writer);
        }
    }

    fn write_db(index: usize, db: &HashMap<String, Value>, writer: &mut Box<dyn Write>) {
        writer.write(b"FE").unwrap();
        writer.write(b"00").unwrap();
        writer.write(b"FB").unwrap();
        for (key, value) in db.iter() {
            Self::write_key_value_pair(key, value, writer)
        }
    }

    fn write_key_value_pair(key: &str, value: &Value, writer: &mut Box<dyn Write>) {
        Self::write_key(key, writer);
        Self::write_value(value, writer);
    }

    fn write_key(key: &str, writer: &mut Box<dyn Write>) {
        writer.write(key.as_bytes()).unwrap();
    }

    fn write_value(value: &Value, writer: &mut Box<dyn Write>) {
        todo!("write value")
    }

    fn write_footer(&mut self, writer: &mut Box<dyn Write>) {
        writer.write(b"FF").unwrap();
        todo!("crc32 checksum");
    }
}

mod tests {
    use super::*;
}
