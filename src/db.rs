use crate::data::Value;
use std::collections::HashMap;

type Bytes = Vec<u8>;

#[derive(Debug, Clone)]
pub struct DB {
    table: HashMap<Bytes, Value>,
}

impl DB {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: Bytes) -> Option<Value> {
        self.table.get(&key).cloned()
    }

    pub fn set(&mut self, key: Bytes, value: Bytes) {
        self.table.insert(key, Value::KV(value));
    }

    pub fn del(&mut self, key: Bytes) {
        self.table.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get() {
        let mut db = DB::new();
        db.set(b"key".to_vec(), b"value".to_vec());
        assert_eq!(db.get(b"key".to_vec()), Some(Value::KV(b"value".to_vec())));
    }

    #[test]
    fn test_set() {
        let mut db = DB::new();
        db.set(b"key".to_vec(), b"value".to_vec());
        assert_eq!(db.get(b"key".to_vec()), Some(Value::KV(b"value".to_vec())));
    }

    #[test]
    fn test_del() {
        let mut db = DB::new();
        db.set(b"key".to_vec(), b"value".to_vec());
        db.del(b"key".to_vec());
        assert_eq!(db.get(b"key".to_vec()), None);
    }
}
