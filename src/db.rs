use crate::data::Value;

use std::{collections::HashMap, time::Instant};

type Bytes = Vec<u8>;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExecuteError {
    WrongType,
    KeyNotFound,
    OutOfMemory,
}

#[derive(Debug, Clone)]
pub struct Entry {
    value: Value,
    expire_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub(crate) struct Database {
    pub(crate) table: HashMap<Bytes, Entry>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &Bytes) -> Result<Bytes, ExecuteError> {
        let entry = self.table.get(key);
        match entry {
            Some(entry) => {
                // check expire on read
                if let Some(expire_at) = entry.expire_at {
                    if expire_at < Instant::now() {
                        self.table.remove(key);
                        return Err(ExecuteError::KeyNotFound);
                    }
                }
                match &entry.value {
                    Value::KV(v) => Ok(v.clone()),
                    _ => Err(ExecuteError::WrongType),
                }
            }
            None => Err(ExecuteError::KeyNotFound),
        }
    }

    pub fn set(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_at: Option<Instant>,
    ) -> Result<(), ExecuteError> {
        let entry = Entry {
            value: Value::KV(value),
            expire_at: expire_at,
        };
        if self.table.contains_key(&key) {
            let old_ent = self.table.get(&key).unwrap();
            if !old_ent.value.is_kv() {
                return Err(ExecuteError::WrongType);
            }
            self.table.insert(key, entry);
            return Ok(());
        }
        self.table.insert(key, entry);
        Ok(())
    }

    pub fn expire(&mut self, key: Bytes, expire_at: Instant) {
        let entry = self.table.get_mut(&key);
        match entry {
            Some(entry) => {
                entry.expire_at = Some(expire_at);
            }
            None => {}
        }
    }

    pub fn del(&mut self, key: &Bytes) {
        self.table.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_get_set() {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let mut db = Database::new();
        let res = db.set(key.clone(), val.clone(), None);
        assert_eq!(res, Ok(()));
        assert_eq!(db.get(&key), Ok(val.clone()));
        db.table.get_mut(&key).unwrap().value = Value::List(vec![]);
        let res = db.set(key, val, None);
        assert_eq!(res, Err(ExecuteError::WrongType));
    }

    #[test]
    fn test_del() {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let mut db = Database::new();
        let res = db.set(key.clone(), val, None);
        assert_eq!(res, Ok(()));

        db.del(&key);
        assert_eq!(db.get(&key), Err(ExecuteError::KeyNotFound));
    }

    #[test]
    fn test_expire() {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let mut db = Database::new();
        let res = db.set(
            key.clone(),
            val.clone(),
            Some(Instant::now() + Duration::from_secs(1)),
        );
        assert_eq!(res, Ok(()));
        assert_eq!(db.get(&key), Ok(val));
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(db.get(&key), Err(ExecuteError::KeyNotFound));
    }
}
