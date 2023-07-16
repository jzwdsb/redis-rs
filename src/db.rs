use crate::{data::Value, err::Err};
use std::{collections::HashMap, time::SystemTime};

type Bytes = Vec<u8>;

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expire_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct DB {
    table: HashMap<Bytes, Entry>,
}

impl DB {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    // only works on key-value type
    // other types will return WrongType error
    pub fn get(&mut self, key: &Bytes) -> Result<Value, Err> {
        let entry = self.table.get(key);
        match entry {
            Some(entry) => {
                // check expire on read
                if let Some(expire_at) = entry.expire_at {
                    if expire_at < SystemTime::now() {
                        self.table.remove(key);
                        return Ok(Value::Nil);
                    }
                }
                match &entry.value {
                    Value::KV(v) => Ok(Value::KV(v.clone())),
                    _ => Err(Err::WrongType),
                }
            }
            None => Ok(Value::Nil),
        }
    }

    pub fn set(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_at: Option<SystemTime>,
    ) -> Result<(), Err> {
        let entry = Entry {
            value: Value::KV(value),
            expire_at: expire_at,
        };
        if self.table.contains_key(&key) {
            let old_ent = self.table.get(&key).unwrap();
            // check whether the old value is a KV
            match old_ent.value {
                Value::KV(_) => {}
                _ => return Err(Err::WrongType),
            }
            self.table.insert(key, entry);
            return Ok(());
        }
        self.table.insert(key, entry);
        Ok(())
    }

    pub fn expire(&mut self, key: Bytes, expire_at: SystemTime) {
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
        let mut db = DB::new();
        let res = db.set(key.clone(), val.clone(), None);
        assert_eq!(res, Ok(()));
        assert_eq!(db.get(&key), Ok(Value::KV(val.clone())));
        db.table.get_mut(&key).unwrap().value = Value::List(vec![]);
        let res = db.set(key, val, None);
        assert_eq!(res, Err(Err::WrongType));
    }

    #[test]
    fn test_del() {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let mut db = DB::new();
        let res = db.set(key.clone(), val, None);
        assert_eq!(res, Ok(()));

        db.del(&key);
        assert_eq!(db.get(&key), Ok(Value::Nil));
    }

    #[test]
    fn test_expire() {
        let key = b"key".to_vec();
        let val = b"value".to_vec();
        let mut db = DB::new();
        let res = db.set(
            key.clone(),
            val.clone(),
            Some(SystemTime::now() + Duration::from_secs(1)),
        );
        assert_eq!(res, Ok(()));
        assert_eq!(db.get(&key), Ok(Value::KV(val)));
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(db.get(&key), Ok(Value::Nil));
    }
}
