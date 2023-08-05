use log::{debug, trace};

use crate::data::Value;

use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

type Bytes = Vec<u8>;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ExecuteError {
    WrongType,
    KeyNotFound,
    OutOfMemory,
}

#[derive(Debug)]
pub struct Entry {
    value: Value,
    expire_at: Option<Instant>,
}

#[derive(Debug)]
pub struct Database {
    pub table: HashMap<String, Entry>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &str) -> Result<Bytes, ExecuteError> {
        trace!("Get key: {}", key);
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
        key: String,
        value: Bytes,
        expire_at: Option<Instant>,
    ) -> Result<(), ExecuteError> {
        trace!(
            "Set key: {}, value: {:?}, expire_at: {:?}",
            key,
            value,
            expire_at
        );
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

    pub fn expire(&mut self, key: &str, expire_at: Instant) -> Result<(), ExecuteError> {
        let entry = self.table.get_mut(key);
        match entry {
            Some(entry) => {
                entry.expire_at = Some(expire_at);
                Ok(())
            }
            None => Err(ExecuteError::KeyNotFound),
        }
    }

    pub fn del(&mut self, key: &str) -> Option<Value> {
        trace!("Del key: {}", key);
        self.table.remove(key).map(|entry| entry.value)
    }

    pub fn lpush(&mut self, key: &str, values: Vec<Bytes>) -> Result<usize, ExecuteError> {
        let entry = self.table.get_mut(key);
        let value_len = values.len();
        match entry {
            Some(entry) => {
                if !entry.value.is_list() {
                    return Err(ExecuteError::WrongType);
                }
                let mut after_len = 0;
                if let Value::List(list) = &mut entry.value {
                    for v in values {
                        list.push_front(v);
                    }
                    after_len = list.len();
                }
                Ok(after_len)
            }
            None => {
                let mut list = VecDeque::new();
                list.extend(values);
                let entry = Entry {
                    value: Value::List(list),
                    expire_at: None,
                };
                self.table.insert(key.to_string(), entry);
                Ok(value_len)
            }
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, ExecuteError> {
        let entry = self.table.get(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_list() {
                    return Err(ExecuteError::WrongType);
                }
                let mut res = Vec::new();
                if let Value::List(list) = &entry.value {
                    let len = list.len() as i64;
                    let start = if start < 0 { len + start } else { start };
                    let stop = if stop < 0 { len + stop } else { stop };
                    let start = if start < 0 { 0 } else { start };
                    let stop = if stop < 0 { 0 } else { stop };
                    let start = start as usize;
                    let stop = stop as usize;
                    if start > stop {
                        return Ok(res);
                    }
                    if start >= list.len() {
                        return Ok(res);
                    }
                    let stop = if stop >= list.len() {
                        list.len() - 1
                    } else {
                        stop
                    };

                    list.iter()
                        .skip(start as usize)
                        .take((stop - start) as usize)
                        .for_each(|v| {
                            res.push(v.clone());
                        });
                }
                Ok(res)
            }
            None => Err(ExecuteError::KeyNotFound),
        }
    }

    pub fn zadd(
        &mut self,
        key: &str,
        nx: bool,
        xx: bool,
        lt: bool,
        gt: bool,
        ch: bool,
        incr: bool,
        zset: Vec<(f64, Bytes)>,
    ) -> Result<usize, ExecuteError> {
        let entry = self.table.get_mut(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_zset() {
                    return Err(ExecuteError::WrongType);
                }
                let mut value_len = 0;
                let value = entry.value.as_zset_mut().unwrap();
                for (score, member) in zset {
                    value_len += value.zadd(nx, xx, lt, gt, ch, incr, score, member);
                }
                Ok(value_len)
            }
            None => {
                let mut value = crate::data::ZSet::new();
                let value_len = zset.len();
                for (score, member) in zset {
                    value.zadd(nx, xx, lt, gt, ch, incr, score, member);
                }
                let entry = Entry {
                    value: Value::ZSet(value),
                    expire_at: None,
                };
                self.table.insert(key.to_string(), entry);
                Ok(value_len)
            }
        }
    }

    pub fn zcard(&self, key: &str) -> Result<usize, ExecuteError> {
        let entry = self.table.get(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_zset() {
                    return Err(ExecuteError::WrongType);
                }

                Ok(entry.value.as_zset().unwrap().len())
            }
            None => Err(ExecuteError::KeyNotFound),
        }
    }

    pub fn zrem(&mut self, key: &str, members: Vec<Bytes>) -> Result<usize, ExecuteError> {
        let entry = self.table.get_mut(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_zset() {
                    return Err(ExecuteError::WrongType);
                }
                let mut value_len = 0;
                let value = entry.value.as_zset_mut().unwrap();
                for member in members {
                    if value.remove(&member) {
                        value_len += 1;
                    }
                }
                Ok(value_len)
            }
            None => Err(ExecuteError::KeyNotFound),
        }
    }

    pub fn flush(&mut self) {
        self.table.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, time::Duration};

    use super::*;

    #[test]
    fn test_get_set() {
        let key = "key".to_string();
        let val = b"value".to_vec();
        let mut db = Database::new();
        let res = db.set(key.clone(), val.clone(), None);
        assert_eq!(res, Ok(()));
        assert_eq!(db.get(&key), Ok(val.clone()));
        db.table.get_mut(&key).unwrap().value = Value::List(VecDeque::new());
        let res = db.set(key, val, None);
        assert_eq!(res, Err(ExecuteError::WrongType));
    }

    #[test]
    fn test_del() {
        let key = "key".to_string();
        let val = b"value".to_vec();
        let mut db = Database::new();
        let res = db.set(key.clone(), val, None);
        assert_eq!(res, Ok(()));

        db.del(&key);
        assert_eq!(db.get(&key), Err(ExecuteError::KeyNotFound));
    }

    #[test]
    fn test_expire() {
        let key = "key".to_string();
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

    #[test]
    fn test_zadd() {
        let key = "key".to_string();
        let mut db = Database::new();
        let res = db.zadd(
            &key,
            true,
            false,
            false,
            false,
            false,
            false,
            vec![(1.0, b"one".to_vec())],
        );
        debug!("{}", db.table.get(&key).unwrap().value);
        assert_eq!(res, Ok(1));

        let res = db.zadd(
            &key,
            true,
            false,
            false,
            false,
            false,
            false,
            vec![(2.0, b"one".to_vec()), (2.0, b"two".to_vec())],
        );
        debug!("{}", db.table.get(&key).unwrap().value);
        assert_eq!(res, Ok(1));

        let res = db.zadd(
            &key,
            false,
            true,
            false,
            false,
            true,
            false,
            vec![(3.0, b"two".to_vec()), (3.0, b"three".to_vec())],
        );

        debug!("{}", db.table.get(&key).unwrap().value);
        assert_eq!(res, Ok(1));

        let res = db.zadd(
            &key,
            false,
            false,
            true,
            false,
            true,
            false,
            vec![(1.0, b"two".to_vec()), (3.0, b"three".to_vec())],
        );
        debug!("{}", db.table.get(&key).unwrap().value);
        assert_eq!(res, Ok(2));

        let res = db.zadd(
            &key,
            false,
            false,
            false,
            true,
            true,
            false,
            vec![(2.0, b"two".to_vec()), (4.0, b"four".to_vec())],
        );
        debug!("{}", db.table.get(&key).unwrap().value);
        assert_eq!(res, Ok(2));

        let res = db.zadd(
            &key,
            false,
            false,
            false,
            false,
            true,
            true,
            vec![(1.0, b"one".to_vec()), (5.0, b"five".to_vec())],
        );
        debug!("{}", db.table.get(&key).unwrap().value);
        assert_eq!(res, Ok(2));
    }
}
