//! Database module


use crate::{
    value::Value,
    RedisErr, Result,
};

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use log::{debug, trace};
use tokio::sync::{broadcast, Notify};

pub struct DBDropGuard {
    db: DB,
}

impl DBDropGuard {
    pub fn new() -> Self {
        Self { db: DB::new() }
    }

    pub fn db(&self) -> DB {
        self.db.clone()
    }
}

impl Drop for DBDropGuard {
    fn drop(&mut self) {
        self.db.shutdown_purge_task();
    }
}

#[derive(Debug, Clone)]
pub struct DB {
    db: Arc<Shared>,
}

impl DB {
    pub fn new() -> Self {
        let shard = Arc::new(Shared {
            state: Mutex::new(State::new()),
            background_task: Notify::new(),
        });

        // spawn a background task to purge expired keys
        tokio::spawn(purge_expired_tasks(shard.clone()));

        Self { db: shard }
    }

    pub fn get(&mut self, key: &str) -> Result<Bytes> {
        trace!("Get key: {}", key);
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get(key);
        match entry {
            Some(entry) => {
                // check expire on read
                if let Some(expire_at) = entry.expire_at {
                    if expire_at < Instant::now() {
                        state.table.remove(key);
                        state.expire_table.remove(&(key.to_string(), expire_at));
                        return Err(RedisErr::KeyNotFound);
                    }
                }
                match &entry.value {
                    Value::KV(v) => Ok(v.clone()),
                    _ => Err(RedisErr::WrongType),
                }
            }
            None => Err(RedisErr::KeyNotFound),
        }
    }

    pub fn set(
        &mut self,
        key: String,
        value: Bytes,
        nx: bool,
        xx: bool,
        get: bool,
        keepttl: bool,
        expire_at: Option<Instant>,
    ) -> Result<Option<Bytes>> {
        trace!(
            "Set key: {}, value: {:?}, nx: {}, xx: {}, get: {}, keepttl: {}, expire_at: {:?}",
            key,
            value,
            nx,
            xx,
            get,
            keepttl,
            expire_at
        );

        let mut state = self.db.state.lock().unwrap();
        let mut entry = Entry::new(Value::KV(value), expire_at);
        let old = state.table.get(&key);
        if nx && old.is_some() {
            return Err(RedisErr::NoAction);
        }
        if xx && old.is_none() {
            return Err(RedisErr::NoAction);
        }
        if keepttl && old.is_some() {
            entry.expire_at = old.unwrap().expire_at;
        }
        let mut notify = false;
        if let Some(expire_at) = entry.expire_at {
            notify = state
                .next_expire()
                .map(|next| next > expire_at)
                .unwrap_or(true);
        }

        let old = state.table.insert(key.clone(), entry);
        if get && old.is_some() {
            return Ok(Some(old.unwrap().value.to_kv().unwrap()));
        }
        if let Some(old) = old {
            if let Some(expire_at) = old.expire_at {
                state.expire_table.remove(&(key.clone(), expire_at));
            }
        }
        if let Some(expire_at) = expire_at {
            state.expire_table.insert((key, expire_at));
        }

        // drop the lock before notify the background task
        // avoid the background task to wait for the lock
        drop(state);
        if notify {
            self.db.background_task.notify_one();
        }
        Ok(None)
    }

    pub fn expire(&mut self, key: &str, expire_at: Instant) -> Result<()> {
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get_mut(key);
        match entry {
            Some(entry) => {
                entry.expire_at = Some(expire_at);
                state.expire_table.insert((key.to_string(), expire_at));
                Ok(())
            }
            None => Err(RedisErr::KeyNotFound),
        }
    }

    pub fn del(&mut self, key: &str) -> Option<Value> {
        self.remove(key)
    }

    pub fn lpush(&mut self, key: &str, values: Vec<Bytes>) -> Result<usize> {
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get_mut(key);
        let value_len = values.len();
        match entry {
            Some(entry) => {
                if !entry.value.is_list() {
                    return Err(RedisErr::WrongType);
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
                let entry = Entry::new(Value::List(list), None);
                state.table.insert(key.to_string(), entry);

                Ok(value_len)
            }
        }
    }

    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let state = self.db.state.lock().unwrap();
        let entry = state.table.get(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_list() {
                    return Err(RedisErr::WrongType);
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
            None => Err(RedisErr::KeyNotFound),
        }
    }

    pub fn hset(&mut self, key: String, field_values: Vec<(String, Bytes)>) -> Result<usize> {
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get_mut(&key);
        match entry {
            Some(entry) => {
                if !entry.value.is_hash() {
                    return Err(RedisErr::WrongType);
                }
                let mut value_len = 0;
                let map = entry.value.as_hash_mut().unwrap();
                for (field, value) in field_values {
                    map.insert(field, value);
                    value_len += 1;
                }
                Ok(value_len)
            }
            None => {
                let mut map = HashMap::new();
                let res = field_values.len();
                for (field, value) in field_values {
                    map.insert(field, value);
                }
                let entry = Entry::new(Value::Hash(map), None);
                state.table.insert(key, entry);
                Ok(res)
            }
        }
    }

    pub fn hget(&mut self, key: &str, field: &str) -> Result<Option<Bytes>> {
        let state = self.db.state.lock().unwrap();
        let entry = state.table.get(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_hash() {
                    return Err(RedisErr::WrongType);
                }
                let map = entry.value.as_hash_ref().unwrap();
                Ok(map.get(field).map(|v| v.clone()))
            }
            None => Err(RedisErr::KeyNotFound),
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
    ) -> Result<usize> {
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get_mut(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_zset() {
                    return Err(RedisErr::WrongType);
                }
                let mut value_len = 0;
                let value = entry.value.as_zset_mut().unwrap();
                for (score, member) in zset {
                    value_len += value.zadd(nx, xx, lt, gt, ch, incr, score, member);
                }
                Ok(value_len)
            }
            None => {
                let mut value = crate::value::ZSet::new();
                let value_len = zset.len();
                for (score, member) in zset {
                    value.zadd(nx, xx, lt, gt, ch, incr, score, member);
                }
                let entry = Entry::new(Value::ZSet(value), None);
                state.table.insert(key.to_string(), entry);
                Ok(value_len)
            }
        }
    }

    pub fn zcard(&mut self, key: &str) -> Result<usize> {
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get_mut(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_zset() {
                    return Err(RedisErr::WrongType);
                }
                Ok(entry.value.as_zset_ref().unwrap().len())
            }
            None => Err(RedisErr::KeyNotFound),
        }
    }

    pub fn zrem(&mut self, key: &str, members: Vec<Bytes>) -> Result<usize> {
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get_mut(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_zset() {
                    return Err(RedisErr::WrongType);
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
            None => Err(RedisErr::KeyNotFound),
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.db
            .state
            .lock()
            .unwrap()
            .table
            .remove(key)
            .map(|entry| entry.value)
    }

    pub fn get_type(&self, key: &str) -> Option<&'static str> {
        let state = self.db.state.lock().unwrap();
        let entry = state.table.get(key);
        match entry {
            Some(entry) => Some(entry.value.get_type().to_str()),
            None => None,
        }
    }

    pub fn flush(&mut self) {
        let mut state = self.db.state.lock().unwrap();
        state.table.clear();
        state.expire_table.clear();
    }

    pub fn subscribe(&self, channel: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;
        let mut state = self.db.state.lock().unwrap();
        match state.publisher.entry(channel.clone()) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(entry) => {
                trace!("subscribe to channel: {}", channel);
                let (tx, rx) = broadcast::channel(1024);
                entry.insert(tx);
                rx
            }
        }
    }

    pub fn publish(&self, channel: String, msg: Bytes) -> usize {
        let state = self.db.state.lock().unwrap();

        if let Some(tx) = state.publisher.get(&channel) {
            trace!(
                "publish message to channel: {}, msg: {}",
                channel,
                String::from_utf8_lossy(&msg.to_vec().as_slice())
            );
            tx.send(msg).unwrap()
        } else {
            0
        }
    }

    pub fn shutdown_purge_task(&self) {
        let mut state = self.db.state.lock().unwrap();

        state.shutdown = true;

        // drop the lock before notify the background task
        drop(state);

        // notify the background task to exit
        self.db.background_task.notify_one();
    }

    pub fn bf_add(&self, key: String, value: String) -> Result<()> {
        let mut state = self.db.state.lock().unwrap();
        let entry = state.table.get_mut(&key);
        match entry {
            Some(entry) => {
                if !entry.value.is_bloomfilter() {
                    return Err(RedisErr::WrongType);
                }
                let bloom = entry.value.as_bloomfilter_mut().unwrap();
                bloom.add(&value);
                Ok(())
            }
            None => {
                let mut bloom = crate::value::BloomFilter::new();
                bloom.add(&value);
                let entry = Entry::new(Value::BloomFilter(bloom), None);
                state.table.insert(key, entry);
                Ok(())
            }
        }
    }

    pub fn bf_exists(&self, key: &str, value: &str) -> Result<bool> {
        let state = self.db.state.lock().unwrap();
        let entry = state.table.get(key);
        match entry {
            Some(entry) => {
                if !entry.value.is_bloomfilter() {
                    return Err(RedisErr::WrongType);
                }
                let bloom = entry.value.as_bloomfilter_ref().unwrap();
                Ok(bloom.contains(&value))
            }
            None => Ok(false),
        }
    }

    pub fn object_info(&self, key: &str) -> Result<String> {
        let state = self.db.state.lock().unwrap();
        let entry = state.table.get(key);
        match entry {
            Some(entry) => Ok(format!("{:?}", entry.value)),
            None => Err(RedisErr::KeyNotFound),
        }
    }

    pub fn get_object_last_touch(&self, key: &str) -> Option<Instant> {
        let state = self.db.state.lock().unwrap();
        let entry = state.table.get(key);
        match entry {
            Some(entry) => Some(entry.touch_at),
            None => None,
        }
    }
} // impl DB

#[derive(Debug)]
struct Shared {
    // guard the state by mutex
    state: Mutex<State>,

    background_task: Notify,
}

impl Shared {
    // purge all the expired keys and return the next expire time
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            return None;
        }

        let now = Instant::now();

        while let Some((key, instant)) = state.expire_table.iter().next().cloned() {
            if instant > now {
                return Some(instant);
            }

            state.expire_table.remove(&(key.clone(), instant));

            state.table.remove(&key);
        }

        None
    }

    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
} // impl Shared

#[derive(Debug)]
pub struct Entry {
    value: Value,
    expire_at: Option<Instant>,
    touch_at: Instant,
}

impl Entry {
    #[allow(dead_code)]
    pub fn new(value: Value, expire_at: Option<Instant>) -> Self {
        Self {
            value,
            expire_at,
            touch_at: Instant::now(),
        }
    }

    #[allow(dead_code)]
    pub fn get_value(&self) -> &Value {
        &self.value
    }

    #[allow(dead_code)]
    pub fn get_touched_at(&self) -> Instant {
        self.touch_at
    }

    #[allow(dead_code)]
    pub fn set_touch_at(&mut self, touch_at: Instant) {
        self.touch_at = touch_at;
    }

    #[allow(dead_code)]
    pub fn get_expire_at(&self) -> Option<Instant> {
        self.expire_at
    }
} // impl Entry

/*
a second thought, Maybe it's not nescery to implment all the date manipulation method in the database layer.
There are duplicate code in the command layer and the database could only provide the basic data operate method.
*/

#[derive(Debug)]
struct State {
    table: HashMap<String, Entry>,

    // seperate key space for pub-sub
    publisher: HashMap<String, broadcast::Sender<Bytes>>,

    expire_table: BTreeSet<(String, Instant)>,

    shutdown: bool,
}

impl State {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
            publisher: HashMap::new(),
            expire_table: BTreeSet::new(),
            shutdown: false,
        }
    }

    pub fn next_expire(&self) -> Option<Instant> {
        self.expire_table.iter().next().map(|(_, instant)| *instant)
    }
}

async fn purge_expired_tasks(sharad: Arc<Shared>) {
    while sharad.is_shutdown() == false {
        if let Some(when) = sharad.purge_expired_keys() {
            tokio::select! {
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(when)) => {
                    // do nothing
                }
                _ = sharad.background_task.notified() => {
                    // do nothing
                }
            }
        } else {
            sharad.background_task.notified().await;
        }
    }

    debug!("purge expired task exit")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_get_set() {
        let key = "key".to_string();
        let val = Bytes::from_static(b"value");
        let mut db = DB::new();
        let res = db.set(key.clone(), val.clone(), false, false, false, false, None);
        assert_eq!(res, Ok(None));
        assert_eq!(db.get(&key), Ok(val.clone()));
        let res = db.set(key.clone(), val.clone(), false, true, false, false, None);
        assert_eq!(res, Ok(None));
        let res = db.set(key.clone(), val.clone(), true, false, false, false, None);
        assert_eq!(res, Err(RedisErr::NoAction));
        assert_eq!(
            db.db
                .state
                .lock()
                .unwrap()
                .table
                .get(&key)
                .unwrap()
                .value
                .as_kv_ref()
                .unwrap()
                .clone(),
            val.clone()
        );

        let res = db.set(
            key.clone(),
            Bytes::from_static(b"new_val"),
            false,
            false,
            true,
            false,
            None,
        );
        assert_eq!(res, Ok(Some(val.clone())));
        assert_eq!(
            db.db
                .state
                .lock()
                .unwrap()
                .table
                .get(&key)
                .unwrap()
                .value
                .as_kv_ref()
                .unwrap()
                .clone(),
            Bytes::from_static(b"new_val")
        );

        let res = db.set(
            key.clone(),
            Bytes::from_static(b"new_val"),
            false,
            false,
            false,
            false,
            Some(Instant::now() + Duration::from_secs(60)),
        );
        assert_eq!(res, Ok(None));
        assert_eq!(
            db.db
                .state
                .lock()
                .unwrap()
                .table
                .get(&key)
                .unwrap()
                .expire_at
                .is_some(),
            true
        );

        let _res = db.set(key.clone(), val.clone(), false, false, false, false, None);
        assert_eq!(
            db.db
                .state
                .lock()
                .unwrap()
                .table
                .get(&key)
                .unwrap()
                .expire_at
                .is_some(),
            false
        );
        db.db
            .state
            .lock()
            .unwrap()
            .table
            .get_mut(&key)
            .unwrap()
            .expire_at = Some(Instant::now() + Duration::from_secs(60));
        let res = db.set(key.clone(), val.clone(), false, false, false, true, None);
        assert_eq!(res, Ok(None));
        assert_eq!(
            db.db
                .state
                .lock()
                .unwrap()
                .table
                .get(&key)
                .unwrap()
                .expire_at
                .is_some(),
            true
        );
    }

    #[test]
    fn test_del() {
        let key = "key".to_string();
        let val = Bytes::from_static(b"value");
        let mut db = DB::new();
        let res = db.set(key.clone(), val, false, false, false, false, None);
        assert_eq!(res, Ok(None));

        db.del(&key);
        assert_eq!(db.get(&key), Err(RedisErr::KeyNotFound));
    }

    #[test]
    fn test_expire() {
        let key = "key".to_string();
        let val = Bytes::from_static(b"value");
        let expire_from_now = Duration::from_secs(10);
        let mut db = DB::new();
        let res = db.set(
            key.clone(),
            val.clone(),
            false,
            false,
            false,
            false,
            Some(Instant::now() + expire_from_now),
        );
        assert_eq!(res, Ok(None));
        assert_eq!(db.get(&key), Ok(val));
        std::thread::sleep(expire_from_now);
        assert_eq!(db.get(&key), Err(RedisErr::KeyNotFound));
    }

    #[test]
    fn test_zadd() {
        let key = "key".to_string();
        let mut db = DB::new();
        let res = db.zadd(
            &key,
            true,
            false,
            false,
            false,
            false,
            false,
            vec![(1.0, Bytes::from_static(b"one"))],
        );
        println!(
            "{}",
            db.db.state.lock().unwrap().table.get(&key).unwrap().value
        );
        assert_eq!(res, Ok(1));

        let res = db.zadd(
            &key,
            true,
            false,
            false,
            false,
            false,
            false,
            vec![
                (2.0, Bytes::from_static(b"one")),
                (2.0, Bytes::from_static(b"two")),
            ],
        );
        println!(
            "{}",
            db.db.state.lock().unwrap().table.get(&key).unwrap().value
        );
        assert_eq!(res, Ok(1));

        let res = db.zadd(
            &key,
            false,
            true,
            false,
            false,
            true,
            false,
            vec![
                (3.0, Bytes::from_static(b"two")),
                (3.0, Bytes::from_static(b"three")),
            ],
        );

        println!(
            "{}",
            db.db.state.lock().unwrap().table.get(&key).unwrap().value
        );
        assert_eq!(res, Ok(1));

        let res = db.zadd(
            &key,
            false,
            false,
            true,
            false,
            true,
            false,
            vec![
                (1.0, Bytes::from_static(b"two")),
                (3.0, Bytes::from_static(b"three")),
            ],
        );
        println!(
            "{}",
            db.db.state.lock().unwrap().table.get(&key).unwrap().value
        );
        assert_eq!(res, Ok(2));

        let res = db.zadd(
            &key,
            false,
            false,
            false,
            true,
            true,
            false,
            vec![
                (2.0, Bytes::from_static(b"two")),
                (4.0, Bytes::from_static(b"four")),
            ],
        );
        println!(
            "{}",
            db.db.state.lock().unwrap().table.get(&key).unwrap().value
        );
        assert_eq!(res, Ok(2));

        let res = db.zadd(
            &key,
            false,
            false,
            false,
            false,
            true,
            true,
            vec![
                (1.0, Bytes::from_static(b"one")),
                (5.0, Bytes::from_static(b"five")),
            ],
        );
        println!(
            "{}",
            db.db.state.lock().unwrap().table.get(&key).unwrap().value
        );
        assert_eq!(res, Ok(2));
    }
}
