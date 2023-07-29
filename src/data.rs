//! Redis Data Definition
//! All Redis data types are defined in this document: https://redis.io/topics/data-types-intro
//! We start implementing the most common data types: String, List, Set, Hash, ZSet


use std::collections::{HashMap, HashSet, VecDeque};

type Bytes = Vec<u8>;

#[derive(Clone, Debug)]
pub(crate) struct Z {
    member: Bytes,
    score: f64,
}
impl PartialEq for Z {
    fn eq(&self, other: &Self) -> bool {
        self.member == other.member
    }
}

impl PartialOrd for Z {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl Eq for Z {}

impl Ord for Z {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.partial_cmp(&other.score).unwrap()
    }
}

#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub(crate) enum Value {
    Nil,
    KV(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    ZSet(Vec<Z>),
}

impl Value {
    #[allow(dead_code)]
    pub fn is_nil (&self) -> bool {
        match self {
            Value::Nil => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_kv (&self) -> bool {
        match self {
            Value::KV(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_list (&self) -> bool {
        match self {
            Value::List(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_set (&self) -> bool {
        match self {
            Value::Set(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_hash (&self) -> bool {
        match self {
            Value::Hash(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn is_zset (&self) -> bool {
        match self {
            Value::ZSet(_) => true,
            _ => false,
        }
    }
}
