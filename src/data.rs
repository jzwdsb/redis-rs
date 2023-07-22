use std::collections::{HashMap, HashSet};

type Bytes = Vec<u8>;

#[derive(Clone, Debug)]
pub struct Z {
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
pub enum Value {
    Nil,
    KV(Bytes),
    List(Vec<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    ZSet(Vec<Z>),
}
