//! Redis Data Definition
//! All Redis data types are defined in this document: https://redis.io/topics/data-types-intro
//! We start implementing the most common data types: String, List, Set, Hash, ZSet

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::{Display, Formatter},
};

use marco::ValueDecorator;

use bloomfilter::Bloom;

type Bytes = Vec<u8>;

#[derive(Clone, Debug)]
pub struct Z {
    score: f64,
    member: Bytes,
}

impl PartialEq for Z {
    fn eq(&self, other: &Self) -> bool {
        self.member == other.member
    }
}

impl PartialOrd for Z {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl Eq for Z {}

impl Ord for Z {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.score.partial_cmp(&other.score) {
            Some(order) => match order {
                std::cmp::Ordering::Equal => self.member.cmp(&other.member),
                _ => order,
            },
            None => std::cmp::Ordering::Equal,
        }
    }
}

impl Display for Z {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.score,
            String::from_utf8_lossy(&self.member)
        )
    }
}

#[derive(Debug)]
pub struct ZSet {
    hmap: HashMap<Bytes, f64>,
    lists: skiplist::OrderedSkipList<Z>,
}

impl ZSet {
    pub fn new() -> Self {
        Self {
            hmap: HashMap::new(),
            lists: skiplist::OrderedSkipList::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.hmap.len()
    }

    pub fn zadd(
        &mut self,
        nx: bool,   // Only set the key if it does not already exist.
        xx: bool,   // Only set the key if it already exist.
        lt: bool,   // Only set the key if its score is less than the score of the key.
        gt: bool,   // Only set the key if its score is greater than the score of the key.
        ch: bool, // Modify the return value from the number of new elements added, to the total number of elements changed
        incr: bool, // When this option is specified ZADD acts like ZINCRBY
        score: f64,
        member: Bytes,
    ) -> usize {
        let z = Z { score, member };
        let contains = self.hmap.contains_key(&z.member);
        if nx && contains {
            return 0;
        }
        if xx && !contains {
            return 0;
        }

        if !contains {
            self.hmap.insert(z.member.clone(), z.score);
            self.lists.insert(z);
            return 1;
        }

        let value = self.hmap.get(&z.member).unwrap();
        if lt && z.score >= *value {
            return 0;
        }
        if gt && z.score <= *value {
            return 0;
        }

        // no value change
        if *value == z.score && !incr {
            return 0;
        }

        if incr {
            let score = self.hmap.get_mut(&z.member).unwrap();
            *score += z.score;
        } else {
            self.hmap.insert(z.member.clone(), z.score);
        }

        self.lists.remove(&z);
        self.lists.insert(z);

        if ch {
            return 1;
        }

        return 0;
    }

    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.hmap.remove(member) {
            let z = Z {
                score,
                member: member.clone(),
            };
            self.lists.remove(&z);
            return true;
        }
        false
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum ValueType {
    KV,
    List,
    Set,
    Hash,
    ZSet,
    BloomFilter,
}

impl ValueType {
    #[allow(dead_code)]
    pub fn to_str(&self) -> &'static str {
        match self {
            ValueType::KV => "string",
            ValueType::List => "list",
            ValueType::Set => "set",
            ValueType::Hash => "hash",
            ValueType::ZSet => "zset",
            ValueType::BloomFilter => "bloomfilter",
        }
    }
}

#[derive(Debug, ValueDecorator)]
#[allow(dead_code)]
pub enum Value {
    KV(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<String, Bytes>),
    ZSet(ZSet),

    BloomFilter(Bloom<String>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::KV(v) => write!(f, "{}", String::from_utf8_lossy(v)),
            Value::List(v) => {
                write!(f, "[")?;
                for (i, v) in v.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", String::from_utf8_lossy(v))?;
                }
                write!(f, "]")
            }
            Value::Set(v) => {
                write!(f, "{{")?;
                for (i, v) in v.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", String::from_utf8_lossy(v))?;
                }
                write!(f, "}}")
            }
            Value::Hash(v) => {
                write!(f, "{{")?;
                for (i, (k, v)) in v.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}:{}", k, String::from_utf8_lossy(v))?;
                }
                write!(f, "}}")
            }
            Value::ZSet(v) => {
                write!(f, "{{")?;
                for (i, (k, v)) in v.hmap.iter().enumerate() {
                    if i != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}:{}", String::from_utf8_lossy(k.clone().as_slice()), v)?;
                }
                write!(f, "}}")
            }
            Value::BloomFilter(_v) => {
                // TODO: implement this
                write!(f, "{{")?;
                write!(f, "}}")
            }
        }
    }
}
