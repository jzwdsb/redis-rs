pub(crate) use std::collections::HashMap;

struct TrieNode<V> {
    children: HashMap<char, TrieNode<V>>,
    value: Option<V>,
    is_end_of_word: bool,
}


impl<V> TrieNode<V> {
    pub fn new(value: V) -> Self {
        TrieNode {
            children: HashMap::new(),
            value: Some(value),
            is_end_of_word: false,
        }
    }

    pub fn new_empty() -> Self {
        TrieNode {
            children: HashMap::new(),
            value: None,
            is_end_of_word: false,
        }
    }

    pub fn is_end_of_word(&self) -> bool {
        self.is_end_of_word
    }


}

pub struct Trie<V> where V: Clone {
    root: TrieNode<V>,
}

impl<V> Trie<V> where V: Clone {
    pub fn new() -> Self {
        Trie {
            root: TrieNode {
                children: HashMap::new(),
                value: None,
                is_end_of_word: false,
            }
        }
    }

    pub fn insert(&mut self, key: &str, value: V) {
        let mut node = &mut self.root;
        for c in key.chars() {
            node = node.children.entry(c).or_insert(TrieNode::new_empty());
        }
        node.value = Some(value);
        node.is_end_of_word = node.children.is_empty();
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        let mut node = &self.root;
        for c in key.chars() {
            if let Some(child) = node.children.get(&c) {
                node = child;
            } else {
                return None;
            }
        }
        node.value.as_ref()
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trie() {
        let mut trie = Trie::new();
        trie.insert("apple", 1);
        trie.insert("app", 2);
        assert_eq!(trie.get("apple"), Some(&1));
        assert_eq!(trie.get("app"), Some(&2));
        assert_eq!(trie.get("ap"), None);
    }
}