
pub trait BloomFilter {
    fn new() -> Self;
    fn insert(&mut self, key: &str);
    fn contains(&self, key: &str) -> bool;
}

pub struct Bloom {

}

impl BloomFilter for Bloom {
    fn new() -> Self {
        Bloom {

        }
    }

    fn insert(&mut self, key: &str) {

    }

    fn contains(&self, key: &str) -> bool {
        false
    }
}


#[cfg(test)]
mod tests {
    
}
