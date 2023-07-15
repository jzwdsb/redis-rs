# Redis Server implement in Rust

this project is to learn rust and redis, reimplement redis server in rust

## Roadmap

- [ ] implement redis server from stratch without 3rd party dependencies
  - [ ] implement basic data structure
  - [ ] implement basic commands
  - [ ] implement basic server
  - [ ] compatible with redis-cli
- [ ] optimize data structure and IO handle
  - [ ] data structure
    - [ ] optimize memory usage
    - [ ] optimize performance
  - [ ] IO handle
    - [ ] IO multiplexing
- [ ] Benchmark
  - [ ] compare with official redis server
  
### Progress

- [x] Implement basic data structure
  - [x] String
  - [x] List
  - [x] Hash
  - [x] Set
  - [ ] Sorted Set
- [ ] Implement basic commands
  - [x] Get
  - [x] Set
  - [x] Del
  - [ ] Exists
  - [ ] Expire
  - [ ] Persist
- [ ] Implement basic server
  - [ ] TCP Server from stratch (no framework)
  - [ ] Basic command parser
  - [ ] Basic command executor
  - [ ] Basic response formatter
  - [ ] Basic error handler
  - [ ] Basic logging
  - [ ] Basic configuration
  - [ ] Basic persistence

## Dependencies

