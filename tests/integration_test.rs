#[allow(unused_imports)]
use redis_rs::client::BlockClient;

mod test {
    #[allow(unused_imports)]
    use super::*;
    use std::thread;
    const REDIS_HOST: &str = "0.0.0.0";
    const REDIS_PORT: u16 = 6379;
    const MAX_CLIENTS: usize = 1024;

    #[allow(dead_code)]
    fn setup() {
        // Setup code here

        // start redis server in the background
        thread::spawn(move || {
            let mut server =
                redis_rs::server::Server::new(REDIS_HOST, REDIS_PORT, MAX_CLIENTS).unwrap();
            server.run().unwrap();
        });
    }

    #[test]
    fn test_redis_get_set() {
        // Connect to Redis
        setup();

        // hold 1 second to wait for redis to start
        std::thread::sleep(std::time::Duration::from_secs(1));

        let mut client = BlockClient::open("127.0.0.1:6379").unwrap();

        let con = client.get_connection();

        // Set a key-value pair
        let _: () = con.set("my_key", "my_value").unwrap();
        // Get the value of the key
        let value = con.get("my_key").unwrap();
        assert_eq!(value, "my_value".as_bytes());
    }
}
