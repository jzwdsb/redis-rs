mod test {

    #[allow(unused_imports)]
    use std::sync::{Arc, Mutex};
    use std::thread;

    use lazy_static::lazy_static;
    use std::sync::Once;

    #[allow(unused_imports)]
    use redis_rs::client::AsyncClient;

    const REDIS_HOST: &str = "0.0.0.0";
    const REDIS_PORT: u16 = 6379;
    const MAX_CLIENTS: usize = 1024;

    lazy_static! {
        static ref INIT: Once = Once::new();
    }

    // async fn setup() {
    //     // start redis server in the background
    //     thread::spawn(async move || {
    //         let mut server =
    //             redis_rs::server::Server::new(REDIS_HOST, REDIS_PORT, MAX_CLIENTS).await.unwrap();
    //         server.run().await.unwrap();
    //     });
    // }

    // FIXME: github action is failed due to test timeout
    // TODO: use BDD framework to write test cases

    // #[test]
    // fn test_block_redis_cli() {
    //     // start redis server
    //     INIT.call_once(setup);

    //     // hold 1 second to wait for redis to start
    //     std::thread::sleep(std::time::Duration::from_secs(1));

    //     let mut client = BlockClient::open("127.0.0.1:6379").unwrap();

    //     let con = client.get_connection();

    //     // Set a key-value pair
    //     let _: () = con.set("my_key", "my_value").unwrap();
    //     // Get the value of the key
    //     let value = con.get("my_key").unwrap();
    //     assert_eq!(value, "my_value".as_bytes());
    // }

    // #[tokio::test]
    // async fn test_async_redis_cli() {
    //     // start redis server
    //     INIT.call_once(setup);

    //     // hold 1 second to wait for redis to start
    //     std::thread::sleep(std::time::Duration::from_secs(1));

    //     let mut client = AsyncClient::open("127.0.0.1:6379").await.unwrap();

    //     let conn = client.get_connection();

    //     // Set a key-value pair
    //     let _: () = conn.set("my_key", "my_value").await.unwrap();
    //     // Get the value of the key
    //     let value = conn.get("my_key").await.unwrap();
    //     assert_eq!(value, "my_value".as_bytes());
    // }
}
