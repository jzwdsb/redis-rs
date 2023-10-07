use redis_rs::client::Client;

#[test]
fn test_redis_integration() {
    // Connect to Redis
    let mut client = Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection();

    // Set a key-value pair
    let _: () = con.set("my_key", "my_value").unwrap();

    // Get the value of the key
    let value = con.get("my_key").unwrap();
    assert_eq!(value, "my_value".as_bytes());
}
