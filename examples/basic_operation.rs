use bytes::Bytes;
use kv_store::{db, options::Options};

fn main() {
    let opts = Options::default();
    let engine = db::Engine::open(opts).expect("Failed to open bitcask engine");

    let res1 = engine.put(Bytes::from("name"), Bytes::from("bitcask-rs"));
    assert!(res1.is_ok());

    let res2 = engine.get(Bytes::from("name"));
    assert!(res2.is_ok());
    println!("{:?}", String::from_utf8(res2.ok().unwrap().to_vec()));

    let res3 = engine.delete(Bytes::from("name"));
    assert!(res3.is_ok());
}
