pub struct KvStore {}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {}
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        println!("{}", key);
        unimplemented!("unimplemented");
    }
    pub fn set(&mut self, key: String, value: String) {
        println!("{} = {}", key, value);
        unimplemented!("unimplemented");
    }
    pub fn remove(&mut self, key: String) {
        println!("{}", key);
        unimplemented!("unimplemented");
    }
}
