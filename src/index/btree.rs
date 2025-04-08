use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::data::log_record::LogRecordPos;

use super::Indexer;

pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_grard = self.tree.read();
        read_grard.get(&key).copied()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        assert!(res1 == true);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
                size: 11,
            },
        );
        assert!(res2 == true);

        // let res3 = bt.put(
        //     "aa".as_bytes().to_vec(),
        //     LogRecordPos {
        //         file_id: 1144,
        //         offset: 22122,
        //         size: 11,
        //     },
        // );
        // assert!(res3==true);
        // let v = res3;
        // assert_eq!(v.file_id, 11);
        // assert_eq!(v.offset, 22);
    }

    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        assert!(res1 == true);
        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
                size: 11,
            },
        );
        assert!(res2 == true);

        let pos1 = bt.get("".as_bytes().to_vec());
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().offset, 10);

        let pos2 = bt.get("aa".as_bytes().to_vec());
        assert!(pos2.is_some());
        assert_eq!(pos2.unwrap().file_id, 11);
        assert_eq!(pos2.unwrap().offset, 22);
    }

    // #[test]
    // fn test_btree_delete() {
    //     let bt = BTree::new();
    //     let res1 = bt.put(
    //         "".as_bytes().to_vec(),
    //         LogRecordPos {
    //             file_id: 1,
    //             offset: 10,
    //             size: 11,
    //         },
    //     );
    //     assert!(res1==true);
    //     let res2 = bt.put(
    //         "aa".as_bytes().to_vec(),
    //         LogRecordPos {
    //             file_id: 11,
    //             offset: 22,
    //             size: 11,
    //         },
    //     );
    //     assert!(res2==true);

    //     let del1 = bt.delete("".as_bytes().to_vec());
    //     assert!(del1==true);
    //     let v1 = del1;
    //     assert_eq!(v1.file_id, 1);
    //     assert_eq!(v1.offset, 10);

    //     let del2 = bt.delete("aa".as_bytes().to_vec());
    //     assert!(del2.is_some());
    //     let v2 = del2.unwrap();
    //     assert_eq!(v2.file_id, 11);
    //     assert_eq!(v2.offset, 22);

    //     let del3 = bt.delete("not exist".as_bytes().to_vec());
    //     assert!(del3.is_none());
    // }
}
