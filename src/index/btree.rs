use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{data::log_record::LogRecordPos, errors::Result, options::IteratorOptions};

use super::{Indexer, IndexerIterator};

#[derive(Clone)]
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        // write_guard.insert(key, pos);
        write_guard
            .entry(key)
            .and_modify(|v| *v = pos)
            .or_insert(pos);
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

    fn iterator(&self, option: IteratorOptions) -> Box<dyn IndexerIterator> {
        let read_guard = self.tree.read();
        let mut items = read_guard
            .iter()
            .map(|(a, b)| (a.clone(), b.clone()))
            .collect::<Vec<_>>();
        if option.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            curr_index: 0,
            options: option,
        })
    }

    fn list_keys(&self) -> Result<Vec<bytes::Bytes>> {
        let read_guard = self.tree.read();
        let keys = read_guard
            .keys()
            .into_iter()
            .map(|a| Bytes::copy_from_slice(a))
            .collect();
        Ok(keys)
    }
}

pub struct BTreeIterator {
    // 存储Key + 索引
    items: Vec<(Vec<u8>, LogRecordPos)>,
    // 当前遍历的位置的下标
    curr_index: usize,
    // 配置项
    options: IteratorOptions,
}

impl IndexerIterator for BTreeIterator {
    fn seek(&mut self, key: Vec<u8>) {
        // 二分查找
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };
    }

    fn rewind(&mut self) {
        self.curr_index = 0
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }

        None
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
                // size: 11,
            },
        );
        assert!(res1 == true);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
                // size: 11,
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
                // size: 11,
            },
        );
        assert!(res1 == true);
        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
                // size: 11,
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

    #[test]
    fn test_btree_iterator_seek() {
        let bt = BTree::new();

        // 没有数据的情况
        let mut iter = bt.iterator(Default::default());
        iter.seek("aa".as_bytes().to_vec());
        let res1 = iter.next();
        assert!(res1.is_none());
        // println!("{:?}", res1);

        bt.put(
            "ccde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter2 = bt.iterator(Default::default());
        iter.seek("aa".as_bytes().to_vec());
        let res2 = iter2.next();
        assert!(res2.is_some());
        iter.seek("zz".as_bytes().to_vec());
        let res3 = iter.next();
        assert!(res3.is_none());

        bt.put(
            "ccdf".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "bcde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "acde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "ccae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "cfde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );

        let mut iter = bt.iterator(Default::default());
        iter.seek("ca".as_bytes().to_vec());
        let res1 = iter.next();
        assert!(res1.is_some());
        let res1 = iter.next();
        assert!(res1.is_some());
        let res1 = iter.next();
        assert!(res1.is_some());
        let res1 = iter.next();
        assert!(res1.is_some());
        let res1 = iter.next();
        assert!(res1.is_none());

        iter.seek("cfde".as_bytes().to_vec());
        let res = iter.next();
        assert!(res.is_some());
        assert!(*res.unwrap().0 == "cfde".as_bytes().to_vec());
        let res = iter.next();
        assert!(res.is_none());

        // 反向迭代
        let mut opts = IteratorOptions::default();
        opts.reverse = true;
        let mut iter = bt.iterator(opts);

        iter.seek("zz".as_bytes().to_vec());
        while let Some(a) = iter.next() {
            println!("{:?}", String::from_utf8(a.0.to_vec()));
        }
    }

    #[test]
    fn test_btree_iterator_next() {
        let bt = BTree::new();
        let mut iter1 = bt.iterator(Default::default());
        assert!(iter1.next().is_none());

        // 有一条数据的情况
        bt.put(
            "ccdf".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter_opt1 = IteratorOptions::default();
        iter_opt1.reverse = true;
        let mut iter2 = bt.iterator(iter_opt1);
        println!("{:?}", iter2.next().is_some());

        // 多条数据的情况
        bt.put(
            "bcde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "acde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "ccae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "cfde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );

        let mut iter_opt2 = IteratorOptions::default();
        iter_opt2.reverse = true;
        let mut iter3 = bt.iterator(iter_opt2);
        while let Some(item) = iter3.next() {
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
            assert!(item.0.len() > 0);
        }

        // 有前缀的情况
        let mut iter_opt3 = IteratorOptions::default();
        iter_opt3.prefix = "ccae".as_bytes().to_vec();
        let mut iter4 = bt.iterator(iter_opt3);
        while let Some(item) = iter4.next() {
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }
    }
}
