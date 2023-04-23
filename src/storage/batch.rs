use std::sync::Arc;

use crate::{
    bigobject::{bigmap::KeyRef, BigObject},
    storage::{
        db::{CacheEntry, DbInner, SyncWrapper},
        prefix::Prefix,
    },
};

#[derive(Default)]
pub struct Batch {
    pub(crate) rocksdb: rocksdb::WriteBatch,
    cache_inserts: Vec<(Vec<u8>, CacheEntry)>,
    cache_entry_deletes: Vec<Vec<u8>>,
    cache_prefix_deletes: Vec<Vec<u8>>,
}

impl Batch {
    pub(crate) fn put<T: BigObject, K: KeyRef>(&mut self, prefix: &Prefix, key: &K, mut value: T) {
        let mut prefix = prefix.clone();
        let prefix_len = prefix.append_map_key(key);
        value.finalize(|| &mut prefix, self);
        let encoded = rmp_serde::to_vec(&value).unwrap();
        let db_key = prefix.into_leaf(prefix_len);
        let len = (db_key.len() + encoded.len()) as u32;
        self.rocksdb.put(&db_key, encoded);
        self.cache_inserts.push((
            db_key,
            CacheEntry {
                len,
                value: Some(Arc::new(SyncWrapper(value))),
            },
        ));
    }
    pub(crate) fn delete<K: KeyRef>(&mut self, prefix: &Prefix, key: &K) {
        let mut prefix = prefix.clone();
        let prefix_len = prefix.append_map_key(key);
        self.delete_prefix(&prefix);
        let db_key = prefix.into_leaf(prefix_len);
        self.rocksdb.delete(&db_key);
        self.cache_entry_deletes.push(db_key);
    }
    pub(crate) fn delete_prefix(&mut self, prefix: &Prefix) {
        let next_prefix = prefix.next_prefix();
        let next_prefix_len = next_prefix.len();
        let from = prefix.clone().into_leaf(prefix.len());
        let to = next_prefix.into_leaf(next_prefix_len);
        self.rocksdb.delete_range(&from, &to);
        self.cache_prefix_deletes.push(from);
    }
    pub(super) fn apply(self, db: &DbInner) {
        db.rocksdb.write(self.rocksdb).unwrap();
        if !self.cache_prefix_deletes.is_empty() {
            db.cache
                .invalidate_entries_if(move |key, _value| {
                    self.cache_prefix_deletes
                        .iter()
                        .any(|prefix| key.starts_with(prefix))
                })
                .unwrap();
        }
        for key in self.cache_entry_deletes {
            let len = key.len();
            db.cache.insert(
                key,
                CacheEntry {
                    len: len.try_into().unwrap(),
                    value: None,
                },
            );
        }
        for (key, value) in self.cache_inserts {
            db.cache.insert(key, value);
        }
    }
}
