use std::sync::Arc;

use crate::{
    storage::{
        db::{CacheEntry, DbInner, SyncWrapper, CACHE_ENTRY_OVERHEAD},
        lock_context::{LockContext, Prefix},
    },
    BigObject,
};

pub struct Batch {
    rocksdb: rocksdb::WriteBatch,
    cache_inserts: Vec<(Vec<u8>, CacheEntry)>,
    cache_deletes: Vec<Vec<u8>>,
}

impl Batch {
    pub(super) fn new() -> Self {
        Batch {
            rocksdb: rocksdb::WriteBatch::default(),
            cache_inserts: Vec::new(),
            cache_deletes: Vec::new(),
        }
    }

    pub fn put<T: BigObject>(&mut self, key: Prefix, value: T) {
        let encoded = rmp_serde::to_vec(&value).unwrap();
        let len = (key.inner.len() + encoded.len() + CACHE_ENTRY_OVERHEAD) as u32;
        self.rocksdb.put(&key.inner, encoded);
        self.cache_inserts.push((
            key.inner,
            CacheEntry {
                len,
                value: Some(Arc::new(SyncWrapper(value))),
            },
        ));
    }
    pub fn delete(&mut self, prefix: &Prefix) {
        let prefix = prefix.inner.clone();
        self.cache_deletes.push(prefix.clone());

        let to = {
            let ffs = prefix
                .iter()
                .rev()
                .take_while(|&&byte| byte == u8::MAX)
                .count();
            let to = &prefix[..(prefix.len() - ffs)];
            if !to.is_empty() {
                let mut to = to.to_vec();
                *to.last_mut().unwrap() += 1;
                to
            } else if let Some(mut to) = LockContext::last_key() {
                to.push(0);
                to
            } else {
                return;
            }
        };
        self.rocksdb.delete_range(prefix, to);
    }
    pub(super) fn apply(self, db: &DbInner) {
        db.rocksdb.write(self.rocksdb).unwrap();
        for (key, value) in self.cache_inserts {
            db.cache.insert(key, value);
        }
        if !self.cache_deletes.is_empty() {
            db.cache
                .invalidate_entries_if(move |key, _value| {
                    self.cache_deletes
                        .iter()
                        .any(|prefix| key.starts_with(prefix))
                })
                .unwrap();
        }
    }
}
