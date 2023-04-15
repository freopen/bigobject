use std::sync::Arc;

use rocksdb::IterateBounds;

use crate::{
    storage::{
        db::{CacheEntry, DbInner, SyncWrapper, CACHE_ENTRY_OVERHEAD},
        lock_context::Prefix,
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

    pub fn put<T: BigObject>(&mut self, key: &Prefix, value: T) {
        let encoded = rmp_serde::to_vec(&value).unwrap();
        self.cache_inserts.push((
            key.inner.clone(),
            CacheEntry {
                len: (key.inner.len() + encoded.len() + CACHE_ENTRY_OVERHEAD) as u32,
                value: Some(Arc::new(SyncWrapper(value))),
            },
        ));
        self.rocksdb.put(&key.inner, encoded);
    }
    pub fn delete(&mut self, key: &Prefix) {
        let (from, to) = rocksdb::PrefixRange(key.inner.clone()).into_bounds();
        self.rocksdb.delete_range(from.unwrap(), to.unwrap());
        self.cache_deletes.push(key.inner.clone());
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
