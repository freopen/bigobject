use std::sync::Arc;

use crate::{
    bigobject::{bigmap::KeyRef, BigObject},
    db_key::{append_map_key, append_prefix_len},
    storage::{
        db::{CacheEntry, DbInner, SyncWrapper, CACHE_ENTRY_OVERHEAD},
        lock_context::LockContext,
    },
};

#[derive(Default)]
pub struct Batch {
    rocksdb: rocksdb::WriteBatch,
    cache_inserts: Vec<(Vec<u8>, CacheEntry)>,
    cache_deletes: Vec<Vec<u8>>,
}

impl Batch {
    pub(crate) fn put<T: BigObject, K: KeyRef>(&mut self, prefix: &[u8], key: &K, mut value: T) {
        let mut db_key = prefix.to_vec();
        let prefix_len = prefix.len();
        append_map_key(&mut db_key, key);
        value.finalize(|| &mut db_key, self);
        let encoded = rmp_serde::to_vec(&value).unwrap();
        let len = (db_key.len() + encoded.len() + CACHE_ENTRY_OVERHEAD) as u32;
        let cache_key_len = db_key.len();
        append_prefix_len(&mut db_key, prefix_len);
        self.rocksdb.put(&db_key, encoded);
        db_key.truncate(cache_key_len);
        self.cache_inserts.push((
            db_key,
            CacheEntry {
                len,
                value: Some(Arc::new(SyncWrapper(value))),
            },
        ));
    }
    pub(crate) fn delete<K: KeyRef>(&mut self, prefix: &[u8], key: &K) {
        let mut db_key = prefix.to_vec();
        let prefix_len = prefix.len();
        append_map_key(&mut db_key, key);
        self.delete_prefix(&db_key);
        append_prefix_len(&mut db_key, prefix_len);
        self.rocksdb.delete(&db_key);
    }
    pub fn delete_prefix(&mut self, prefix: &[u8]) {
        let mut from = prefix.to_vec();
        let mut to = if let Some(nonff) = prefix.iter().rposition(|&byte| byte < u8::MAX) {
            let mut to = prefix[..nonff].to_vec();
            *to.last_mut().unwrap() += 1;
            to
        } else if let Some(mut to) = LockContext::last_key() {
            to.push(0);
            to
        } else {
            return;
        };
        let from_len = from.len();
        let to_len = to.len();
        append_prefix_len(&mut from, from_len);
        append_prefix_len(&mut to, to_len);
        self.rocksdb.delete_range(&from, &to);
        self.cache_deletes.push(prefix.to_vec());
    }
    pub(super) fn apply(self, db: &DbInner) {
        db.rocksdb.write(self.rocksdb).unwrap();
        if !self.cache_deletes.is_empty() {
            db.cache
                .invalidate_entries_if(move |key, _value| {
                    self.cache_deletes
                        .iter()
                        .any(|prefix| key.starts_with(prefix))
                })
                .unwrap();
        }
        for (key, value) in self.cache_inserts {
            db.cache.insert(key, value);
        }
    }
}
