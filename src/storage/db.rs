use std::{any::Any, marker::PhantomData, path::Path, sync::Arc};

use moka::sync::Cache;
use parking_lot::RwLock;

use crate::{
    storage::guard::{RGuard, RWGuard},
    BigObject,
};

pub(super) const CACHE_ENTRY_OVERHEAD: usize = 24;

#[repr(transparent)]
pub(super) struct SyncWrapper<T: BigObject>(pub(super) T);
unsafe impl<T: BigObject> Send for SyncWrapper<T> {}
unsafe impl<T: BigObject> Sync for SyncWrapper<T> {}

#[derive(Clone)]
pub(super) struct CacheEntry {
    pub(super) len: u32,
    pub(super) value: Option<Arc<dyn Any + Send + Sync>>,
}

pub(super) struct DbInner {
    pub(super) rocksdb: rocksdb::DB,
    pub(super) cache: Cache<Vec<u8>, CacheEntry>,
}

pub struct Db<T: BigObject> {
    pub(super) inner: Arc<RwLock<DbInner>>,
    _phantom: PhantomData<T>,
}

impl<T: BigObject + Default> Db<T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let rocksdb = rocksdb::DB::open_default(path).unwrap();
        if rocksdb.get([]).unwrap().is_none() {
            rocksdb
                .put([], rmp_serde::to_vec(&T::default()).unwrap())
                .unwrap();
        }
        let cache = Cache::builder()
            .max_capacity(128 * 1024 * 1024)
            .weigher(|_key, value: &CacheEntry| value.len)
            .support_invalidation_closures()
            .build();
        Db {
            inner: Arc::new(RwLock::new(DbInner { rocksdb, cache })),
            _phantom: PhantomData,
        }
    }
    pub fn r(&self) -> RGuard<'_, T> {
        RGuard::new(self)
    }
    pub fn rw(&self) -> RWGuard<'_, T> {
        RWGuard::new(self)
    }
}