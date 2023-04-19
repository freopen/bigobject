use std::{any::Any, marker::PhantomData, path::Path, sync::Arc};

use moka::sync::Cache;
use parking_lot::RwLock;

use crate::{
    bigobject::BigObject,
    db_key::split_db_key,
    storage::guard::{RGuard, WGuard},
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
    pub rocksdb: rocksdb::DB,
    pub cache: Cache<Vec<u8>, CacheEntry>,
}

pub struct Db<T: BigObject> {
    pub(super) inner: Arc<RwLock<DbInner>>,
    _phantom: PhantomData<T>,
}

fn db_opts() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.increase_parallelism(
        std::thread::available_parallelism()
            .unwrap_or_else(|_| std::num::NonZeroUsize::new(1).unwrap())
            .get() as i32,
    );
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_bottommost_compression_options(0, 5, 0, 16 * 1024, true);
    opts.set_bottommost_zstd_max_train_bytes(100 * 16 * 1024, true);
    opts.set_comparator("BigObjectComparator", |a, b| {
        let (a_prefix, a_key) = split_db_key(a);
        let (b_prefix, b_key) = split_db_key(b);
        a_prefix.cmp(b_prefix).then_with(|| a_key.cmp(b_key))
    });
    opts.set_prefix_extractor(rocksdb::SliceTransform::create(
        "BigObjectPrefixExtractor",
        |key| split_db_key(key).0,
        None,
    ));
    opts.set_optimize_filters_for_hits(true);
    opts.set_bytes_per_sync(1024 * 1024);
    opts.set_allow_concurrent_memtable_write(false);
    opts.set_inplace_update_support(true);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&block_opts);
    opts.set_use_adaptive_mutex(true);
    opts.set_memtable_prefix_bloom_ratio(0.1);
    opts.set_memtable_whole_key_filtering(true);
    opts.set_max_log_file_size(1024 * 1024);
    opts.set_recycle_log_file_num(5);
    opts
}

impl<T: BigObject + Default> Db<T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let rocksdb = rocksdb::DB::open(&db_opts(), path).unwrap();
        if rocksdb.get([0]).unwrap().is_none() {
            rocksdb
                .put([0], rmp_serde::to_vec(&T::default()).unwrap())
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
    pub fn w(&self) -> WGuard<'_, T> {
        WGuard::new(self)
    }
}
