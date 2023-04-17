use std::{any::Any, cell::RefCell, marker::PhantomData, sync::Arc};

use elsa::FrozenVec;

use crate::{
    bigobject::BigObject,
    storage::db::{CacheEntry, DbInner, SyncWrapper},
};

pub type PhantomContext = PhantomData<*const ()>;
struct LockContextInner<'a> {
    db: &'a DbInner,
    read_stash: FrozenVec<Arc<dyn Any + Send + Sync>>,
}

thread_local! {
    static LOCK_CONTEXT: RefCell<Option<&'static LockContextInner<'static>>> = RefCell::new(None);
}

pub struct LockContext {
    _inner: Box<LockContextInner<'static>>,
    _phantom: PhantomContext,
}

impl LockContext {
    pub(super) fn new(db: &DbInner) -> Self {
        let inner = Box::new(LockContextInner {
            db: unsafe { std::mem::transmute(db) },
            read_stash: FrozenVec::new(),
        });
        LOCK_CONTEXT.with(|context| {
            assert!(context
                .replace(Some(unsafe { std::mem::transmute(inner.as_ref()) }))
                .is_none())
        });
        Self {
            _inner: inner,
            _phantom: PhantomContext::default(),
        }
    }

    pub fn last_key() -> Option<Vec<u8>> {
        LOCK_CONTEXT.with(|context| {
            context
                .borrow_mut()
                .unwrap()
                .db
                .rocksdb
                .iterator(rocksdb::IteratorMode::End)
                .next()
                .map(|kv| kv.unwrap().0.into_vec())
        })
    }

    pub fn get<T: BigObject>(key: &[u8]) -> Option<&'static T> {
        LOCK_CONTEXT.with(|context| {
            let context = context.borrow_mut().unwrap();
            context
                .db
                .cache
                .get_with_by_ref(key, || {
                    if let Some(encoded) = context.db.rocksdb.get_pinned(key).unwrap() {
                        let mut value = rmp_serde::decode::from_slice::<T>(&encoded).unwrap();
                        value.initialize(|| key.to_vec());
                        CacheEntry {
                            len: (key.len() + encoded.len() + 24) as u32,
                            value: Some(Arc::new(SyncWrapper(value))),
                        }
                    } else {
                        CacheEntry {
                            len: (key.len() + 24) as u32,
                            value: None,
                        }
                    }
                })
                .value
                .map(|value| {
                    &context
                        .read_stash
                        .push_get(value)
                        .downcast_ref::<SyncWrapper<T>>()
                        .unwrap()
                        .0
                })
        })
    }
}

impl Drop for LockContext {
    fn drop(&mut self) {
        LOCK_CONTEXT.with(|context| {
            assert!(context.replace(None).is_some());
        })
    }
}
