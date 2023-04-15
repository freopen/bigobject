use std::{any::Any, cell::RefCell, marker::PhantomData, sync::Arc};

use elsa::FrozenVec;

use crate::{
    bigobject::Key,
    storage::db::{CacheEntry, DbInner, SyncWrapper},
    BigObject,
};

pub type PhantomContext = PhantomData<*const ()>;
struct LockContextInner<'a> {
    db: &'a DbInner,
    read_stash: FrozenVec<Arc<dyn Any + Send + Sync>>,
}

thread_local! {
    static LOCK_CONTEXT: RefCell<Option<&'static LockContextInner<'static>>> = RefCell::new(None);
}

pub(super) struct LockContext {
    _inner: Box<LockContextInner<'static>>,
    _phantom: PhantomContext,
}

#[derive(Clone, Default)]
pub struct Prefix {
    pub(super) inner: Vec<u8>,
    _phantom: PhantomContext,
}

impl Prefix {
    pub fn child<K: Key>(&self, key: &K) -> Self {
        let mut child = self.clone();
        storekey::serialize_into(&mut child.inner, key).unwrap();
        child
    }
}

impl LockContext {
    pub fn new(db: &DbInner) -> Self {
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

    pub fn root_prefix(&self) -> Prefix {
        Prefix {
            inner: vec![],
            _phantom: PhantomContext::default(),
        }
    }

    pub fn get<T: BigObject>(prefix: &Prefix) -> Option<&'static T> {
        LOCK_CONTEXT.with(|context| {
            let context = context.borrow_mut().unwrap();
            context
                .db
                .cache
                .get_with_by_ref(&prefix.inner, || {
                    if let Some(encoded) = context.db.rocksdb.get_pinned(&prefix.inner).unwrap() {
                        let mut value = rmp_serde::decode::from_slice::<T>(&encoded).unwrap();
                        value.initialize(|| prefix.clone());
                        CacheEntry {
                            len: (prefix.inner.len() + encoded.len() + 24) as u32,
                            value: Some(Arc::new(SyncWrapper(value))),
                        }
                    } else {
                        CacheEntry {
                            len: (prefix.inner.len() + 24) as u32,
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

pub trait WithContext: BigObject {
    type Key: Key;
    type Value: BigObject;

    fn get_child(&self, prefix: &Prefix, key: &Self::Key) -> Option<&Self::Value> {
        LockContext::get(&prefix.child(key))
    }
}
