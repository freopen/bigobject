use std::ops::{Deref, DerefMut};

use parking_lot::{RwLockReadGuard, RwLockUpgradableReadGuard};

use crate::{
    storage::{db::DbInner, lock_context::LockContext, Batch, Db},
    BigObject,
};

pub struct RGuard<'a, T: BigObject> {
    _guard: RwLockReadGuard<'a, DbInner>,
    _context: LockContext,
    root: &'a T,
}

impl<'a, T: BigObject> RGuard<'a, T> {
    pub(super) fn new(db: &'a Db<T>) -> RGuard<'a, T> {
        let guard = db.inner.read();
        let context = LockContext::new(&guard);
        let root = LockContext::get(&context.root_prefix()).unwrap();
        RGuard {
            _guard: guard,
            _context: context,
            root,
        }
    }
}

impl<'a, T: BigObject> Deref for RGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.root
    }
}

pub struct RWGuard<'a, T: BigObject> {
    guard: Option<RwLockUpgradableReadGuard<'a, DbInner>>,
    context: LockContext,
    root: Option<T>,
}

impl<'a, T: BigObject> RWGuard<'a, T> {
    pub(super) fn new(db: &'a Db<T>) -> RWGuard<'a, T> {
        let guard = db.inner.upgradable_read();
        let context = LockContext::new(&guard);
        let root = LockContext::get::<T>(&context.root_prefix())
            .unwrap()
            .internal_clone();
        RWGuard {
            guard: Some(guard),
            context,
            root: Some(root),
        }
    }
}

impl<'a, T: BigObject> Deref for RWGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.root.as_ref().unwrap()
    }
}

impl<'a, T: BigObject> DerefMut for RWGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.root.as_mut().unwrap()
    }
}

impl<'a, T: BigObject> Drop for RWGuard<'a, T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let mut batch = Batch::new();
        let mut root = self.root.take().unwrap();
        root.finalize(|| self.context.root_prefix(), &mut batch);
        batch.put(self.context.root_prefix(), root);
        let db = RwLockUpgradableReadGuard::upgrade(self.guard.take().unwrap());
        batch.apply(&db);
    }
}
