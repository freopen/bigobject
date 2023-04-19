use std::ops::{Deref, DerefMut};

use parking_lot::{RwLockReadGuard, RwLockUpgradableReadGuard};

use crate::{
    bigobject::BigObject,
    storage::{
        batch::Batch,
        db::{Db, DbInner},
        lock_context::LockContext,
    },
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
        let root = LockContext::get(&[], &()).unwrap();
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

pub struct WGuard<'a, T: BigObject> {
    guard: Option<RwLockUpgradableReadGuard<'a, DbInner>>,
    _context: LockContext,
    root: Option<T>,
}

impl<'a, T: BigObject> WGuard<'a, T> {
    pub(super) fn new(db: &'a Db<T>) -> WGuard<'a, T> {
        let guard = db.inner.upgradable_read();
        let context = LockContext::new(&guard);
        let root = LockContext::get::<T, _>(&[], &()).unwrap().big_clone();
        WGuard {
            guard: Some(guard),
            _context: context,
            root: Some(root),
        }
    }
}

impl<'a, T: BigObject> Deref for WGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.root.as_ref().unwrap()
    }
}

impl<'a, T: BigObject> DerefMut for WGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.root.as_mut().unwrap()
    }
}

impl<'a, T: BigObject> Drop for WGuard<'a, T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let mut batch = Batch::default();
        let mut root = self.root.take().unwrap();
        let mut key = Vec::new();
        root.finalize(|| &mut key, &mut batch);
        batch.put(&[], &(), root);
        let db = RwLockUpgradableReadGuard::upgrade(self.guard.take().unwrap());
        batch.apply(&db);
    }
}
