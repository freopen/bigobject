use std::{
    cell::{Ref, RefCell},
    mem::swap,
    ops::{Deref, DerefMut},
};

use parking_lot::{RwLockReadGuard, RwLockUpgradableReadGuard};

use crate::{
    bigobject::BigObject,
    storage::{
        batch::Batch,
        db::{Db, DbInner},
        lock_context::LockContext,
        prefix::Prefix,
    },
};

pub struct RGuard<'a, T: BigObject> {
    _guard: RwLockReadGuard<'a, DbInner>,
    _context: LockContext,
    root: Ref<'a, T>,
}

impl<'a, T: BigObject> RGuard<'a, T> {
    pub(super) fn new(db: &'a Db<T>) -> RGuard<'a, T> {
        let guard = db.inner.read();
        let context = LockContext::new(&guard);
        RGuard {
            _guard: guard,
            _context: context,
            root: db.root.borrow(),
        }
    }
}

impl<'a, T: BigObject> Deref for RGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.root
    }
}

pub struct WGuard<'a, T: BigObject> {
    guard: Option<RwLockUpgradableReadGuard<'a, DbInner>>,
    _context: LockContext,
    root: T,
    db_root: &'a RefCell<T>,
}

impl<'a, T: BigObject> WGuard<'a, T> {
    pub(super) fn new(db: &'a Db<T>) -> WGuard<'a, T> {
        let guard = db.inner.upgradable_read();
        let context = LockContext::new(&guard);
        let root = db.root.borrow().big_clone();
        WGuard {
            guard: Some(guard),
            _context: context,
            root,
            db_root: &db.root,
        }
    }
}

impl<'a, T: BigObject> Deref for WGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.root
    }
}

impl<'a, T: BigObject> DerefMut for WGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.root
    }
}

impl<'a, T: BigObject> Drop for WGuard<'a, T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let mut batch = Batch::default();
        let mut prefix = Prefix::new();
        self.root.finalize(|| &mut prefix, &mut batch);
        batch
            .rocksdb
            .put([0], rmp_serde::to_vec(&self.root).unwrap());
        let db = RwLockUpgradableReadGuard::upgrade(self.guard.take().unwrap());
        batch.apply(&db);
        swap(self.db_root.borrow_mut().deref_mut(), &mut self.root);
    }
}
