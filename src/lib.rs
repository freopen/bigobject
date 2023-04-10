use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
    thread,
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rocksdb::WriteBatch;
use serde::{de::DeserializeOwned, Serialize};

pub trait BigObject: Serialize + DeserializeOwned {
    fn attach<'val, 'db: 'val>(&'val mut self, _db: &'db DBInner, _key: &[u8]) {}
    fn finalize(&mut self, _batch: &mut WriteBatch) {}
}

impl<T: Serialize + DeserializeOwned + 'static> BigObject for T {}

pub struct DBInner {
    lock: RwLock<()>,
    rocksdb: rocksdb::DB,
}

#[derive(Clone)]
pub struct DB<T: BigObject> {
    inner: Arc<DBInner>,
    _phantom: std::marker::PhantomData<T>,
}

pub struct RGuard<'a, T: BigObject> {
    _guard: RwLockReadGuard<'a, ()>,
    root: T,
}

impl<'a, T: BigObject> Deref for RGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.root
    }
}

pub struct RWGuard<'a, T: BigObject> {
    _guard: RwLockWriteGuard<'a, ()>,
    db: &'a DBInner,
    root: T,
}

impl<'a, T: BigObject> Deref for RWGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.root
    }
}

impl<'a, T: BigObject> DerefMut for RWGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.root
    }
}

impl<'a, T: BigObject> Drop for RWGuard<'a, T> {
    fn drop(&mut self) {
        if thread::panicking() {
            // Do not commit when panicking.
            return;
        }
        let mut batch = WriteBatch::default();
        self.root.finalize(&mut batch);
        batch.put([], &rmp_serde::to_vec(&self.root).unwrap());
        self.db.rocksdb.write(batch).unwrap();
    }
}

impl<T: BigObject + Default> DB<T> {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let rocksdb = rocksdb::DB::open_default(path).unwrap();
        DB {
            inner: Arc::new(DBInner {
                rocksdb,
                lock: RwLock::new(()),
            }),
            _phantom: std::marker::PhantomData,
        }
    }
    pub fn r(&self) -> RGuard<'_, T> {
        let guard = self.inner.lock.read();
        let mut root = self
            .inner
            .rocksdb
            .get_pinned([])
            .unwrap()
            .map(|root| rmp_serde::decode::from_slice::<T>(&root).unwrap())
            .unwrap_or_default();
        root.attach(&self.inner, &[]);
        RGuard {
            _guard: guard,
            root,
        }
    }
    pub fn rw(&self) -> RWGuard<'_, T> {
        let guard = self.inner.lock.write();
        let mut root = self
            .inner
            .rocksdb
            .get_pinned([])
            .unwrap()
            .map(|root| rmp_serde::decode::from_slice::<T>(&root).unwrap())
            .unwrap_or_default();
        root.attach(&self.inner, &[]);
        RWGuard {
            _guard: guard,
            db: &self.inner,
            root,
        }
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use crate::DB;

    #[derive(Serialize, Deserialize, Default)]
    struct SerdeObj {
        int: i32,
        str: String,
    }

    #[test]
    fn serde_root() -> Result<()> {
        let dir = TempDir::new()?;
        {
            let db: DB<SerdeObj> = DB::open(dir.path());
            let mut db = db.rw();
            assert_eq!(db.int, 0);
            assert_eq!(db.str, "".to_string());
            db.int = 2;
            db.str = "abc".to_string();
        }
        {
            let db: DB<SerdeObj> = DB::open(dir.path());
            let db = db.r();
            assert_eq!(db.int, 2);
            assert_eq!(db.str, "abc".to_string());
        }
        Ok(())
    }
}
