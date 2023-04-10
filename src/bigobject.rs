use rocksdb::WriteBatch;
use serde::{de::DeserializeOwned, Serialize};

use crate::db::DBInner;

pub trait BigObject: Serialize + DeserializeOwned {
    fn attach<'val, 'db: 'val>(&'val mut self, _db: &'db DBInner, _prefix: Vec<u8>) {}
    fn finalize(&mut self, _batch: &mut WriteBatch) {}
}

impl<T: Serialize + DeserializeOwned> BigObject for T {
    default fn attach<'val, 'db: 'val>(&'val mut self, _db: &'db DBInner, _prefix: Vec<u8>) {}
    default fn finalize(&mut self, _batch: &mut WriteBatch) {}
}
