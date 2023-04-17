use std::any::Any;

use serde::{de::DeserializeOwned, Serialize};

use crate::storage::Batch;

pub trait KeyRef: Serialize + Ord {}
impl<T: Serialize + Ord + ?Sized> KeyRef for T {}

pub trait Key: Serialize + DeserializeOwned + Ord + Clone + 'static {}
impl<T: Serialize + DeserializeOwned + Ord + Clone + 'static> Key for T {}

pub trait BigObject: Serialize + DeserializeOwned + Any {
    fn initialize<F: FnOnce() -> Vec<u8>>(&mut self, prefix: F);
    fn finalize<F: FnOnce() -> Vec<u8>>(&mut self, prefix: F, batch: &mut Batch);
    fn big_clone(&self) -> Self;
}

impl<T: Serialize + DeserializeOwned + Any + Clone> BigObject for T {
    fn initialize<F: FnOnce() -> Vec<u8>>(&mut self, _prefix: F) {}
    fn finalize<F: FnOnce() -> Vec<u8>>(&mut self, _prefix: F, _batch: &mut Batch) {}
    fn big_clone(&self) -> Self {
        self.clone()
    }
}
