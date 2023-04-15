use std::any::Any;

use serde::{de::DeserializeOwned, Serialize};

use crate::storage::{Batch, Prefix};

pub trait Key: Serialize + DeserializeOwned + Ord + Clone + 'static {}
impl<T: Serialize + DeserializeOwned + Ord + Clone + 'static> Key for T {}

pub trait BigObject: Serialize + DeserializeOwned + Any + InternalClone {
    fn initialize<F: FnOnce() -> Prefix>(&mut self, prefix: F);
    fn finalize(&mut self, batch: &mut Batch);
}

impl<T: Serialize + DeserializeOwned + Any + Clone> BigObject for T {
    default fn initialize<F: FnOnce() -> Prefix>(&mut self, _prefix: F) {}
    default fn finalize(&mut self, _batch: &mut Batch) {}
}

pub trait InternalClone {
    fn internal_clone(&self) -> Self;
}

impl<T: Clone> InternalClone for T {
    default fn internal_clone(&self) -> Self {
        self.clone()
    }
}
