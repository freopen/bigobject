pub mod bigmap;
pub mod bigvec;

use std::any::Any;

use serde::{de::DeserializeOwned, Serialize};

use crate::storage::{batch::Batch, prefix::Prefix};

pub trait BigObject: Serialize + DeserializeOwned + Any {
    fn initialize<'a, F: FnOnce() -> &'a mut Prefix>(&mut self, prefix: F);
    fn finalize<'a, F: FnOnce() -> &'a mut Prefix>(&mut self, prefix: F, batch: &mut Batch);
    fn big_clone(&self) -> Self;
}

impl<T: Serialize + DeserializeOwned + Any + Clone> BigObject for T {
    fn initialize<'a, F: FnOnce() -> &'a mut Prefix>(&mut self, _prefix: F) {}
    fn finalize<'a, F: FnOnce() -> &'a mut Prefix>(&mut self, _prefix: F, _batch: &mut Batch) {}
    fn big_clone(&self) -> Self {
        self.clone()
    }
}
