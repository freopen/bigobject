#![feature(min_specialization)]
#![feature(rustc_attrs)]

mod bigmap;
mod bigobject;
mod storage;

pub use crate::{bigmap::BigMap, bigobject::BigObject, storage::Db};
pub use bigobject_derive::BigObject;

pub mod internal {
    pub use crate::{
        bigobject::InternalClone,
        storage::{Batch, Prefix},
    };
}
