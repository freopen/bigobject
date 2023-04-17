#![feature(min_specialization)]
#![feature(rustc_attrs)]

mod bigmap;
mod bigobject;
mod storage;

pub use crate::{bigmap::BigMap, storage::Db};
pub use bigobject_derive::BigObject;

pub mod internal {
    pub use crate::{bigobject::BigObject, storage::Batch};
}
