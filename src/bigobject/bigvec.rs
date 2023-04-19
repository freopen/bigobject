use std::ops::{Index, IndexMut};

use bigobject_derive::BigObject;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate as bigobject;
use crate::{bigobject::BigObject, BigMap};

#[derive(BigObject)]
pub struct BigVec<T: BigObject> {
    len: u64,
    data: BigMap<u64, T>,
}

impl<V: BigObject> Serialize for BigVec<V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.len.serialize(serializer)
    }
}

impl<'a, V: BigObject> Deserialize<'a> for BigVec<V> {
    fn deserialize<D: Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self {
            len: u64::deserialize(deserializer)?,
            data: BigMap::default(),
        })
    }
}

impl<T: BigObject> Default for BigVec<T> {
    fn default() -> Self {
        Self {
            len: 0,
            data: BigMap::default(),
        }
    }
}

impl<T: BigObject> Index<u64> for BigVec<T> {
    type Output = T;

    fn index(&self, index: u64) -> &T {
        &self.data[&index]
    }
}

impl<T: BigObject> IndexMut<u64> for BigVec<T> {
    fn index_mut(&mut self, index: u64) -> &mut T {
        &mut self.data[&index]
    }
}

pub struct Iter<'a, T: BigObject> {
    data: &'a BigMap<u64, T>,
    index: u64,
    end: u64,
}

impl<'a, T: BigObject> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            let index = self.index;
            self.index += 1;
            Some(&self.data[&index])
        } else {
            None
        }
    }
}

impl<'a, T: BigObject> DoubleEndedIterator for Iter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index < self.end {
            self.end -= 1;
            Some(&self.data[&self.end])
        } else {
            None
        }
    }
}

impl<'a, T: BigObject> ExactSizeIterator for Iter<'a, T> {
    fn len(&self) -> usize {
        (self.end - self.index) as usize
    }
}

impl<T: BigObject> BigVec<T> {
    pub fn len(&self) -> u64 {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn iter(&self) -> Iter<T> {
        Iter {
            data: &self.data,
            index: 0,
            end: self.len,
        }
    }
    pub fn push(&mut self, value: T) {
        self.data.insert(self.len, value);
        self.len += 1;
    }
    pub fn truncate(&mut self, len: u64) {
        if len >= self.len {
            return;
        }
        (len..self.len).map(|i| self.data.remove(&i)).count();
        self.len = len;
    }
}
