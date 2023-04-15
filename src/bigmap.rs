use std::{
    any::Any,
    collections::BTreeMap,
    mem::take,
    ops::{Index, IndexMut},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    bigobject::{InternalClone, Key},
    storage::{Prefix, WithContext},
    BigObject,
};

#[derive(Serialize, Deserialize)]
pub struct BigMap<K: Key, V: BigObject> {
    #[serde(skip)]
    prefix: Prefix,
    #[serde(skip)]
    changes: BTreeMap<K, Option<V>>,
}

impl<K: Key, V: BigObject> BigObject for BigMap<K, V>
where
    Self: Serialize + DeserializeOwned + Any,
{
    fn initialize<F: FnOnce() -> Prefix>(&mut self, prefix: F) {
        self.prefix = prefix();
    }

    fn finalize(&mut self, batch: &mut crate::storage::Batch) {
        for (key, value) in take(&mut self.changes).into_iter() {
            if let Some(mut value) = value {
                value.finalize(batch);
            } else {
                batch.delete(&self.prefix.child(&key));
            }
        }
    }
}

impl<K: Key, V: BigObject> WithContext for BigMap<K, V>
where
    Self: BigObject,
{
    type Key = K;
    type Value = V;
}

impl<K: Key, V: BigObject> InternalClone for BigMap<K, V> {
    fn internal_clone(&self) -> Self {
        assert!(self.changes.is_empty());
        Self {
            prefix: self.prefix.clone(),
            changes: BTreeMap::new(),
        }
    }
}

impl<K: Key, V: BigObject> Index<&K> for BigMap<K, V>
where
    Self: WithContext<Key = K, Value = V>,
{
    type Output = V;

    fn index(&self, key: &K) -> &V {
        self.get(key).unwrap()
    }
}

impl<K: Key, V: BigObject> IndexMut<&K> for BigMap<K, V>
where
    Self: WithContext<Key = K, Value = V>,
{
    fn index_mut(&mut self, key: &K) -> &mut V {
        self.get_mut(key).unwrap()
    }
}

impl<K: Key, V: BigObject> BigMap<K, V>
where
    Self: WithContext<Key = K, Value = V>,
{
    pub fn get(&self, key: &K) -> Option<&V> {
        match self.changes.get(key) {
            Some(value) => value.as_ref(),
            None => self.get_child(&self.prefix, key),
        }
    }
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if !self.changes.contains_key(key) {
            self.changes.insert(
                key.clone(),
                self.get_child(&self.prefix, key)
                    .map(|value| value.internal_clone()),
            );
        }
        self.changes.get_mut(key).unwrap().as_mut()
    }
    pub fn insert(&mut self, key: K, value: V) {
        self.changes.insert(key, Some(value));
    }
    pub fn remove(&mut self, key: &K) {
        self.changes.insert(key.clone(), None);
    }
}
