use std::{
    any::Any,
    borrow::Borrow,
    collections::BTreeMap,
    mem::take,
    ops::{Index, IndexMut},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    bigobject::{InternalClone, Key, KeyRef},
    storage::{Prefix, WithContext},
    BigObject,
};

#[derive(Serialize, Deserialize, Default)]
pub struct BigMap<K: Key, V: BigObject> {
    #[serde(skip)]
    prefix: Option<Prefix>,
    #[serde(skip)]
    changes: BTreeMap<K, Option<V>>,
}

impl<K: Key, V: BigObject> BigObject for BigMap<K, V>
where
    Self: Serialize + DeserializeOwned + Any,
{
    fn initialize<F: FnOnce() -> Prefix>(&mut self, prefix: F) {
        self.prefix = Some(prefix());
    }

    fn finalize<F: FnOnce() -> Prefix>(&mut self, prefix: F, batch: &mut crate::storage::Batch) {
        let prefix = self.prefix.get_or_insert_with(|| {
            let prefix = prefix();
            batch.delete(&prefix);
            prefix
        });
        for (key, value) in take(&mut self.changes).into_iter() {
            if let Some(mut value) = value {
                let child = prefix.child(&key);
                value.finalize(|| child.clone(), batch);
                batch.put(child, value);
            } else {
                batch.delete(&prefix.child(&key));
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

impl<K, Q, V> Index<&Q> for BigMap<K, V>
where
    K: Borrow<Q> + Key,
    Q: KeyRef + ?Sized,
    Self: WithContext<Key = K, Value = V>,
    V: BigObject,
{
    type Output = V;

    fn index(&self, key: &Q) -> &V {
        self.get(key).unwrap()
    }
}

impl<K, Q, V> IndexMut<&Q> for BigMap<K, V>
where
    K: Borrow<Q> + Key,
    Q: KeyRef + ?Sized + ToOwned<Owned = K>,
    Self: WithContext<Key = K, Value = V>,
    V: BigObject,
{
    fn index_mut(&mut self, key: &Q) -> &mut V {
        self.get_mut(key).unwrap()
    }
}

impl<K: Key, V: BigObject> BigMap<K, V>
where
    Self: WithContext<Key = K, Value = V>,
{
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: KeyRef + ?Sized,
    {
        self.changes.get(key).map_or_else(
            || {
                self.prefix
                    .as_ref()
                    .and_then(|prefix| self.get_child(prefix, key))
            },
            |value| value.as_ref(),
        )
    }
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: KeyRef + ?Sized + ToOwned<Owned = K>,
    {
        if !self.changes.contains_key(key) {
            self.changes.insert(
                key.to_owned(),
                self.prefix.as_ref().and_then(|prefix| {
                    self.get_child(prefix, key)
                        .map(|value| value.internal_clone())
                }),
            );
        }
        self.changes.get_mut(key).unwrap().as_mut()
    }
    pub fn insert(&mut self, key: K, value: V) {
        self.changes.insert(key, Some(value));
    }
    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: KeyRef + ?Sized + ToOwned<Owned = K>,
    {
        match self.changes.get_mut(key) {
            Some(value) => {
                *value = None;
            }
            None => {
                self.changes.insert(key.to_owned(), None);
            }
        };
    }
    pub fn clear(&mut self) {
        self.prefix = None;
        self.changes = BTreeMap::new();
    }
}
