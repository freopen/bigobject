use std::{
    any::Any,
    borrow::Borrow,
    collections::BTreeMap,
    mem::take,
    ops::{Index, IndexMut},
};

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    bigobject::BigObject,
    storage::{
        batch::Batch,
        lock_context::{LockContext, PhantomContext},
        prefix::Prefix,
    },
};

pub trait KeyRef: Serialize + Ord {}
impl<T: Serialize + Ord + ?Sized> KeyRef for T {}

pub trait Key: Serialize + DeserializeOwned + Ord + Clone + 'static {}
impl<T: Serialize + DeserializeOwned + Ord + Clone + 'static> Key for T {}

pub struct BigMap<K: Key, V: BigObject> {
    prefix: Option<Prefix>,
    changes: BTreeMap<K, Option<V>>,
    _phantom: PhantomContext,
}

impl<K: Key, V: BigObject> Default for BigMap<K, V> {
    fn default() -> Self {
        Self {
            prefix: None,
            changes: BTreeMap::new(),
            _phantom: Default::default(),
        }
    }
}

impl<K: Key, V: BigObject> Serialize for BigMap<K, V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

impl<'a, K: Key, V: BigObject> Deserialize<'a> for BigMap<K, V> {
    fn deserialize<D: Deserializer<'a>>(_deserializer: D) -> Result<Self, D::Error> {
        Ok(Self {
            prefix: None,
            changes: BTreeMap::new(),
            _phantom: Default::default(),
        })
    }
}

impl<K, V> BigObject for BigMap<K, V>
where
    Self: Serialize + DeserializeOwned + Any,
    K: Key,
    V: BigObject,
{
    fn initialize<'a, F: FnOnce() -> &'a mut Prefix>(&mut self, prefix: F) {
        self.prefix = Some(prefix().clone());
    }

    fn finalize<'a, F: FnOnce() -> &'a mut Prefix>(&mut self, prefix: F, batch: &mut Batch) {
        let prefix = self.prefix.get_or_insert_with(|| {
            let prefix = prefix().clone();
            batch.delete_prefix(&prefix);
            prefix
        });
        for (key, value) in take(&mut self.changes).into_iter() {
            if let Some(value) = value {
                batch.put(prefix, &key, value);
            } else {
                batch.delete(prefix, &key);
            }
        }
    }
    fn big_clone(&self) -> Self {
        assert!(self.changes.is_empty());
        Self {
            prefix: self.prefix.as_ref().map(|prefix| prefix.clone()),
            changes: BTreeMap::new(),
            _phantom: Default::default(),
        }
    }
}

impl<K, Q, V> Index<&Q> for BigMap<K, V>
where
    K: Borrow<Q> + Key,
    Q: KeyRef + ?Sized,
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
    V: BigObject,
{
    fn index_mut(&mut self, key: &Q) -> &mut V {
        self.get_mut(key).unwrap()
    }
}

impl<K: Key, V: BigObject> BigMap<K, V> {
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: KeyRef + ?Sized,
    {
        self.changes.get(key).map_or_else(
            || {
                self.prefix
                    .as_ref()
                    .and_then(|prefix| LockContext::get(prefix, &key))
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
                self.prefix
                    .as_ref()
                    .and_then(|prefix| LockContext::get(prefix, &key).map(V::big_clone)),
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
