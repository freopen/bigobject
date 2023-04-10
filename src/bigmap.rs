use std::ops::Index;

use elsa::FrozenBTreeMap;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{db::DBInner, BigObject};

pub struct BigMapInner<'a, K, V> {
    db: &'a DBInner,
    prefix: Vec<u8>,
    cache: FrozenBTreeMap<K, Box<V>>,
}

#[derive(Serialize, Deserialize)]
pub struct BigMap<'a, K, V> {
    #[serde(skip)]
    inner: Option<BigMapInner<'a, K, V>>,
}

impl<'a, K, V> BigObject for BigMap<'a, K, V>
where
    Self: Serialize + DeserializeOwned,
{
    fn attach<'val, 'db: 'val>(&'val mut self, _db: &'db DBInner, _prefix: Vec<u8>) {}

    fn finalize(&mut self, _batch: &mut rocksdb::WriteBatch) {}
}

impl<K: Serialize + DeserializeOwned + Ord + Clone, V: BigObject> Index<&K> for BigMap<'_, K, V> {
    type Output = V;

    fn index(&self, key: &K) -> &V {
        let inner = self.inner.as_ref().unwrap();
        inner.cache.get(key).unwrap_or_else(|| {
            let mut child_key = inner.prefix.clone();
            let separator_index = child_key.len();
            child_key.push(0);
            storekey::serialize_into(&mut child_key, key).unwrap();
            let mut value: V = inner.db.get(&child_key).unwrap();
            child_key[separator_index] = 1;
            value.attach(inner.db, child_key);
            inner.cache.insert(key.clone(), Box::new(value))
        })
    }
}
