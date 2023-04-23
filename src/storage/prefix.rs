use std::io::Write;

use crate::{bigobject::bigmap::KeyRef, storage::lock_context::LockContext};

pub struct Prefix(pub(crate) Vec<u8>);

impl Prefix {
    pub fn push_field_index(&mut self) {
        self.0.push(0);
    }
    pub fn set_field_index(&mut self, index: u8) {
        *self.0.last_mut().unwrap() = index;
    }
    pub fn pop_field_index(&mut self) {
        self.0.pop();
    }
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }
    pub(crate) fn clone(&self) -> Self {
        Self(self.0.clone())
    }
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
    pub(crate) fn extract_prefix(key: &[u8]) -> &[u8] {
        let len = key.len();
        if len == 0 {
            return key;
        }
        match key[len - 1] {
            0x0..=0x7F => {
                let prefix = key[len - 1] as usize;
                &key[..prefix]
            }
            0x80..=0xBF => {
                let prefix =
                    u16::from_le_bytes(key[len - 2..].try_into().unwrap()) as usize & !0x8000;
                &key[..prefix]
            }
            0xC0..=0xDF => {
                let prefix =
                    u32::from_le_bytes(key[len - 4..].try_into().unwrap()) as usize & !0xC0000000;
                &key[..prefix]
            }
            _ => unreachable!(),
        }
    }
    pub(crate) fn append_map_key<K: KeyRef>(&mut self, map_key: &K) -> usize {
        let prefix_len = self.0.len();
        self.0.push(1);
        storekey::serialize_into(self.0.by_ref(), map_key).unwrap();
        prefix_len
    }
    pub(crate) fn next_prefix(&self) -> Prefix {
        let next = if let Some(nonff) = self.0.iter().rposition(|&byte| byte < u8::MAX) {
            let mut next = self.0[..nonff].to_vec();
            *next.last_mut().unwrap() += 1;
            next
        } else if let Some(mut next) = LockContext::last_key() {
            next.push(0);
            next
        } else {
            vec![]
        };
        Prefix(next)
    }
    pub(crate) fn into_leaf(mut self, prefix_len: usize) -> Vec<u8> {
        if prefix_len != self.0.len() {
            self.0[prefix_len] = 0;
        }
        match prefix_len {
            0x0..=0x7F => {
                self.0.push(prefix_len as u8);
            }
            0x80..=0x3FFF => {
                let prefix_len = (prefix_len as u16) | 0x8000;
                self.0.extend_from_slice(&prefix_len.to_le_bytes())
            }
            0x4000..=0x1FFFFFFF => {
                let prefix_len = (prefix_len as u32) | 0xC0000000;
                self.0.extend_from_slice(&prefix_len.to_le_bytes())
            }
            _ => unimplemented!("Database key is too big"),
        }
        self.0
    }
    pub(crate) fn from_leaf(mut leaf: Vec<u8>, prefix_len: usize) -> Self {
        leaf[prefix_len] = 1;
        match prefix_len {
            0x0..=0x7F => {
                leaf.truncate(leaf.len() - 1);
            }
            0x80..=0x3FFF => {
                leaf.truncate(leaf.len() - 2);
            }
            0x4000..=0x1FFFFFFF => {
                leaf.truncate(leaf.len() - 4);
            }
            _ => unimplemented!("Database key is too big"),
        }
        Self(leaf)
    }
}
