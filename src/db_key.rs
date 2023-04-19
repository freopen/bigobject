use std::io::Write;

use crate::bigobject::bigmap::KeyRef;

pub fn split_db_key(key: &[u8]) -> (&[u8], &[u8]) {
    let len = key.len();
    if len == 0 {
        return (key, key);
    }
    match key[len - 1] {
        0x0..=0x7F => {
            let prefix = key[len - 1] as usize;
            (&key[..prefix], &key[prefix..len - 1])
        }
        0x80..=0xBF => {
            let prefix = u16::from_le_bytes(key[len - 2..].try_into().unwrap()) as usize & !0x8000;
            (&key[..prefix], &key[prefix..len - 2])
        }
        0xC0..=0xDF => {
            let prefix =
                u32::from_le_bytes(key[len - 4..].try_into().unwrap()) as usize & !0xC0000000;
            (&key[..prefix], &key[prefix..len - 4])
        }
        _ => unreachable!(),
    }
}

pub fn append_map_key<K: KeyRef>(db_key: &mut Vec<u8>, map_key: &K) {
    storekey::serialize_into(db_key.by_ref(), map_key).unwrap();
}

pub fn append_prefix_len(key: &mut Vec<u8>, prefix_len: usize) {
    match prefix_len {
        0x0..=0x7F => {
            key.push(prefix_len as u8);
        }
        0x80..=0x3FFF => {
            let prefix_len = (prefix_len as u16) | 0x8000;
            key.extend_from_slice(&prefix_len.to_le_bytes())
        }
        0x4000..=0x1FFFFFFF => {
            let prefix_len = (prefix_len as u32) | 0xC0000000;
            key.extend_from_slice(&prefix_len.to_le_bytes())
        }
        _ => unimplemented!("Database key is too big"),
    }
}
