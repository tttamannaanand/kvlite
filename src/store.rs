//! The in-memory map, kept durable by the write-ahead log.
//!
//! One mutex guards both the map and the log, so the order records hit the
//! log is exactly the order they were applied. Every write goes to the log
//! *before* the map: if the append fails, the map is left untouched.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Mutex;

use crate::wal::{Record, Wal};

pub struct Store {
    inner: Mutex<Inner>,
}

struct Inner {
    map: HashMap<String, String>,
    wal: Wal,
}

impl Store {
    /// Replays any existing log at `path`, then opens it for appending.
    pub fn open(path: impl AsRef<Path>, sync: bool) -> io::Result<Self> {
        let map = Wal::replay(&path)?;
        let wal = Wal::open(&path, sync)?;
        Ok(Self {
            inner: Mutex::new(Inner { map, wal }),
        })
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.lock().map.get(key).cloned()
    }

    pub fn set(&self, key: String, value: String) -> io::Result<()> {
        let mut inner = self.lock();
        inner.wal.append(&Record::Set(key.clone(), value.clone()))?;
        inner.map.insert(key, value);
        Ok(())
    }

    /// Returns whether the key existed. Deleting a missing key writes nothing.
    pub fn del(&self, key: &str) -> io::Result<bool> {
        let mut inner = self.lock();
        if !inner.map.contains_key(key) {
            return Ok(false);
        }
        inner.wal.append(&Record::Del(key.to_string()))?;
        inner.map.remove(key);
        Ok(true)
    }

    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Rewrites the log to one record per live key.
    pub fn compact(&self) -> io::Result<()> {
        let mut inner = self.lock();
        let Inner { map, wal } = &mut *inner;
        wal.compact(map)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding the lock can't leave the map half-updated
        // (each write is a single insert/remove), so recover from poisoning.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn tmp(name: &str) -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("kvlite-store-{name}-{}.wal", std::process::id()));
        let _ = fs::remove_file(&p);
        p
    }

    #[test]
    fn writes_survive_reopen() {
        let p = tmp("reopen");
        {
            let s = Store::open(&p, false).unwrap();
            s.set("a".into(), "1".into()).unwrap();
            s.set("b".into(), "2".into()).unwrap();
            assert!(s.del("a").unwrap());
        }
        let s = Store::open(&p, false).unwrap();
        assert_eq!(s.get("a"), None);
        assert_eq!(s.get("b").as_deref(), Some("2"));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn deleting_a_missing_key_writes_nothing() {
        let p = tmp("del-missing");
        let s = Store::open(&p, false).unwrap();
        assert!(!s.del("nope").unwrap());
        assert_eq!(fs::metadata(&p).unwrap().len(), 0);
    }

    #[test]
    fn compact_preserves_data() {
        let p = tmp("compact");
        let s = Store::open(&p, false).unwrap();
        for i in 0..50 {
            s.set("k".into(), i.to_string()).unwrap();
        }
        s.compact().unwrap();
        s.set("after".into(), "yes".into()).unwrap();
        drop(s);
        let s = Store::open(&p, false).unwrap();
        assert_eq!(s.get("k").as_deref(), Some("49"));
        assert_eq!(s.get("after").as_deref(), Some("yes"));
    }
}
