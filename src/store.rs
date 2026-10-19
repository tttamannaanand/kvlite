//! The in-memory map, kept durable by the write-ahead log.
//!
//! One mutex guards both the map and the log, so the order records hit the
//! log is exactly the order they were applied. Every write goes to the log
//! *before* the map: if the append fails, the map is left untouched.
//!
//! ## Group commit
//!
//! fsync is slow (milliseconds), so doing one per write while holding the
//! lock caps throughput at a few hundred writes a second. With
//! [`Durability::Group`], a write appends its record under the lock, takes a
//! sequence number, releases the lock, and then waits until the log is
//! synced up to that number. The first waiter becomes the *leader*: it
//! fsyncs once, covering every record written so far, and wakes everyone
//! that fsync made durable. Writers that arrive during a sync queue up for
//! the next one, so under load one fsync acknowledges many writes.
//!
//! Each write is still acknowledged only once it is on disk. One trade-off:
//! a concurrent reader can see a value a few milliseconds before the writer
//! is told it is durable.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Condvar, Mutex, MutexGuard};

use crate::wal::{Record, Wal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Batch concurrent writes into shared fsyncs. Durable and fast.
    Group,
    /// fsync every write while holding the lock. Durable and slow.
    EachWrite,
    /// Never fsync. Survives process crashes, not power loss.
    None,
}

pub struct Store {
    inner: Mutex<Inner>,
    durability: Durability,
    sync: Mutex<SyncState>,
    synced: Condvar,
}

struct Inner {
    map: HashMap<String, String>,
    wal: Wal,
    /// Sequence number of the last record appended to the log.
    written: u64,
}

struct SyncState {
    /// Every record up to this sequence number is on disk.
    synced: u64,
    /// A leader is currently running an fsync.
    in_progress: bool,
    /// Set when a sync fails, so every waiter for it sees the error.
    failed: Option<String>,
}

impl Store {
    /// Replays any existing log at `path`, then opens it for appending.
    pub fn open(path: impl AsRef<Path>, durability: Durability) -> io::Result<Self> {
        let map = Wal::replay(&path)?;
        let wal = Wal::open(&path, durability == Durability::EachWrite)?;
        Ok(Self {
            inner: Mutex::new(Inner {
                map,
                wal,
                written: 0,
            }),
            durability,
            sync: Mutex::new(SyncState {
                synced: 0,
                in_progress: false,
                failed: None,
            }),
            synced: Condvar::new(),
        })
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.lock().map.get(key).cloned()
    }

    pub fn set(&self, key: String, value: String) -> io::Result<()> {
        let seq = {
            let mut inner = self.lock();
            inner.wal.append(&Record::Set(key.clone(), value.clone()))?;
            inner.map.insert(key, value);
            inner.written += 1;
            inner.written
        };
        self.wait_durable(seq)
    }

    /// Returns whether the key existed. Deleting a missing key writes nothing.
    pub fn del(&self, key: &str) -> io::Result<bool> {
        let seq = {
            let mut inner = self.lock();
            if !inner.map.contains_key(key) {
                return Ok(false);
            }
            inner.wal.append(&Record::Del(key.to_string()))?;
            inner.map.remove(key);
            inner.written += 1;
            inner.written
        };
        self.wait_durable(seq).map(|()| true)
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
        let Inner { map, wal, written } = &mut *inner;
        wal.compact(map)?;
        // compact() fsyncs the new file, so everything written so far is
        // durable. Lock order is always inner -> sync; leaders never take
        // inner while holding sync.
        let mut sync = self.sync_state();
        sync.synced = sync.synced.max(*written);
        self.synced.notify_all();
        Ok(())
    }

    /// Blocks until record `seq` is on disk (in Group mode).
    fn wait_durable(&self, seq: u64) -> io::Result<()> {
        if self.durability != Durability::Group {
            return Ok(()); // EachWrite already synced; None never does
        }
        let mut sync = self.sync_state();
        loop {
            if sync.synced >= seq {
                return Ok(());
            }
            if let Some(msg) = &sync.failed {
                return Err(io::Error::other(msg.clone()));
            }
            if sync.in_progress {
                sync = self.synced.wait(sync).unwrap_or_else(|e| e.into_inner());
                continue;
            }

            // Become the leader. Read the target and the file handle under
            // the same lock, so a concurrent compaction can't swap the file
            // between them: every record up to `target` is then in this
            // file's page cache, and one fsync covers it all.
            sync.in_progress = true;
            drop(sync);
            let handle = {
                let inner = self.lock();
                inner.wal.sync_handle().map(|f| (inner.written, f))
            };
            let result = handle.and_then(|(target, file)| file.sync_data().map(|()| target));

            sync = self.sync_state();
            sync.in_progress = false;
            match result {
                Ok(target) => sync.synced = sync.synced.max(target),
                Err(e) => sync.failed = Some(format!("fsync failed: {e}")),
            }
            self.synced.notify_all();
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic while holding the lock can't leave the map half-updated
        // (each write is a single insert/remove), so recover from poisoning.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn sync_state(&self) -> MutexGuard<'_, SyncState> {
        self.sync.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;

    fn tmp(name: &str) -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("kvlite-store-{name}-{}.wal", std::process::id()));
        let _ = fs::remove_file(&p);
        p
    }

    #[test]
    fn writes_survive_reopen_in_every_mode() {
        for mode in [Durability::Group, Durability::EachWrite, Durability::None] {
            let p = tmp(&format!("reopen-{mode:?}"));
            {
                let s = Store::open(&p, mode).unwrap();
                s.set("a".into(), "1".into()).unwrap();
                s.set("b".into(), "2".into()).unwrap();
                assert!(s.del("a").unwrap());
            }
            let s = Store::open(&p, mode).unwrap();
            assert_eq!(s.get("a"), None, "{mode:?}");
            assert_eq!(s.get("b").as_deref(), Some("2"), "{mode:?}");
            assert_eq!(s.len(), 1);
        }
    }

    #[test]
    fn deleting_a_missing_key_writes_nothing() {
        let p = tmp("del-missing");
        let s = Store::open(&p, Durability::Group).unwrap();
        assert!(!s.del("nope").unwrap());
        assert_eq!(fs::metadata(&p).unwrap().len(), 0);
    }

    #[test]
    fn compact_preserves_data() {
        let p = tmp("compact");
        let s = Store::open(&p, Durability::Group).unwrap();
        for i in 0..50 {
            s.set("k".into(), i.to_string()).unwrap();
        }
        s.compact().unwrap();
        s.set("after".into(), "yes".into()).unwrap();
        drop(s);
        let s = Store::open(&p, Durability::Group).unwrap();
        assert_eq!(s.get("k").as_deref(), Some("49"));
        assert_eq!(s.get("after").as_deref(), Some("yes"));
    }

    #[test]
    fn group_commit_acknowledges_every_concurrent_write() {
        let p = tmp("group");
        let s = Arc::new(Store::open(&p, Durability::Group).unwrap());
        let workers: Vec<_> = (0..16)
            .map(|w| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for i in 0..50 {
                        s.set(format!("{w}:{i}"), i.to_string()).unwrap();
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        // Every acknowledged write must be covered by a completed sync.
        let written = s.lock().written;
        assert_eq!(written, 800);
        assert!(s.sync_state().synced >= written);
        drop(s);
        assert_eq!(Store::open(&p, Durability::Group).unwrap().len(), 800);
    }

    #[test]
    fn compaction_during_group_commit_loses_nothing() {
        let p = tmp("group-compact");
        let s = Arc::new(Store::open(&p, Durability::Group).unwrap());
        let writers: Vec<_> = (0..4)
            .map(|w| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for i in 0..100 {
                        s.set(format!("{w}:{i}"), "v".into()).unwrap();
                    }
                })
            })
            .collect();
        for _ in 0..5 {
            s.compact().unwrap();
        }
        for w in writers {
            w.join().unwrap();
        }
        drop(s);
        assert_eq!(Store::open(&p, Durability::Group).unwrap().len(), 400);
    }
}
