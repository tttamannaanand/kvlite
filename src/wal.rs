//! Append-only write-ahead log.
//!
//! Record layout (little-endian):
//!   op: u8 | key_len: u32 | key | val_len: u32 | val
//! `val_len`/`val` are present only for SET. A truncated record at the tail
//! (e.g. from a crash mid-write) is ignored on replay.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Set(String, String),
    Del(String),
}

pub struct Wal {
    path: PathBuf,
    file: File,
    sync: bool,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>, sync: bool) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { path, file, sync })
    }

    pub fn append(&mut self, rec: &Record) -> io::Result<()> {
        let mut buf = Vec::new();
        encode(rec, &mut buf);
        self.file.write_all(&buf)?;
        if self.sync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// Replays the log into a map. Stops cleanly at a truncated tail.
    pub fn replay(path: impl AsRef<Path>) -> io::Result<HashMap<String, String>> {
        let mut map = HashMap::new();
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(map),
            Err(e) => return Err(e),
        };
        let mut r = BufReader::new(file);
        while let Some(rec) = decode(&mut r)? {
            match rec {
                Record::Set(k, v) => {
                    map.insert(k, v);
                }
                Record::Del(k) => {
                    map.remove(&k);
                }
            }
        }
        Ok(map)
    }

    /// Rewrites the log so it holds exactly one SET per live key.
    pub fn compact(&mut self, snapshot: &HashMap<String, String>) -> io::Result<()> {
        let tmp = self.path.with_extension("wal.tmp");
        {
            let mut w = BufWriter::new(File::create(&tmp)?);
            let mut buf = Vec::new();
            for (k, v) in snapshot {
                buf.clear();
                encode(&Record::Set(k.clone(), v.clone()), &mut buf);
                w.write_all(&buf)?;
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        self.file = OpenOptions::new().append(true).open(&self.path)?;
        Ok(())
    }
}

fn encode(rec: &Record, buf: &mut Vec<u8>) {
    let put = |buf: &mut Vec<u8>, s: &str| {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    };
    match rec {
        Record::Set(k, v) => {
            buf.push(OP_SET);
            put(buf, k);
            put(buf, v);
        }
        Record::Del(k) => {
            buf.push(OP_DEL);
            put(buf, k);
        }
    }
}

/// Returns Ok(None) at EOF or on a truncated record.
fn decode(r: &mut impl Read) -> io::Result<Option<Record>> {
    let mut op = [0u8; 1];
    if !read_full(r, &mut op)? {
        return Ok(None);
    }
    let Some(key) = read_str(r)? else {
        return Ok(None);
    };
    match op[0] {
        OP_SET => Ok(read_str(r)?.map(|v| Record::Set(key, v))),
        OP_DEL => Ok(Some(Record::Del(key))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad wal op {other}"),
        )),
    }
}

fn read_str(r: &mut impl Read) -> io::Result<Option<String>> {
    let mut len = [0u8; 4];
    if !read_full(r, &mut len)? {
        return Ok(None);
    }
    let mut bytes = vec![0u8; u32::from_le_bytes(len) as usize];
    if !read_full(r, &mut bytes)? {
        return Ok(None);
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Like read_exact, but reports a short read as Ok(false) instead of an error.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    match r.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("kvlite-{name}-{}.wal", std::process::id()));
        let _ = fs::remove_file(&p);
        p
    }

    #[test]
    fn replay_applies_sets_and_dels() {
        let p = tmp("replay");
        let mut wal = Wal::open(&p, false).unwrap();
        wal.append(&Record::Set("a".into(), "1".into())).unwrap();
        wal.append(&Record::Set("b".into(), "2".into())).unwrap();
        wal.append(&Record::Del("a".into())).unwrap();
        let map = Wal::replay(&p).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["b"], "2");
    }

    #[test]
    fn truncated_tail_is_ignored() {
        let p = tmp("trunc");
        let mut wal = Wal::open(&p, false).unwrap();
        wal.append(&Record::Set("k".into(), "v".into())).unwrap();
        wal.append(&Record::Set("k2".into(), "long value".into()))
            .unwrap();
        let len = fs::metadata(&p).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&p)
            .unwrap()
            .set_len(len - 3)
            .unwrap();
        let map = Wal::replay(&p).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map["k"], "v");
    }

    #[test]
    fn compact_keeps_live_keys_only() {
        let p = tmp("compact");
        let mut wal = Wal::open(&p, false).unwrap();
        for i in 0..100 {
            wal.append(&Record::Set("x".into(), i.to_string())).unwrap();
        }
        let before = fs::metadata(&p).unwrap().len();
        let snap = Wal::replay(&p).unwrap();
        wal.compact(&snap).unwrap();
        assert!(fs::metadata(&p).unwrap().len() < before);
        assert_eq!(Wal::replay(&p).unwrap()["x"], "99");
    }
}
