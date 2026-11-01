//! Binary encoding for Raft messages and log entries, used both on the wire
//! between nodes and in each node's on-disk Raft log.
//!
//! Integers are little-endian u64; strings are a u32 length then UTF-8 bytes.

use std::io::{self, Read, Write};

use crate::raft::{Entry, Envelope, Message, NodeId, Op};

pub fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }
    pub fn done(&self) -> bool {
        self.pos >= self.buf.len()
    }
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    pub fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn str(&mut self) -> io::Result<String> {
        let n = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        String::from_utf8(self.take(n)?.to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

pub fn put_entry(buf: &mut Vec<u8>, e: &Entry) {
    put_u64(buf, e.term);
    match &e.op {
        Op::Noop => buf.push(0),
        Op::Set(k, v) => {
            buf.push(1);
            put_str(buf, k);
            put_str(buf, v);
        }
        Op::Del(k) => {
            buf.push(2);
            put_str(buf, k);
        }
        Op::Get(k) => {
            buf.push(3);
            put_str(buf, k);
        }
    }
}

pub fn get_entry(r: &mut Reader) -> io::Result<Entry> {
    let term = r.u64()?;
    let op = match r.u8()? {
        0 => Op::Noop,
        1 => Op::Set(r.str()?, r.str()?),
        2 => Op::Del(r.str()?),
        3 => Op::Get(r.str()?),
        t => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad op {t}"),
            ))
        }
    };
    Ok(Entry { term, op })
}

pub fn encode(env: &Envelope) -> Vec<u8> {
    let mut b = Vec::new();
    put_u64(&mut b, env.from);
    put_u64(&mut b, env.to);
    match &env.msg {
        Message::RequestVote {
            term,
            last_index,
            last_term,
        } => {
            b.push(0);
            put_u64(&mut b, *term);
            put_u64(&mut b, *last_index);
            put_u64(&mut b, *last_term);
        }
        Message::Vote { term, granted } => {
            b.push(1);
            put_u64(&mut b, *term);
            b.push(u8::from(*granted));
        }
        Message::Append {
            term,
            prev_index,
            prev_term,
            entries,
            commit,
        } => {
            b.push(2);
            put_u64(&mut b, *term);
            put_u64(&mut b, *prev_index);
            put_u64(&mut b, *prev_term);
            put_u64(&mut b, *commit);
            put_u64(&mut b, entries.len() as u64);
            for e in entries {
                put_entry(&mut b, e);
            }
        }
        Message::AppendReply {
            term,
            success,
            hint,
        } => {
            b.push(3);
            put_u64(&mut b, *term);
            b.push(u8::from(*success));
            put_u64(&mut b, *hint);
        }
    }
    b
}

pub fn decode(buf: &[u8]) -> io::Result<Envelope> {
    let mut r = Reader::new(buf);
    let from: NodeId = r.u64()?;
    let to: NodeId = r.u64()?;
    let msg = match r.u8()? {
        0 => Message::RequestVote {
            term: r.u64()?,
            last_index: r.u64()?,
            last_term: r.u64()?,
        },
        1 => Message::Vote {
            term: r.u64()?,
            granted: r.u8()? == 1,
        },
        2 => {
            let (term, prev_index, prev_term, commit) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
            let n = r.u64()?;
            let mut entries = Vec::with_capacity(n.min(1024) as usize);
            for _ in 0..n {
                entries.push(get_entry(&mut r)?);
            }
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            }
        }
        3 => Message::AppendReply {
            term: r.u64()?,
            success: r.u8()? == 1,
            hint: r.u64()?,
        },
        t => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad message {t}"),
            ))
        }
    };
    Ok(Envelope { from, to, msg })
}

/// Length-prefixed frame on a stream.
pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(payload)
}

pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let n = u32::from_le_bytes(len) as usize;
    if n > 64 << 20 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Entry;

    #[test]
    fn every_message_round_trips() {
        let entries = vec![
            Entry {
                term: 3,
                op: Op::Noop,
            },
            Entry {
                term: 3,
                op: Op::Set("k".into(), "v with spaces ☕".into()),
            },
            Entry {
                term: 4,
                op: Op::Del("k".into()),
            },
            Entry {
                term: 4,
                op: Op::Get("k".into()),
            },
        ];
        for msg in [
            Message::RequestVote {
                term: 9,
                last_index: 7,
                last_term: 4,
            },
            Message::Vote {
                term: 9,
                granted: true,
            },
            Message::Append {
                term: 9,
                prev_index: 6,
                prev_term: 4,
                entries,
                commit: 5,
            },
            Message::AppendReply {
                term: 9,
                success: false,
                hint: 3,
            },
        ] {
            let env = Envelope {
                from: 1,
                to: 2,
                msg,
            };
            assert_eq!(decode(&encode(&env)).unwrap(), env);
        }
    }

    #[test]
    fn truncated_input_is_an_error_not_a_panic() {
        let env = Envelope {
            from: 1,
            to: 2,
            msg: Message::Vote {
                term: 1,
                granted: true,
            },
        };
        let bytes = encode(&env);
        for cut in 0..bytes.len() {
            assert!(decode(&bytes[..cut]).is_err());
        }
    }
}
