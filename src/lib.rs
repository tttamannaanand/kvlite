//! kvlite: a tiny in-memory key-value store over TCP, made durable by a
//! write-ahead log. See README.md for the protocol and design.

pub mod protocol;
pub mod server;
pub mod store;
pub mod wal;
