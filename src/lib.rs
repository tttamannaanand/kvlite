//! kvlite: a tiny in-memory key-value store over TCP, made durable by a
//! write-ahead log. See README.md for the protocol and design.

pub mod cluster;
pub mod codec;
pub mod linear;
pub mod protocol;
pub mod raft;
pub mod server;
pub mod sim;
pub mod store;
pub mod wal;
