use tokio::sync::oneshot;

type Key = Vec<u8>;
type Value = Vec<u8>;
type Notification = anyhow::Result<Value>;
type ReadResult = anyhow::Result<Option<Value>>;

pub enum StoreCommand {
    Write(Key, Value),
    Read(Key, oneshot::Sender<ReadResult>),
    NotifyRead(Key, oneshot::Sender<Notification>),
}

mod traits;
pub use traits::*;

#[cfg(feature = "rocksdb")]
mod rocksdb_impl;
#[cfg(feature = "rocksdb")]
pub mod rocksdb { pub use crate::rocksdb_impl::*; }

#[cfg(feature = "mdbx")]
pub mod mdbx;