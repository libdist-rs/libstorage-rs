use async_trait::async_trait;
use anyhow::Result;
use crate::{Key, Value};

#[async_trait]
pub trait Store: Send + Sync + Clone + 'static 
{
    /// Write a value to the disk
    async fn write(&mut self, key: Key, value: Value);

    /// Reading values from the disk
    /// This will return with an option indicating whether the value was found or not
    async fn read(&mut self, key: Key) -> Result<Option<Value>>;

    /// Wait for value to be available from the disk
    async fn notify_read(&mut self, key: Key) -> Result<Value>;
}