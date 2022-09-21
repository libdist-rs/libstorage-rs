use std::collections::{HashMap, VecDeque};
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use crate::{StoreCommand, Value, Key};
use crate::Store;

#[derive(Clone)]
pub struct Storage {
    channel: Sender<StoreCommand>,
}

impl Storage {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        let db = rocksdb::DB::open(&db_opts, path)?;
        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value) => {
                        let _ = db.put(&key, &value);
                        if let Some(mut senders) = obligations.remove(&key) {
                            while let Some(s) = senders.pop_front() {
                                let _ = s.send(Ok(value.clone()));
                            }
                        }
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(&key);
                        let _ = sender.send(response.map_err(|e| e.into()));
                    }
                    StoreCommand::NotifyRead(key, sender) => {
                        let response = db.get(&key);
                        match response {
                            Ok(None) => obligations
                                .entry(key)
                                .or_insert_with(VecDeque::new)
                                .push_back(sender),
                            _ => {
                                let _ = sender.send(
                                            response.map(|x| x.unwrap())
                                                .map_err(|e| e.into())
                                        );
                            }
                        }
                    }
                }
            }
            log::info!("The database is shutting down!");
            db.flush()
                .expect("Failed to flush the database after dropping");
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
            panic!("Failed to send Write command to store: {}", e);
        }
    }

    pub async fn read(&mut self, key: Key) -> anyhow::Result<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read command from store")
    }

    pub async fn notify_read(&mut self, key: Key) -> anyhow::Result<Value> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::NotifyRead(key, sender))
            .await
        {
            panic!("Failed to send NotifyRead command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to NotifyRead command from store")
    }
}

#[async_trait]
impl Store for Storage
{
    async fn write(&mut self, key: Key, value: Value) {
       self.write(key, value).await
    }

    async fn read(&mut self, key: Key) -> anyhow::Result<Option<Value>> {
        self.read(key).await
    }
    
    async fn notify_read(&mut self, key: Key) -> anyhow::Result<Value> {
        self.notify_read(key).await
    }
}