use std::collections::{HashMap, VecDeque};
use std::path::Path;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use crate::{StoreCommand, Key, Value};

type Environment = libmdbx::Environment<libmdbx::NoWriteMap>;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct Storage {
    channel: Sender<StoreCommand>,
}

impl Storage {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let env = Box::new(Environment::new()
                    .set_max_dbs(1)
                    .open(&Path::new(path))?);
        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            let db_name = Some("Data");
            let t = env.begin_rw_txn().unwrap();
            let db = t.create_db(db_name, libmdbx::DatabaseFlags::default()).unwrap();
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value) => {
                        let txn = env.begin_rw_txn().expect("Failed to create a transaction");
                        txn.put(
                            &db,
                            key.clone(), 
                            value.clone(), 
                            libmdbx::WriteFlags::default(),
                        ).expect("failed to put data into the DB");
                        if let Some(mut senders) = obligations.remove(&key) {
                            while let Some(s) = senders.pop_front() {
                                let _ = s.send(Ok(value.clone()));
                            }
                        }
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = {
                            let txn = env.begin_rw_txn().expect("Failed to create a transaction");
                            txn.get(
                                &db,
                                &key
                            )
                        };
                        let _ = sender.send(response.map_err(|e| e.into()));
                    }
                    StoreCommand::NotifyRead(key, sender) => {
                        let response: Result<Option<_>, libmdbx::Error> = {
                            let txn = env.begin_ro_txn().expect("Failed to create a transaction");
                            txn.get::<Key>(
                                &db,
                                &key
                            )
                        };
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
                        };
                    }
                    // _ => { todo!() }
                }
            }
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

