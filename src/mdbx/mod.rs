use std::collections::{HashMap, VecDeque};
use std::path::Path;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use crate::{StoreCommand, Key, Value};

type Environment = libmdbx::Database<libmdbx::NoWriteMap>;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct Storage {
    channel: Sender<StoreCommand>,
}
const DB_NAME: Option<&str> = None;

impl Storage {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let env = {
            let env = Box::new(Environment::open(&Path::new(path))?);
            let t = env.begin_rw_txn()?;
            let _ = t.open_table(DB_NAME)?;
            t.commit()?;
            env
        };
        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value) => {
                        let txn = env.begin_rw_txn().expect("Failed to create a transaction");
                        txn.put(
                            &txn.open_table(DB_NAME).unwrap(),
                            key.clone(), 
                            value.clone(), 
                            libmdbx::WriteFlags::default(),
                        ).expect("failed to put data into the DB");
                        txn.commit().unwrap();
                        if let Some(mut senders) = obligations.remove(&key) {
                            while let Some(s) = senders.pop_front() {
                                let _ = s.send(Ok(value.clone()));
                            }
                        }
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = {
                            let txn = env.begin_rw_txn().expect("Failed to create a transaction");
                            let res = txn.get(
                                &txn.open_table(DB_NAME).unwrap(),
                                &key
                            );
                            txn.commit().unwrap();
                            res
                        };
                        let _ = sender.send(response.map_err(|e| e.into()));
                    }
                    StoreCommand::NotifyRead(key, sender) => {
                        let response: Result<Option<_>, libmdbx::Error> = {
                            let txn = env.begin_ro_txn().expect("Failed to create a transaction");
                            let res = txn.get::<Key>(
                                &txn.open_table(DB_NAME).unwrap(),
                                &key
                            );
                            txn.commit().unwrap();
                            res
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

