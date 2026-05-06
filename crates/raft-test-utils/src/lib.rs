use std::{collections::HashMap, io::Read, sync::Mutex};

use raft_event_loop::types::{PersistedMetadata, Snapshot, Storage, Transport};
use raftcore::types::{Message, NodeId};
use tokio::sync::mpsc;

pub struct InMemoryStorage {
    metadata: Mutex<Vec<u8>>,
    log_entries: Mutex<Vec<u8>>,
    snapshot: Mutex<Vec<u8>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            metadata: Mutex::new(Vec::new()),
            log_entries: Mutex::new(Vec::new()),
            snapshot: Mutex::new(Vec::new()),
        }
    }

    fn get_snapshot(&self) -> Result<Snapshot, std::io::Error> {
        let buf = self.snapshot.lock().unwrap();
        postcard::from_bytes(&buf).map_err(|_| std::io::Error::other("Unable to deserialize"))
    }
}

impl Storage for InMemoryStorage {
    fn store_metadata(
        &self,
        metadata: raft_event_loop::types::PersistedMetadata,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        let mut buf = self.metadata.lock().unwrap();
        *buf = postcard::to_allocvec(&metadata).unwrap();
        std::future::ready(Ok(()))
    }

    fn restore_metadata(
        &self,
    ) -> impl std::future::Future<
        Output = std::io::Result<Option<raft_event_loop::types::PersistedMetadata>>,
    > + Send {
        let buf = self.metadata.lock().unwrap();
        let out: PersistedMetadata = match postcard::from_bytes(buf.as_slice()) {
            Ok(m) => m,
            Err(_e) => {
                if buf.is_empty() {
                    // return none if there isn't a 'file' or metadata yet
                    return std::future::ready(Ok(None));
                } else {
                    return std::future::ready(Err(std::io::Error::other("No metadata")));
                }
            }
        };
        std::future::ready(Ok(Some(out)))
    }

    fn store_log_entries(
        &self,
        addendum: raft_event_loop::types::PersistedLogAddendum,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        // serialize the new entries first
        let mut serialized = Vec::with_capacity(addendum.entries.len());
        for entry in &addendum.entries {
            let Ok(s) = postcard::to_allocvec(entry) else {
                return std::future::ready(Err(std::io::Error::other("Could not serialize")));
            };
            serialized.push(s);
        }

        let mut buf = self.log_entries.lock().unwrap();

        // attempt to find where we need to start writing (if needed) and truncate buf if needed
        let truncate_at = {
            let mut cursor = std::io::Cursor::new(buf.as_slice());
            let mut pos = None;
            loop {
                // read the index
                let mut index = [0u8; std::mem::size_of::<u64>()];
                if cursor.read_exact(&mut index).is_err() {
                    break;
                }
                let i: u64 = u64::from_be_bytes(index);
                if i == addendum.start_index {
                    // this is where we start writing so truncate buf to here
                    pos = Some(cursor.position() as usize - std::mem::size_of::<u64>());
                    break;
                }
                // read past length and serialized entry to get the next iteration
                let mut len = [0u8; std::mem::size_of::<u32>()];
                cursor.read_exact(&mut len).unwrap();
                let i: u32 = u32::from_be_bytes(len);
                cursor.set_position(cursor.position() + i as u64);
            }
            pos
        };
        if let Some(pos) = truncate_at {
            buf.truncate(pos);
        }

        // regardless of what happened in that loop, we just append from here starting with
        // start_index
        let mut index = addendum.start_index;

        for s in serialized {
            // build out the serialized frame
            let mut v: Vec<u8> = Vec::with_capacity(
                std::mem::size_of::<u64>() + std::mem::size_of::<u32>() + s.len(),
            );
            v.extend(index.to_be_bytes());
            v.extend((s.len() as u32).to_be_bytes());
            v.extend(s);

            buf.extend(v);

            index += 1;
        }

        std::future::ready(Ok(()))
    }

    fn restore_log_entries(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<Option<Vec<raftcore::types::LogEntry>>>> + Send
    {
        let buf = self.log_entries.lock().unwrap();
        if buf.is_empty() {
            return std::future::ready(Ok(None));
        }
        let mut cursor = std::io::Cursor::new(buf.as_slice());
        let mut entries = Vec::new();

        loop {
            // read index and discard
            let mut index = [0u8; std::mem::size_of::<u64>()];
            if cursor.read_exact(&mut index).is_err() {
                break;
            }
            // get length to read from cursor
            let mut len = [0u8; std::mem::size_of::<u32>()];
            if cursor.read_exact(&mut len).is_err() {
                break;
            }
            let len = u32::from_be_bytes(len);
            let mut entry = vec![0u8; len as usize];
            if cursor.read_exact(&mut entry).is_err() {
                break;
            }
            let Ok(entry) = postcard::from_bytes(&entry) else {
                return std::future::ready(Err(std::io::Error::other("Unable to deserialize")));
            };
            entries.push(entry);
        }

        std::future::ready(Ok(Some(entries)))
    }

    fn store_snapshot(
        &self,
        snapshot: raft_event_loop::types::Snapshot,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        let Ok(snap) = postcard::to_allocvec(&snapshot) else {
            return std::future::ready(Err(std::io::Error::other("Could not serialize snapshot")));
        };
        let mut buf = self.snapshot.lock().unwrap();
        *buf = snap;
        std::future::ready(Ok(()))
    }

    fn truncate_log(&self) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        let mut buf = self.log_entries.lock().unwrap();
        buf.clear();
        std::future::ready(Ok(()))
    }

    fn retrieve_snapshot_bytes(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<Vec<u8>>> + Send {
        let Ok(snapshot) = self.get_snapshot() else {
            return std::future::ready(Err(std::io::Error::other("Unable to deserialize")));
        };
        std::future::ready(Ok(snapshot.data))
    }

    fn restore_snapshot(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<Option<raft_event_loop::types::Snapshot>>>
           + Send {
        {
            let buf = self.snapshot.lock().unwrap();
            if buf.is_empty() {
                return std::future::ready(Ok(None));
            }
        }
        let Ok(snapshot) = self.get_snapshot() else {
            return std::future::ready(Err(std::io::Error::other("Unable to deserialize")));
        };
        std::future::ready(Ok(Some(snapshot)))
    }
}

pub struct InMemoryTransport {
    rx: mpsc::Receiver<Message>,
    senders: HashMap<NodeId, mpsc::Sender<Message>>,
}

impl InMemoryTransport {
    fn new(rx: mpsc::Receiver<Message>, senders: HashMap<NodeId, mpsc::Sender<Message>>) -> Self {
        Self { rx, senders }
    }
}

impl Transport for InMemoryTransport {
    type Address = NodeId;

    async fn send(&self, node: NodeId, message: raftcore::types::Message) {
        if let Some(tx) = self.senders.get(&node) {
            let _ = tx.send(message).await;
        }
    }

    async fn recv(&mut self) -> Message {
        self.rx.recv().await.unwrap()
    }
}

pub fn build_in_memory_transport(nodes: &[NodeId]) -> HashMap<NodeId, InMemoryTransport> {
    let mut channels = HashMap::with_capacity(nodes.len());
    // build up a pair of senders and receivers for each node
    for node in nodes {
        let ch = mpsc::channel(1024);
        channels.insert(*node, ch);
    }
    // create the 'sender' HashMap with each sender
    let mut senders: HashMap<NodeId, mpsc::Sender<Message>> = HashMap::with_capacity(nodes.len());
    for (id, (tx, _rx)) in channels.iter() {
        senders.insert(*id, tx.clone());
    }

    let mut transports = HashMap::with_capacity(nodes.len());
    for (id, (_tx, rx)) in channels.into_iter() {
        transports.insert(id, InMemoryTransport::new(rx, senders.clone()));
    }
    transports
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_event_loop::types::*;
    use raftcore::types::LogEntry;

    #[tokio::test]
    async fn test_store_metadata() {
        let s = InMemoryStorage::new();
        let res = s
            .store_metadata(raft_event_loop::types::PersistedMetadata {
                term: 1,
                voted_for: Some(1),
            })
            .await;
        assert!(res.is_ok());

        // check that it deserializes back into PersistedMetadata
        {
            let buf = s.metadata.lock().unwrap();
            let out: PersistedMetadata = postcard::from_bytes(buf.as_slice()).unwrap();
            assert_eq!(out.term, 1);
            assert_eq!(out.voted_for, Some(1));
        }

        // check it works with a None variant on voted_for
        s.store_metadata(PersistedMetadata {
            term: 2,
            voted_for: None,
        })
        .await
        .unwrap();

        let buf = s.metadata.lock().unwrap();
        let out: PersistedMetadata = postcard::from_bytes(buf.as_slice()).unwrap();
        assert_eq!(out.term, 2);
        assert_eq!(out.voted_for, None);
    }

    #[tokio::test]
    async fn store_and_restore_metadata() {
        // test restore_* methods with a full loop
        let s = InMemoryStorage::new();
        let metadata = PersistedMetadata {
            term: 1,
            voted_for: Some(2),
        };

        s.store_metadata(metadata.clone()).await.unwrap();

        let compare = s.restore_metadata().await.unwrap().unwrap();

        assert_eq!(metadata.term, compare.term);
        assert_eq!(metadata.voted_for, compare.voted_for);
    }

    fn make_log_entry(term: u64, command: &str) -> LogEntry {
        LogEntry {
            term,
            command: command.as_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn store_and_restore_log() {
        let s = InMemoryStorage::new();
        let entries = vec![make_log_entry(1, "CMD1"), make_log_entry(1, "CMD2")];
        let addendum = PersistedLogAddendum {
            start_index: 1,
            entries: entries.clone(),
        };

        s.store_log_entries(addendum.clone()).await.unwrap();
        {
            let buf = s.log_entries.lock().unwrap();
            assert!(!buf.is_empty());
        }
        let compare = s.restore_log_entries().await.unwrap().unwrap();

        assert_eq!(compare, entries);
    }

    #[tokio::test]
    async fn test_overwrite_log() {
        // build out the log with 3 entries and instruct to overwrite starting at index 2, then
        // check restore gives those new entries
        let s = InMemoryStorage::new();
        let initial_entries = vec![
            make_log_entry(1, "CMD1"),
            make_log_entry(1, "CMD2"),
            make_log_entry(1, "CMD3"),
        ];
        let addendum = PersistedLogAddendum {
            start_index: 1,
            entries: initial_entries,
        };

        s.store_log_entries(addendum).await.unwrap();
        let overwrite = PersistedLogAddendum {
            start_index: 2,
            entries: vec![make_log_entry(2, "CMD2_MOD"), make_log_entry(2, "CMD3_MOD")],
        };
        s.store_log_entries(overwrite).await.unwrap();

        let out = s.restore_log_entries().await.unwrap().unwrap();
        assert_eq!(
            out,
            vec![
                make_log_entry(1, "CMD1"),
                make_log_entry(2, "CMD2_MOD"),
                make_log_entry(2, "CMD3_MOD")
            ]
        );
    }

    #[tokio::test]
    async fn store_and_restore_snapshot() {
        let s = InMemoryStorage::new();
        let snapshot = Snapshot {
            last_included_index: 100,
            last_included_term: 2,
            data: "SNAPSHOT BYTES".as_bytes().to_vec(),
        };
        s.store_snapshot(snapshot.clone()).await.unwrap();

        {
            let buf = s.snapshot.lock().unwrap();
            assert!(!buf.is_empty());
        }

        let compare = s.restore_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.last_included_index, compare.last_included_index);
        assert_eq!(snapshot.last_included_term, compare.last_included_term);
        assert_eq!(snapshot.data, compare.data);
    }

    #[tokio::test]
    async fn retrieve_snapshot_bytes() {
        let s = InMemoryStorage::new();
        let snapshot = Snapshot {
            last_included_index: 100,
            last_included_term: 2,
            data: "SNAPSHOT BYTES".as_bytes().to_vec(),
        };
        s.store_snapshot(snapshot).await.unwrap();

        let bytes = s.retrieve_snapshot_bytes().await.unwrap();
        assert_eq!(bytes, "SNAPSHOT BYTES".as_bytes().to_vec());
    }

    #[tokio::test]
    async fn truncate_log() {
        let s = InMemoryStorage::new();
        let entries = vec![make_log_entry(1, "CMD1"), make_log_entry(1, "CMD2")];
        let addendum = PersistedLogAddendum {
            start_index: 1,
            entries: entries.clone(),
        };
        s.store_log_entries(addendum).await.unwrap();

        {
            let buf = s.log_entries.lock().unwrap();
            assert!(!buf.is_empty());
        }
        s.truncate_log().await.unwrap();
        {
            let buf = s.log_entries.lock().unwrap();
            assert!(buf.is_empty());
        }
    }

    #[tokio::test]
    async fn test_on_empty() {
        let s = InMemoryStorage::new();
        assert!(s.restore_metadata().await.unwrap().is_none());
        assert!(s.restore_log_entries().await.unwrap().is_none());
        assert!(s.restore_snapshot().await.unwrap().is_none());
    }
}
