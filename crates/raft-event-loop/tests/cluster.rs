#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use raft_event_loop::{
        RaftNode,
        types::{AppliedEntry, PersistedMetadata, ProposalError, RaftConfig, RaftNodeDescription},
    };
    use raft_test_utils::{InMemoryStorage, InMemoryTransport, build_in_memory_transport};
    use raftcore::types::{LogEntry, NodeId};
    use tokio::sync::mpsc;

    async fn init_three_node_cluster() -> (NodeId, Vec<(RaftNode, mpsc::Receiver<AppliedEntry>)>) {
        let transports = build_in_memory_transport(&[1, 2, 3]);
        let mut configs = Vec::new();
        for (id, transport) in transports {
            configs.push(RaftConfig {
                id,
                nodes: vec![
                    RaftNodeDescription { id: 1, address: 1 },
                    RaftNodeDescription { id: 2, address: 2 },
                    RaftNodeDescription { id: 3, address: 3 },
                ],
                heartbeat_interval: None,
                election_range: 15..31,
                tick_length: Duration::from_millis(10),
                snapshot_threshold: 100,
                transport,
                storage: InMemoryStorage::new(),
            });
        }

        // create the RaftNode for each node
        let mut nodes = Vec::new();
        for config in configs {
            nodes.push(RaftNode::new(config).await);
        }

        // attempt to propose to each of the nodes in a loop until one doesn't have a
        // ProposalError::FollowerNode response
        loop {
            for (idx, n) in nodes.iter().enumerate() {
                if let Ok(proposal) = tokio::time::timeout(
                    Duration::from_millis(30),
                    n.0.propose("Hello, Raft".into()),
                )
                .await
                {
                    if let Err(e) = proposal {
                        match e {
                            ProposalError::FollowerNode { .. } => continue,
                            _ => panic!("Unexpected error from proposal"),
                        }
                    } else {
                        // got a good proposal in and found the leader
                        return ((idx + 1) as u64, nodes);
                    }
                } else {
                    // move on to another node, this timed out
                    continue;
                }
            }
        }
    }

    #[tokio::test]
    async fn basic_cluster() {
        // since we successfully proposed a command, each node should get an applied entry out of
        // its applied channel
        let (_leader_id, nodes) = init_three_node_cluster().await;
        for mut n in nodes {
            if let Some(application) = n.1.recv().await {
                match application {
                    AppliedEntry::Command(cmd) => {
                        assert_eq!(cmd, "Hello, Raft".as_bytes().to_vec())
                    }
                    AppliedEntry::Snapshot(_) => panic!("Unexpected Snapshot on applied entry"),
                    AppliedEntry::SnapshotRequest(_) => {
                        panic!("Unexpected Snapshot on applied entry")
                    }
                }
            } else {
                panic!("Errored out getting applied command")
            }
        }
    }

    #[tokio::test]
    async fn cluster_with_snapshot() {
        use raft_event_loop::types::Storage;

        // create some storage and attempt to create a RaftNode with that storage and check the
        // event loop properly starts up and sets the node up correctly
        let s = InMemoryStorage::new();
        let mut t = build_in_memory_transport(&[1, 2]);
        let _ = s
            .store_metadata(PersistedMetadata {
                term: 2,
                voted_for: None,
            })
            .await;
        let _ = s
            .store_log_entries(raft_event_loop::types::PersistedLogAddendum {
                start_index: 3,
                entries: vec![LogEntry {
                    term: 1,
                    command: "CMD3".as_bytes().to_vec(),
                }],
            })
            .await;
        let _ = s
            .store_snapshot(raft_event_loop::types::Snapshot {
                last_included_index: 2,
                last_included_term: 1,
                data: "CMD1/CMD2".as_bytes().to_vec(),
            })
            .await;

        // with some data, now we get the RaftNode
        let (node1, mut applied1) = RaftNode::new(RaftConfig {
            id: 1,
            nodes: vec![
                RaftNodeDescription::<InMemoryTransport> { id: 1, address: 1 },
                RaftNodeDescription::<InMemoryTransport> { id: 2, address: 2 },
            ],
            heartbeat_interval: None,
            election_range: 15..31,
            tick_length: Duration::from_millis(10),
            snapshot_threshold: 100,
            transport: t.remove(&1).unwrap(),
            storage: s,
        })
        .await;
        let (_node2, mut applied2) = RaftNode::new(RaftConfig {
            id: 2,
            nodes: vec![
                RaftNodeDescription::<InMemoryTransport> { id: 1, address: 1 },
                RaftNodeDescription::<InMemoryTransport> { id: 2, address: 2 },
            ],
            heartbeat_interval: None,
            election_range: 15..31,
            tick_length: Duration::from_millis(10),
            snapshot_threshold: 100,
            transport: t.remove(&2).unwrap(),
            storage: InMemoryStorage::new(),
        })
        .await;

        // need to have a snapshot to install first
        match applied2.recv().await.unwrap() {
            AppliedEntry::Snapshot(data) => {}
            _ => panic!("Expected AppliedEntry::Snapshot"),
        }

        // now propose a new command through node1 (should be the leader but we'll retry until it
        // is based on timing consensus)
        node1.propose("Hello, Raft".into()).await.unwrap();

        // expect two commands (CMD3 and Hello, Raft) to come out of the applied channels
        match applied1.recv().await.unwrap() {
            AppliedEntry::Command(cmd) => assert_eq!(cmd, "CMD3".as_bytes().to_vec()),
            _ => panic!("Expected AppliedEntry::Command"),
        }
        match applied2.recv().await.unwrap() {
            AppliedEntry::Command(cmd) => assert_eq!(cmd, "CMD3".as_bytes().to_vec()),
            _ => panic!("Expected AppliedEntry::Command"),
        }
        match applied1.recv().await.unwrap() {
            AppliedEntry::Command(cmd) => assert_eq!(cmd, "Hello, Raft".as_bytes().to_vec()),
            _ => panic!("Expected AppliedEntry::Command"),
        }
        match applied2.recv().await.unwrap() {
            AppliedEntry::Command(cmd) => assert_eq!(cmd, "Hello, Raft".as_bytes().to_vec()),
            _ => panic!("Expected AppliedEntry::Command"),
        }
    }
}
