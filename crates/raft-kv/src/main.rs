use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use clap::Parser;
use raft_event_loop::{
    RaftNode,
    types::{AppliedEntry, RaftConfig, RaftNodeDescription},
};
use raft_file_storage::FileStorage;
use raft_tcp_transport::TcpTransport;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Parser, Debug)]
struct Args {
    // NOTE: This is where having a config file would be better and we can name the nodes and
    // ensure we are on the same page
    /// Comma separated list of Raft Nodes including the address for this Raft Node's (for
    /// participating in the cluster). Format for these addresses is IP:Port. Note: each RaftNode
    /// should be started with the exact same order of these addresses to ensure compatibility.
    #[arg(short, long, value_delimiter = ',')]
    nodes: Vec<SocketAddr>,

    /// The ID of this RaftNode. This should be the index (starting from 1) of this node's address
    /// in the 'nodes' argument.
    #[arg(short, long)]
    id: u64,

    /// HTTP address for this Key-Value store. Default: 0.0.0.0:3000.
    #[arg(short, long, default_value = "0.0.0.0:3000")]
    addr: SocketAddr,

    /// The path to store durable storage. Must be a directory path!
    #[arg(short, long, default_value = "/tmp/raft")]
    directory: PathBuf,
}

#[derive(Serialize, Deserialize)]
enum KvCommand {
    Get(String),
    Set(String, String),
    Delete(String),
}

struct AppState {
    node: RaftNode,

    store: Arc<RwLock<HashMap<String, String>>>,
}

async fn build_config(args: Args) -> RaftConfig<TcpTransport, FileStorage> {
    let nodes: Vec<RaftNodeDescription<TcpTransport>> = args
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, s)| RaftNodeDescription {
            id: (idx + 1) as u64,
            address: *s,
        })
        .collect();

    let storage = FileStorage::new(args.directory)
        .await
        .expect("Unable to initialize FileStorage");
    let transport = TcpTransport::new(args.id, &nodes).await;

    RaftConfig {
        id: args.id,
        nodes,
        heartbeat_interval: None,
        election_range: 15..31,
        tick_length: Duration::from_millis(10),
        //snapshot_threshold: 1024,
        snapshot_threshold: 5,
        transport,
        storage,
    }
}
#[derive(Serialize, Deserialize)]
pub struct SetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct SetResponse {
    pub success: bool,
}

#[derive(Serialize)]
pub struct GetResponse {
    pub value: Option<String>,
}

#[derive(Serialize)]
pub struct DeleteResponse {
    pub success: bool,
}

async fn set_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SetRequest>,
) -> Result<Json<SetResponse>, StatusCode> {
    // propose the command through Raft and wait the response
    let command = postcard::to_allocvec(&KvCommand::Set(body.key, body.value))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // TODO: This may turn into a redirect in the future
    state
        .node
        .propose(command)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(SetResponse { success: true }))
}

async fn get_handler(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, StatusCode> {
    state.node.read_request().await.map_err(|e| match e {
        raft_event_loop::types::ProposalError::FollowerNode => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    // cleared to do a read from the local state
    let state = state
        .store
        .read()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let value = state.get(&key);
    Ok(Json(GetResponse {
        value: value.cloned(),
    }))
}

async fn delete_handler(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Json<DeleteResponse>, StatusCode> {
    let command = postcard::to_allocvec(&KvCommand::Delete(key))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .node
        .propose(command)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(DeleteResponse { success: true }))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    tracing::debug!("Starting Raft KV with configuration: {args:?}");

    tracing::info!("Initializing persistent storage and transport");
    let http_addr = args.addr;
    let config = build_config(args).await;
    tracing::info!("Initializing Raft node");
    let (raft_node, mut applied_receiver) = RaftNode::new(config).await;

    // at this point, Raft is running and figuring out its leader :)
    // let's start axum
    let store = Arc::new(RwLock::new(HashMap::new()));
    let state = Arc::new(AppState {
        node: raft_node,
        store: store.clone(),
    });

    // kick off the ApplyReceiver task to read from the apply channel and write to the HashMap when
    // entries are received and meant to be applied
    tokio::spawn(async move {
        let map = store.clone();

        while let Some(entry) = applied_receiver.recv().await {
            match entry {
                AppliedEntry::Command(cmd) => {
                    // deserialize the command to figure out what we do with it
                    let Ok(kv_cmd) = postcard::from_bytes(&cmd) else {
                        // on failure, just continue? or maybe this is unrecoverable
                        continue;
                    };
                    match kv_cmd {
                        KvCommand::Get(_key) => {}
                        KvCommand::Set(key, value) => {
                            let mut writer = map.write().unwrap();
                            let _ = writer.insert(key, value); // don't care if had been set
                        }
                        KvCommand::Delete(key) => {
                            let mut writer = map.write().unwrap();
                            let _ = writer.remove(&key); // don't care if had been set
                        }
                    }
                }
                AppliedEntry::Snapshot(snapshot_data) => {
                    // deserialize the snapshot data and replace the current hashmap
                    let new_store = postcard::from_bytes(&snapshot_data)
                        .expect("Serialization error; cannot continue");
                    let mut writer = map.write().unwrap();
                    *writer = new_store;
                }
                AppliedEntry::SnapshotRequest(sender) => {
                    // serialize our current store and send it to the sender
                    // we can get 'away' with a read lock here because we are the only task that
                    // holds write locks anyway so there would be no one that could make a write
                    // while we serialize the hashmap
                    let reader = map.read().unwrap();
                    let current_state = postcard::to_allocvec(&*reader)
                        .expect("Serialization error; cannot continue");
                    let _ = sender.send(current_state);
                }
            }
        }
    });

    let app = Router::new()
        .route("/key", post(set_handler))
        .route("/key/{name}", get(get_handler))
        .route("/key/{name}", delete(delete_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
