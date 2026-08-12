use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, Uri, header},
    response::IntoResponse,
    routing::{delete, get, post},
};
use clap::Parser;
use raft_event_loop::{
    RaftNode,
    types::{AppliedEntry, NodeId, ProposalError, RaftConfig, RaftNodeDescription},
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
    #[arg(short, long, value_delimiter = ',', required = true)]
    nodes: Vec<String>,

    /// Optional list of HTTP addresses if nodes run on the same machine. raft-kv will use the HTTP
    /// port for _this_ instance to send redirects to the leader of the cluster. The only time this
    /// is not available is local testing when you can't share a port. When deployed in a
    /// kubernetes cluster, for example, each node can share the port.
    #[arg(long, value_delimiter = ',')]
    http_nodes: Option<Vec<String>>,

    /// The ID of this RaftNode. This should be the index (starting from 1) of this node's address
    /// in the 'nodes' argument.
    #[arg(short, long)]
    id: u64,

    /// The address for Raft to bind on. Most of the time, this will match the IP:Port address in
    /// the nodes list, but in the case that the advertised address and bound address differ, this
    /// argument will be important.
    #[arg(short, long, default_value = "0.0.0.0:9000")]
    bind_addr: SocketAddr,

    /// HTTP address for this Key-Value store. Default: 0.0.0.0:3000.
    #[arg(short, long, default_value = "0.0.0.0:3000")]
    addr: SocketAddr,

    /// The path to store durable storage. Must be a directory path!
    #[arg(short, long, default_value = "/var/lib/raft")]
    directory: PathBuf,
}

#[derive(Serialize, Deserialize)]
enum KvCommand {
    Get(String),
    Set(String, String),
    Delete(String),
}

enum KvError {
    /// Not the leader, but we're aware who is so we can redirect
    Redirect(String),
    /// Not the leader but the leader is not known
    NoLeader,
    /// Was the leader when the proposal was accepted
    Uncertain,
    Internal,
}

impl IntoResponse for KvError {
    fn into_response(self) -> axum::response::Response {
        match self {
            KvError::Redirect(url) => {
                (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, url)]).into_response()
            }
            KvError::NoLeader | KvError::Uncertain => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
            )
                .into_response(),
            KvError::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

struct AppState {
    node: RaftNode,

    store: Arc<RwLock<HashMap<String, String>>>,

    http_addrs: HashMap<NodeId, String>,
}

impl AppState {
    fn map_proposal_error(&self, err: ProposalError, uri: &Uri) -> KvError {
        match err {
            ProposalError::FollowerNode { leader: Some(id) } => match self.http_addrs.get(&id) {
                Some(addr) => KvError::Redirect(format!("http://{addr}{}", uri.path())),
                None => KvError::NoLeader,
            },
            ProposalError::FollowerNode { leader: None } => KvError::NoLeader,
            ProposalError::LostLeadership => KvError::Uncertain,
            ProposalError::OtherError => KvError::Internal,
        }
    }
}

async fn build_config(args: Args) -> RaftConfig<TcpTransport, FileStorage> {
    let nodes: Vec<RaftNodeDescription<TcpTransport>> = args
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, s)| RaftNodeDescription {
            id: (idx + 1) as u64,
            address: s.clone(),
        })
        .collect();

    let storage = FileStorage::new(args.directory)
        .await
        .expect("Unable to initialize FileStorage");
    let transport = TcpTransport::new(args.id, args.bind_addr, &nodes).await;

    RaftConfig {
        id: args.id,
        nodes,
        heartbeat_interval: None,
        election_range: 15..31,
        tick_length: Duration::from_millis(10),
        snapshot_threshold: 1024,
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
    uri: Uri,
    Json(body): Json<SetRequest>,
) -> Result<Json<SetResponse>, KvError> {
    // propose the command through Raft and wait the response
    let command = postcard::to_allocvec(&KvCommand::Set(body.key, body.value))
        .map_err(|_| KvError::Internal)?;
    state
        .node
        .propose(command)
        .await
        .map_err(|e| state.map_proposal_error(e, &uri))?;
    Ok(Json(SetResponse { success: true }))
}

async fn get_handler(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, KvError> {
    state
        .node
        .read_request()
        .await
        .map_err(|e| state.map_proposal_error(e, &uri))?;

    // cleared to do a read from the local state
    let state = state.store.read().map_err(|_| KvError::Internal)?;
    let value = state.get(&key);
    Ok(Json(GetResponse {
        value: value.cloned(),
    }))
}

async fn delete_handler(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    Path(key): Path<String>,
) -> Result<Json<DeleteResponse>, KvError> {
    let command = postcard::to_allocvec(&KvCommand::Delete(key)).map_err(|_| KvError::Internal)?;
    state
        .node
        .propose(command)
        .await
        .map_err(|e| state.map_proposal_error(e, &uri))?;
    Ok(Json(DeleteResponse { success: true }))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    tracing::debug!("Starting Raft KV with configuration: {args:?}");

    if let Some(ref http_addrs) = args.http_nodes
        && http_addrs.len() != args.nodes.len()
    {
        panic!("Number of Raft nodes and HTTP nodes for KV service don't match. Check your config");
    }

    tracing::info!("Initializing persistent storage and transport");
    let http_addr = args.addr;
    // TODO(cb): NodeId's being 1-indexed causes the idx + 1 in these HashMap constructions. This
    // should be done in one place with the construction of the config because I had forgotten it
    // at first and it would've been a bad off-by-one error. The HTTP addrs should come out of the
    // build_config, but for now I'll leave this here.
    let http_addrs = match args.http_nodes {
        Some(ref http_nodes) => {
            // specific addresses from the user
            HashMap::from_iter(
                http_nodes
                    .iter()
                    .enumerate()
                    .map(|(idx, s)| (idx as NodeId + 1, s.clone())),
            )
        }
        None => {
            // iterate over the raft node addresses, and use our args.addr.port() as the HTTP port
            HashMap::from_iter(args.nodes.iter().enumerate().map(|(idx, s)| {
                let ip = s.rsplit_once(":").expect("Invalid address for Raft node").0;
                (idx as NodeId + 1, format!("{}:{}", ip, args.addr.port()))
            }))
        }
    };
    // validate id passed
    if args.id == 0 || args.id as usize > args.nodes.len() {
        panic!(
            "--id must be between 1 and {} (got {})",
            args.nodes.len(),
            args.id
        );
    }
    let advertised_port = args.nodes[args.id as usize - 1]
        .rsplit_once(':')
        .expect("Invalid node address")
        .1;
    if advertised_port
        .parse::<u16>()
        .expect("Invalid port number for advertised address")
        != args.bind_addr.port()
    {
        tracing::warn!(
            "Advertised address for this node to others and this node's bind address is different. This MAY not be an error, but ensure you meant to do this."
        );
    }
    let config = build_config(args).await;
    tracing::info!("Initializing Raft node");
    let (raft_node, mut applied_receiver) = RaftNode::new(config).await;

    // at this point, Raft is running and figuring out its leader :)
    // let's start axum
    let store = Arc::new(RwLock::new(HashMap::new()));
    let state = Arc::new(AppState {
        node: raft_node,
        store: store.clone(),
        http_addrs,
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
