use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, Uri, header},
    response::IntoResponse,
    routing::{delete, get, post},
};
use clap::{ArgGroup, Parser};
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
#[command(group(ArgGroup::new("identity").required(true).args(["id", "derive_id"])))]
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
    /// in the 'nodes' argument. More often than not, you'll want to use this option unless
    /// deploying on a platform like Kubernetes. This option cannot be used in conjunction with
    /// derive-id.
    #[arg(short, long)]
    id: Option<NodeId>,

    /// Derive the Raft node ID from the hostname set in the environment variable HOSTNAME. This
    /// should only be used when you are deploying into an environment like Kubernetes where the
    /// hostname of each instance is derivable. The hostnames in your configuration should be
    /// structured like 'raft-node-0', 'raft-node-1', etc. and this derivation will take the number
    /// following the final '-'. This cannot be used in conjunction with id and remember you most
    /// likely don't want this unless deploying into Kubernetes. Start
    /// numbering from 0 (which is default in kubernetes).
    #[arg(long)]
    derive_id: bool,

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

fn id_from_hostname(hostname: &str) -> Result<NodeId, String> {
    let (_, num) = hostname
        .rsplit_once('-')
        .ok_or_else(|| format!("Hostname {hostname} has no final '-' with ID number"))?;
    let id: u64 = num
        .parse()
        .map_err(|_| format!("Invalid number for hostname: {num}"))?;
    id.checked_add(1)
        .ok_or_else(|| format!("{id} is not a valid number (try using something < 2^64)"))
}

impl Args {
    fn resolve_id(&self) -> Result<NodeId, String> {
        match (self.id, self.derive_id) {
            (Some(id), false) => Ok(id),
            (None, true) => {
                let hostname = std::env::var("HOSTNAME").map_err(|_| {
                    "Attempted to derive node ID but HOSTNAME variable not set.".to_string()
                })?;
                id_from_hostname(&hostname)
            }
            // anything else should be unreachable from the ArgGroup
            _ => unreachable!("Use one of --id or --derive-id to set or derive this node's ID"),
        }
    }
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

async fn build_config(node_id: NodeId, args: Args) -> RaftConfig<TcpTransport, FileStorage> {
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
    let transport = TcpTransport::new(node_id, args.bind_addr, &nodes).await;

    RaftConfig {
        id: node_id,
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

async fn health() -> StatusCode {
    // TODO(cb): this is sort of a 'dumb' health check for kubernetes / docker. A possible issue is
    // that the raft-event-loop could actually die and HTTP still runs perfectly. If the event
    // loop's task panic'ed, HTTP would continue to serve requests just saying it can't. In reality
    // we'd want a smarter way to query the event loop and would know that if a channel is dropped,
    // we're actually not ok and should get restarted. This should work for now though.
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
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
    let node_id = match args.resolve_id() {
        Ok(id) => id,
        Err(e) => {
            panic!("Encountered error while deriving ID: {e}");
        }
    };
    // validate id passed
    if node_id == 0 || node_id as usize > args.nodes.len() {
        panic!(
            "--id must be between 1 and {} (got {} from hostname: {})",
            args.nodes.len(),
            node_id - 1,
            // SAFETY: we already know HOSTNAME is in the environment from resolve_id working
            std::env::var("HOSTNAME").unwrap(),
        );
    }
    let advertised_port = args.nodes[node_id as usize - 1]
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
    let config = build_config(node_id, args).await;
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
        .route("/healthz", get(health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler");
    };

    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_from_hostname() {
        assert_eq!(id_from_hostname("raft-kv-0"), Ok(1));
        assert_eq!(
            id_from_hostname("raft-kv-7d9f-x2k302"),
            Err("Invalid number for hostname: x2k302".to_string())
        );
        assert_eq!(
            id_from_hostname("localhost"),
            Err("Hostname localhost has no final '-' with ID number".to_string())
        );
    }
}
