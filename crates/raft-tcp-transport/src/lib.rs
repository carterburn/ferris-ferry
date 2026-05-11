use std::{collections::HashMap, net::SocketAddr};

use futures::{sink::SinkExt, StreamExt};
use raft_event_loop::types::{RaftNodeDescription, Transport};
use raftcore::types::{Message, NodeId};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct TcpTransport {
    senders: HashMap<NodeId, UnboundedSender<Message>>,

    receiver_rx: UnboundedReceiver<Message>,
}

impl TcpTransport {
    pub async fn new(id: NodeId, nodes: &[RaftNodeDescription<Self>]) -> Self {
        let mut senders = HashMap::new();
        let (receiver_tx, receiver_rx) = mpsc::unbounded_channel();
        for n in nodes {
            if n.id == id {
                // spawn the listener at the specified address for receiving messages from other
                // nodes
                let rtx = receiver_tx.clone();
                let listener = TcpListener::bind(n.address)
                    .await
                    .expect("Unable to bind to address");
                tokio::spawn(async move {
                    // if we can't listen, we can't participate
                    while let Ok((client_stream, _)) = listener.accept().await {
                        // clone the receiver_tx to provide ANOTHER tokio task the ability to just
                        // receive messages
                        let pipe = rtx.clone();
                        tokio::spawn(async move {
                            // read messages from the stream and pipe to the pipe
                            let mut framed =
                                Framed::new(client_stream, LengthDelimitedCodec::new());
                            while let Some(Ok(buf)) = framed.next().await {
                                // deserialize the msg and send to the pipe
                                let Ok(msg) = postcard::from_bytes(&buf) else {
                                    // continue loop on deserialization error
                                    continue;
                                };
                                // ignore channel sending errors
                                let _ = pipe.send(msg);
                            }
                        });
                    }
                });
            } else {
                // spawn the sender task for this peer providing the address
                let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
                let address = n.address;

                tokio::spawn(async move {
                    let mut stream: Option<Framed<TcpStream, LengthDelimitedCodec>> = None;
                    while let Some(msg) = rx.recv().await {
                        if stream.is_none() {
                            let Ok(connected_stream) = TcpStream::connect(address).await else {
                                // can't connect so just continue the loop for the next message to
                                // try again
                                continue;
                            };
                            let framed = Framed::new(connected_stream, LengthDelimitedCodec::new());
                            stream = Some(framed);
                        }

                        if let Some(s) = stream.as_mut() {
                            let Ok(buf) = postcard::to_allocvec(&msg) else {
                                continue;
                            };
                            if s.send(buf.into()).await.is_err() {
                                stream = None;
                            }
                        }
                    }
                });
                senders.insert(n.id, tx);
            }
        }

        Self {
            senders,
            receiver_rx,
        }
    }
}

impl Transport for TcpTransport {
    type Address = SocketAddr;

    async fn send(&self, node: NodeId, message: Message) {
        let Some(sender) = self.senders.get(&node) else {
            return;
        };
        let _ = sender.send(message);
    }

    async fn recv(&mut self) -> Message {
        // we have to return a Message here, so we'll panic if we somehow don't (or we lost the
        // sending end which would also be bad)
        self.receiver_rx.recv().await.expect("Receiving pipe lost")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        str::FromStr,
    };

    use raft_event_loop::types::{RaftNodeDescription, Transport};
    use raftcore::types::Message;

    use crate::TcpTransport;

    #[tokio::test]
    async fn network_rt() {
        // make two nodes and have them send a Message::RequestVote to one another and both receive
        // it
        let desc = vec![
            RaftNodeDescription::<TcpTransport> {
                id: 1,
                address: SocketAddr::new(
                    std::net::IpAddr::V4(Ipv4Addr::from_str("127.0.0.1").unwrap()),
                    65000,
                ),
            },
            RaftNodeDescription::<TcpTransport> {
                id: 2,
                address: SocketAddr::new(
                    std::net::IpAddr::V4(Ipv4Addr::from_str("127.0.0.1").unwrap()),
                    65001,
                ),
            },
        ];

        let mut node_1_transport = TcpTransport::new(1, &desc).await;
        let mut node_2_transport = TcpTransport::new(2, &desc).await;

        // both transports are up so let's send some messages!
        let m = Message::RequestVote(raftcore::types::RequestVoteRPC {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        });
        node_1_transport.send(2, m.clone()).await;
        let compare = node_2_transport.recv().await;
        assert_eq!(compare, m);

        node_2_transport.send(1, m.clone()).await;
        let compare = node_1_transport.recv().await;
        assert_eq!(compare, m);
    }

    #[tokio::test]
    async fn failed_send() {
        // attempt to send a message through a nodes transport to a non-existent node (similar
        // situation to if a node dropped its connection)
        let transport = TcpTransport::new(
            1,
            &[RaftNodeDescription::<TcpTransport> {
                id: 1,
                address: SocketAddr::new(
                    std::net::IpAddr::V4(Ipv4Addr::from_str("127.0.0.1").unwrap()),
                    65002,
                ),
            }],
        )
        .await;

        let m = Message::RequestVote(raftcore::types::RequestVoteRPC {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        });
        transport.send(2, m).await;
        // no panic is a good thing
    }
}
