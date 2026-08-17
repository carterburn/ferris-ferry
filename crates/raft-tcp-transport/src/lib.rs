use std::{collections::HashMap, net::SocketAddr, time::Duration};

use futures::{StreamExt, sink::SinkExt};
use raft_event_loop::types::{RaftNodeDescription, Transport};
use raftcore::types::{Message, NodeId};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
    time::Instant,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct TcpTransport {
    senders: HashMap<NodeId, UnboundedSender<Message>>,

    receiver_rx: UnboundedReceiver<Message>,
}

impl TcpTransport {
    // The max time we wait to try reconnects must be less than the default tick interval
    // window so that we retry before a timeout occurs. Of course, this is a variale in the
    // instantiation of RaftCore, but we're writing this transport for raft-kv, so we can make a
    // const tied to our creation.
    const MAX_BACKOFF: Duration = Duration::from_millis(100);

    // This is the starting point of our exponential backoff retry loop. This is just a small
    // number decently far from the MAX_BACKOFF.
    const MIN_BACKOFF: Duration = Duration::from_millis(10);

    // The connect timeout is normally two minutes. We will set it to 10 seconds. Of course, this
    // can be network dependent so this currently isn't a 'sound' constant.
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

    pub async fn new(
        id: NodeId,
        bind_addr: SocketAddr,
        nodes: &[RaftNodeDescription<Self>],
    ) -> Self {
        let mut senders = HashMap::new();
        let (receiver_tx, receiver_rx) = mpsc::unbounded_channel();
        for n in nodes {
            if n.id == id {
                // spawn the listener at the specified address for receiving messages from other
                // nodes
                let rtx = receiver_tx.clone();
                let listener = TcpListener::bind(bind_addr)
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
                let address = n.address.clone();

                // this send loop is more important than it may seem. we must not just watch the rx
                // from the channel but also want to detect if the TCP connection is dropped (we
                // receive a FIN from the peer)
                tokio::spawn(async move {
                    let mut stream: Option<Framed<TcpStream, LengthDelimitedCodec>> = None;
                    let mut pending: Option<Message> = None;
                    let mut backoff = Self::MIN_BACKOFF;
                    let mut deadline: Option<Instant> = None;

                    loop {
                        if stream.is_none() {
                            // no connection to the peer currently exists, so we need to attempt to
                            // reconnect but not forget things coming in the channel. we only store
                            // the most recent item from the channel as that is all that's needed
                            // when the peer rejoins
                            let fire = deadline.get_or_insert_with(|| Instant::now() + backoff);

                            tokio::select! {
                                maybe = rx.recv() => match maybe {
                                    Some(m) => pending = Some(m),
                                    None => break, // channel closed, we're done
                                },
                                _ = tokio::time::sleep_until(*fire) => {
                                    match tokio::time::timeout(Self::CONNECT_TIMEOUT, TcpStream::connect(address.as_str())).await {
                                        Ok(Ok(s)) => {
                                            // successful connection
                                            stream = Some(Framed::new(s, LengthDelimitedCodec::new()));
                                            backoff = Self::MIN_BACKOFF;
                                            deadline = None;
                                        },
                                        _ => {
                                            // timeout or couldn't connect, backoff
                                            backoff = (2 * backoff).min(Self::MAX_BACKOFF);
                                            deadline = Some(Instant::now() + backoff);
                                        }
                                    }
                                }
                            }
                        } else {
                            // we have a stream connected (hopefully), so we can attempt to send
                            // first, we try to send anything in pending
                            if let Some(msg) = pending.take()
                                && let Some(s) = stream.as_mut()
                            {
                                let Ok(buf) = postcard::to_allocvec(&msg) else {
                                    // try again
                                    continue;
                                };
                                if s.send(buf.into()).await.is_err() {
                                    // something happened with the stream...try again
                                    pending = Some(msg);
                                    stream = None;
                                }
                            }

                            // then, we wait either for a new message on the channel or a FIN
                            let Some(s) = stream.as_mut() else {
                                // stream is None now
                                continue;
                            };

                            tokio::select! {
                                msg = rx.recv() => {
                                    match msg {
                                        Some(msg) => {
                                            let Ok(buf) = postcard::to_allocvec(&msg) else {
                                                continue;
                                            };
                                            if s.send(buf.into()).await.is_err() {
                                                // save off the message
                                                pending = Some(msg);
                                                stream = None;
                                            }
                                        },
                                        None => break,
                                    }
                                },
                                _ = s.next() => {
                                    stream = None;
                                }
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
    type Address = String;

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
                address: "127.0.0.1:65000".to_string(),
            },
            RaftNodeDescription::<TcpTransport> {
                id: 2,
                address: "127.0.0.1:65001".to_string(),
            },
        ];

        let mut node_1_transport = TcpTransport::new(
            1,
            SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 65000),
            &desc,
        )
        .await;
        let mut node_2_transport = TcpTransport::new(
            2,
            SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 65001),
            &desc,
        )
        .await;

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
            SocketAddr::new(
                std::net::IpAddr::V4(Ipv4Addr::from_str("127.0.0.1").unwrap()),
                65002,
            ),
            &[RaftNodeDescription::<TcpTransport> {
                id: 1,
                address: "127.0.0.1:65002".to_string(),
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
