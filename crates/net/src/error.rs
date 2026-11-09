use bitcoin_p2p_messages::message::CommandStringError;
use std::net::SocketAddr;

use bitcoin_p2p_messages::{Magic, UnknownNetworkError};
use interfaces::net::PeerId;
use store::StoreError;

use crate::connection::CONNECT_TIMEOUT;
use crate::ping::PONG_TIMEOUT;
use crate::routing::Command;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Network(#[from] UnknownNetworkError),
    #[error("connection failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid message header: {0}")]
    Header(String),
    #[error("wrong network magic: {0:?}")]
    Magic(Magic),
    #[error("payload of {0} bytes exceeds the limit")]
    TooLong(usize),
    #[error("checksum does not match the payload")]
    Checksum,
    #[error("undecodable {} payload: {reason}", .command.name())]
    Payload { command: Command, reason: String },
    #[error("connection flow did not complete in {:?}", CONNECT_TIMEOUT)]
    ConnectTimeout,
    #[error("no pong for {:?}", PONG_TIMEOUT)]
    PongTimeout,
    /// A node failure: the node must stop.
    #[error("the block-sync queue is full")]
    QueueFull,
    #[error(transparent)]
    Command(#[from] CommandStringError),
    #[error("the system clock is before 1970 or too far in the future")]
    Clock,
    #[error("the connection has ended")]
    Closed,
    #[error("unknown peer {0}")]
    UnknownPeer(PeerId),
    #[error("peer {0} is not connected")]
    NotConnected(PeerId),
    #[error("peer {0} is connecting or connected")]
    StillConnected(PeerId),
    #[error("{address} gets the same peer id as {existing}")]
    IdCollision {
        address: SocketAddr,
        existing: SocketAddr,
    },
    /// A node failure: the node must stop.
    #[error("storage failure: {0}")]
    Storage(#[from] StoreError),
}
