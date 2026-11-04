//! Interface of `net` (`docs/modules/net/README.md`).

use std::future::Future;
use std::net::SocketAddr;
use std::time::SystemTime;

use bitcoin_p2p_messages::message::CommandString;

/// Assigned by `net`; unique while the process runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerState {
    Known,
    Connecting,
    Connected,
}

/// TBD-02: the features exchange is not specified yet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeerFeatures {}

/// A `peer-list` row (address, features) with the peer's in-memory fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerId,
    pub address: SocketAddr,
    pub features: PeerFeatures,
    pub state: PeerState,
    pub last_ping: Option<SystemTime>,
    pub last_pong: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetError {
    #[error("unknown peer {0:?}")]
    UnknownPeer(PeerId),
    /// TBD-11: the results for a peer in the wrong state are not specified yet.
    #[error("peer {peer:?} is {state:?}")]
    InvalidState { peer: PeerId, state: PeerState },
    /// The store engine failed; the message is the engine's error.
    #[error("storage failure: {0}")]
    Storage(String),
}

/// Every call can also return `Storage`.
pub trait Net: Send + Sync {
    fn get_peers(
        &self,
        state: Option<PeerState>,
    ) -> impl Future<Output = Result<Vec<Peer>, NetError>> + Send;

    fn get_peer(&self, id: PeerId) -> impl Future<Output = Result<Option<Peer>, NetError>> + Send;

    /// TBD-11: the arguments are not specified yet.
    fn add_peer(
        &self,
        address: SocketAddr,
        features: PeerFeatures,
    ) -> impl Future<Output = Result<PeerId, NetError>> + Send;

    fn remove_peer(&self, id: PeerId) -> impl Future<Output = Result<(), NetError>> + Send;

    /// Starts the connection flow with the peer.
    fn connect(&self, id: PeerId) -> impl Future<Output = Result<(), NetError>> + Send;

    fn disconnect(&self, id: PeerId) -> impl Future<Output = Result<(), NetError>> + Send;

    /// Only for a connected peer. The payload is sent unchanged; the peer's answer, if any,
    /// arrives through the queue of the command's owner.
    fn send(
        &self,
        id: PeerId,
        command: CommandString,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), NetError>> + Send;
}
