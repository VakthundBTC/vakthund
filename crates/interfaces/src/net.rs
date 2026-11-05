//! Interface of `net` (`docs/modules/net/README.md`).

use std::future::Future;
use std::net::SocketAddr;

use bitcoin_p2p_messages::ServiceFlags;
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

/// Taken from the peer's `version` message during the features exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerFeatures {
    pub services: ServiceFlags,
    pub version: u32,
    pub user_agent: String,
}

/// A `peer-list` row (address, features) with the peer's in-memory fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerId,
    pub address: SocketAddr,
    /// `None` until the first features exchange with the peer.
    pub features: Option<PeerFeatures>,
    pub state: PeerState,
}

/// TBD-11: the results for a peer in the wrong state are not specified yet.
pub trait Net: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn get_peers(
        &self,
        state: Option<PeerState>,
    ) -> impl Future<Output = Result<Vec<Peer>, Self::Error>> + Send;

    fn get_peer(
        &self,
        id: PeerId,
    ) -> impl Future<Output = Result<Option<Peer>, Self::Error>> + Send;

    /// TBD-11: the arguments are not specified yet.
    fn add_peer(
        &self,
        address: SocketAddr,
        features: Option<PeerFeatures>,
    ) -> impl Future<Output = Result<PeerId, Self::Error>> + Send;

    fn remove_peer(&self, id: PeerId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Starts the connection flow with the peer.
    fn connect(&self, id: PeerId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn disconnect(&self, id: PeerId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Only for a connected peer. The payload is sent unchanged; the peer's answer, if any,
    /// arrives through the queue of the command's owner.
    fn send(
        &self,
        id: PeerId,
        command: CommandString,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
