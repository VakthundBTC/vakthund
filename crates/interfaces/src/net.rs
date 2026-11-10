//! Interface of `net` (`docs/modules/net/README.md`).

use std::fmt;
use std::future::Future;
use std::net::SocketAddr;

use bitcoin_p2p_messages::ServiceFlags;
use bitcoin_p2p_messages::message::CommandString;

/// Derived by `net` from the peer's address, so it stays the same across restarts. Shown as 16
/// hex digits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(pub u64);

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({self})")
    }
}

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

/// Every call except `get_peers` and `get_peer` fails for an unknown peer id.
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

    /// Stores the peer in `peer-list` as `Known`; does not connect. A known address returns its
    /// peer.
    fn add_peer(
        &self,
        address: SocketAddr,
    ) -> impl Future<Output = Result<PeerId, Self::Error>> + Send;

    /// Removes the peer from `peer-list`. Fails for a peer that is not `Known` or that `net` is
    /// waiting to reconnect to: disconnect first.
    fn remove_peer(&self, id: PeerId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Starts the connection flow if the peer is `Known` and `net` is not waiting to reconnect to
    /// it; does nothing otherwise.
    fn connect(&self, id: PeerId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stops the connection and the reconnects; the peer becomes `Known` and stays in `peer-list`.
    fn disconnect(&self, id: PeerId) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Fails for a peer that is not `Connected`. The payload is sent unchanged; the peer's answer,
    /// if any, arrives through the queue of the command's owner.
    fn send(
        &self,
        id: PeerId,
        command: CommandString,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
