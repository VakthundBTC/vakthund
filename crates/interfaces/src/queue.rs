//! The push-only entry to a module's queue. The owning module keeps the receiving end, so other
//! modules do not depend on how the queue is built.

use bitcoin_p2p_messages::message::CommandString;

use crate::net::PeerId;

/// A received message routed by `net` to the owner of its command. The payload is not decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedMessage {
    pub peer: PeerId,
    pub command: CommandString,
    pub payload: Vec<u8>,
}

/// A full queue is a node failure: the node stops. Messages are never dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueFull;

/// Pushing never calls into the owning module and never waits.
pub trait MessageQueue: Send + Sync {
    fn push(&self, message: RoutedMessage) -> Result<(), QueueFull>;
}
