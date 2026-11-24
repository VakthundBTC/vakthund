//! The peer book: what block-sync learns about each connected peer, for metrics
//! (`docs/modules/block-sync/README.md`, "Internal structure"). The shell writes it; machines
//! only read it.

use std::collections::HashMap;

use bitcoin::BlockHash;
use interfaces::net::PeerId;
use tokio::time::Instant;

#[derive(Default)]
pub struct PeerBook {
    peers: HashMap<PeerId, PeerInfo>,
}

#[expect(
    dead_code,
    reason = "read by metrics, which the node does not have yet"
)]
pub struct PeerInfo {
    pub connected_at: Instant,
    /// The last header of its last announcement.
    pub best_announced: Option<BlockHash>,
    pub last_announced_at: Option<Instant>,
    pub requests_sent: u64,
    /// Deadlines passed, `notfound`, sends that failed.
    pub requests_failed: u64,
    pub blocks_received: u64,
}

impl PeerBook {
    pub fn connected(&mut self, peer: PeerId, now: Instant) {
        let info = PeerInfo {
            connected_at: now,
            best_announced: None,
            last_announced_at: None,
            requests_sent: 0,
            requests_failed: 0,
            blocks_received: 0,
        };
        self.peers.insert(peer, info);
    }

    pub fn disconnected(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
    }

    pub fn contains(&self, peer: PeerId) -> bool {
        self.peers.contains_key(&peer)
    }

    pub fn peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.keys().copied()
    }

    pub fn announced(&mut self, peer: PeerId, hash: BlockHash, now: Instant) {
        if let Some(info) = self.peers.get_mut(&peer) {
            info.best_announced = Some(hash);
            info.last_announced_at = Some(now);
        }
    }

    pub fn request_sent(&mut self, peer: PeerId) {
        if let Some(info) = self.peers.get_mut(&peer) {
            info.requests_sent += 1;
        }
    }

    pub fn request_failed(&mut self, peer: PeerId) {
        if let Some(info) = self.peers.get_mut(&peer) {
            info.requests_failed += 1;
        }
    }

    pub fn block_received(&mut self, peer: PeerId) {
        if let Some(info) = self.peers.get_mut(&peer) {
            info.blocks_received += 1;
        }
    }
}
