//! Peers connecting and disconnecting, and the IBD sync peer they change.

use interfaces::net::{Net, PeerId};
use tokio::time::Instant;

use crate::block_sync::BlockSync;
use crate::error::BlockSyncError;
use crate::state::{IbdPhase, PeerData, Status};

impl<N: Net> BlockSync<N> {
    pub(crate) async fn on_peer_connected(&mut self, peer: PeerId) -> Result<(), BlockSyncError> {
        self.peers.insert(peer, PeerData::default());
        self.choose_sync_peer().await
    }

    /// Its requests end: a block request it had fails under the "Failed block request" rule
    /// (`docs/modules/block-sync/README.md`), and so does its role as the sync peer.
    pub(crate) async fn on_peer_disconnected(
        &mut self,
        peer: PeerId,
    ) -> Result<(), BlockSyncError> {
        let blocks = self.requests.end_peer(peer);
        // Its candidate goes with it; another candidate that needs its blocks requests them.
        self.peers.remove(&peer);
        for hash in blocks {
            self.refetch(hash)?;
        }
        self.sync_peer_failed(peer).await
    }

    /// Every connected peer failed in turn and the wait is over.
    pub(crate) async fn on_ibd_restart(&mut self) -> Result<(), BlockSyncError> {
        if let Status::Ibd(ibd) = &mut self.status {
            ibd.restart_at = None;
        }
        self.choose_sync_peer().await
    }

    /// Nothing unless `peer` is the IBD sync peer. Its requests end and another peer continues:
    /// the header download from the last header received, or IBD from the headers if the blocks
    /// were downloading (`docs/flows/mode-ibd.md`). Headers being validated and stored are kept.
    pub(crate) async fn sync_peer_failed(&mut self, peer: PeerId) -> Result<(), BlockSyncError> {
        let Status::Ibd(ibd) = &mut self.status else {
            return Ok(());
        };
        if ibd.sync_peer != Some(peer) {
            return Ok(());
        }
        ibd.sync_peer = None;
        ibd.tried.insert(peer);
        if matches!(ibd.phase, IbdPhase::Blocks(_)) {
            ibd.phase = IbdPhase::Headers {
                received: Vec::new(),
            };
        }
        self.requests.end_peer(peer);
        self.choose_sync_peer().await
    }

    /// In IBD without a sync peer: a random connected peer that has not failed. When every
    /// connected peer has, IBD waits `request_timeout_secs` and starts again with the first.
    pub(crate) async fn choose_sync_peer(&mut self) -> Result<(), BlockSyncError> {
        let Status::Ibd(ibd) = &mut self.status else {
            return Ok(());
        };
        if ibd.sync_peer.is_some() {
            return Ok(());
        }
        let untried: Vec<PeerId> = self
            .peers
            .keys()
            .filter(|peer| !ibd.tried.contains(peer))
            .copied()
            .collect();
        if untried.is_empty() {
            if !self.peers.is_empty() {
                ibd.tried.clear();
                ibd.restart_at = Some(Instant::now() + self.timeout);
            }
            return Ok(());
        }
        let Some(&peer) = untried.get(rand::random_range(0..untried.len())) else {
            return Ok(());
        };
        ibd.sync_peer = Some(peer);
        // A peer that connects during the wait ends it.
        ibd.restart_at = None;
        self.continue_ibd_headers().await
    }
}
