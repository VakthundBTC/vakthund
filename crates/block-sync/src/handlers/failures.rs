//! What a failed request means for its flow.

use bitcoin::BlockHash;
use interfaces::net::{Net, PeerId};

use crate::block_sync::BlockSync;
use crate::error::BlockSyncError;
use crate::state::{HeadersPurpose, Status};

impl<N: Net> BlockSync<N> {
    pub(crate) async fn on_headers_request_failed(
        &mut self,
        peer: PeerId,
        purpose: HeadersPurpose,
    ) -> Result<(), BlockSyncError> {
        match purpose {
            HeadersPurpose::Ibd => self.sync_peer_failed(peer).await,
            // `docs/flows/mode-standard.md`: a failed request ends the search as a dropped H.
            HeadersPurpose::Search { .. } => {
                self.drop_candidate(peer);
                Ok(())
            }
        }
    }

    /// `docs/modules/block-sync/README.md`, "Failed block request": in IBD the sync peer has
    /// failed; otherwise the peer's candidate is dropped and another candidate that needs the
    /// block requests it from its own peer.
    pub(crate) async fn on_block_request_failed(
        &mut self,
        peer: PeerId,
        hash: BlockHash,
    ) -> Result<(), BlockSyncError> {
        match self.status {
            Status::Ibd(_) => self.sync_peer_failed(peer).await,
            Status::Standard | Status::Reorg(_) => {
                self.drop_candidate(peer);
                self.refetch(hash)
            }
        }
    }
}
