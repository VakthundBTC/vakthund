//! What a failed request means for its flow.

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

    /// `docs/modules/block-sync/README.md`, "Failed block request".
    pub(crate) async fn on_block_request_failed(
        &mut self,
        peer: PeerId,
    ) -> Result<(), BlockSyncError> {
        match self.status {
            Status::Ibd(_) => self.sync_peer_failed(peer).await,
            // Other candidates that need the block request it with the standard-mode flow, which
            // comes next.
            Status::Standard | Status::Reorg(_) => {
                self.drop_candidate(peer);
                Ok(())
            }
        }
    }

    fn drop_candidate(&mut self, peer: PeerId) {
        if let Some(data) = self.peers.get_mut(&peer) {
            data.candidate = None;
        }
    }
}
