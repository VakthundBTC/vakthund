//! Sending block-sync's requests; each is registered in `Requests` with its handlers.

use bitcoin::BlockHash;
use interfaces::chain::ChainCall;
use interfaces::net::{Net, PeerId};

use crate::block_sync::BlockSync;
use crate::error::BlockSyncError;
use crate::messages;
use crate::requests::Request;
use crate::state::HeadersPurpose;

impl<N: Net> BlockSync<N> {
    /// Asks `chain` for the locator; its answer sends the `getheaders`.
    pub(crate) fn request_locator(
        &mut self,
        peer: PeerId,
        purpose: HeadersPurpose,
    ) -> Result<(), BlockSyncError> {
        self.requests
            .send_to_chain(ChainCall::Locator, Request::Locator { peer, purpose })?;
        Ok(())
    }

    pub(crate) async fn send_getheaders(
        &mut self,
        peer: PeerId,
        locator: Vec<BlockHash>,
        purpose: HeadersPurpose,
    ) -> Result<(), BlockSyncError> {
        if !self.peers.contains_key(&peer) {
            return Ok(());
        }
        let stop = match &purpose {
            // All zeros: as many headers as allowed.
            HeadersPurpose::Ibd => BlockHash::GENESIS_PREVIOUS_BLOCK_HASH,
            HeadersPurpose::Search { target, .. } => *target,
        };
        let (command, payload) = messages::getheaders(&locator, stop)?;
        let sent = self.net.send(peer, command, payload).await.is_ok();
        let request = Request::Headers {
            peer,
            locator,
            purpose,
        };
        self.requests.sent_to_peer(request, sent);
        Ok(())
    }
}
