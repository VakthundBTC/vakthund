//! Fetching, storing and connecting blocks (`docs/modules/block-sync/README.md`, "Block
//! download"), for IBD, candidates and reorg mode.

use bitcoin::{Block, BlockHash};
use interfaces::chain::{ChainCall, Tip};
use interfaces::net::{Net, PeerId};

use crate::block_sync::BlockSync;
use crate::error::BlockSyncError;
use crate::requests::Request;
use crate::state::{Candidate, Ibd, IbdPhase, Status};

impl<N: Net> BlockSync<N> {
    /// Nothing if the block is being fetched or stored already. Asks `chain` first: a stored
    /// block is not requested.
    pub(crate) fn fetch_block(
        &mut self,
        peer: PeerId,
        hash: BlockHash,
    ) -> Result<(), BlockSyncError> {
        if self.requests.is_block_wanted(&hash) {
            return Ok(());
        }
        self.requests
            .send_to_chain(ChainCall::Header(hash), Request::HasBlock { peer, hash })?;
        Ok(())
    }

    /// Whether a flow still needs the block: none does in reorg mode.
    pub(crate) fn needs_block(&self, hash: &BlockHash) -> bool {
        match &self.status {
            Status::Ibd(Ibd {
                phase: IbdPhase::Blocks(download),
                ..
            }) => download.pending().contains(hash),
            Status::Ibd(_) | Status::Reorg(_) => false,
            Status::Standard => self.peers.values().any(|data| match &data.candidate {
                Some(Candidate::Downloading(download)) => download.missing.contains(hash),
                _ => false,
            }),
        }
    }

    /// A requested block, or in standard mode a late or unsolicited one: stored if its header is.
    pub(crate) fn on_block(
        &mut self,
        peer: PeerId,
        block: Block,
        _requested: bool,
    ) -> Result<(), BlockSyncError> {
        let hash = block.block_hash();
        self.requests.send_to_chain(
            ChainCall::StoreBlock(block),
            Request::StoreBlock { peer, hash },
        )?;
        Ok(())
    }

    pub(crate) fn on_block_stored(&mut self, hash: BlockHash) {
        match &mut self.status {
            Status::Ibd(Ibd {
                phase: IbdPhase::Blocks(download),
                ..
            }) => {
                let pending =
                    &download.hashes[download.connect_sent_index..download.requested_index];
                if pending.contains(&hash) {
                    download.stored.insert(hash);
                }
            }
            Status::Standard => {
                for data in self.peers.values_mut() {
                    if let Some(Candidate::Downloading(download)) = &mut data.candidate {
                        download.missing.remove(&hash);
                    }
                }
            }
            Status::Ibd(_) | Status::Reorg(_) => {}
        }
    }

    /// Counts only the connect the running download or reorg sent: answers of an earlier one
    /// move the tip, nothing else.
    pub(crate) async fn on_block_connected(
        &mut self,
        hash: BlockHash,
        tip: Tip,
    ) -> Result<(), BlockSyncError> {
        self.tip = tip;
        match &mut self.status {
            Status::Ibd(Ibd {
                phase: IbdPhase::Blocks(download),
                ..
            }) => {
                if download.hashes.get(download.connected_index) == Some(&hash) {
                    download.connected_index += 1;
                    if download.connected_index == download.hashes.len() {
                        return self.ibd_finished().await;
                    }
                }
            }
            Status::Reorg(reorg) => {
                if reorg.chain.get(reorg.connected_index) == Some(&hash) {
                    reorg.connected_index += 1;
                    if reorg.connected_index == reorg.chain.len() {
                        self.status = Status::Standard;
                    }
                }
            }
            Status::Ibd(_) | Status::Standard => {}
        }
        Ok(())
    }
}
