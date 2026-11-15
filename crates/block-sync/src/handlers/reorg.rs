//! Reorg mode (`docs/flows/mode-reorg.md`): every block of A+1..B is stored. It also appends
//! blocks to the tip, with k = 0.

use interfaces::chain::ChainCall;
use interfaces::net::Net;

use bitcoin::{BlockHash, BlockHeight};

use crate::block_sync::BlockSync;
use crate::error::BlockSyncError;
use crate::requests::Request;
use crate::state::{Reorg, Status};

impl<N: Net> BlockSync<N> {
    /// Drops every candidate (`docs/modules/block-sync/README.md`, "Candidates"). The tip cache is
    /// exact: only one tip change runs at a time, and none runs in standard mode.
    pub(crate) fn start_reorg(
        &mut self,
        lca: BlockHash,
        lca_height: BlockHeight,
        chain: Vec<BlockHash>,
    ) {
        for data in self.peers.values_mut() {
            data.candidate = None;
        }
        self.status = Status::Reorg(Reorg {
            lca,
            lca_height,
            disconnect_sent_height: self.tip.height,
            chain,
            connect_sent_index: 0,
            connected_index: 0,
        });
    }

    /// Sends `disconnect_tip` down to A, then `connect_block` for A+1..B: `chain` runs them in
    /// request order, so none waits for an answer.
    pub(crate) fn advance_reorg(&mut self) -> Result<(), BlockSyncError> {
        let Status::Reorg(reorg) = &mut self.status else {
            return Ok(());
        };
        while reorg.disconnect_sent_height > reorg.lca_height && self.requests.has_room(1) {
            self.requests
                .send_to_chain(ChainCall::DisconnectTip, Request::DisconnectTip)?;
            reorg.disconnect_sent_height =
                BlockHeight::from_u32(reorg.disconnect_sent_height.to_u32().saturating_sub(1));
        }
        while reorg.disconnect_sent_height <= reorg.lca_height
            && let Some(&hash) = reorg.chain.get(reorg.connect_sent_index)
            && self.requests.has_room(1)
        {
            self.requests.send_to_chain(
                ChainCall::ConnectBlock(hash),
                Request::ConnectBlock { hash },
            )?;
            reorg.connect_sent_index += 1;
        }
        Ok(())
    }
}
