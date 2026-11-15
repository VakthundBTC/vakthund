//! Received blocks, in IBD and standard mode (`docs/modules/block-sync/README.md`, "Block
//! download").

use bitcoin::Block;
use interfaces::net::{Net, PeerId};

use crate::block_sync::BlockSync;

impl<N: Net> BlockSync<N> {
    /// A requested block, or in standard mode a late or unsolicited one. Comes next.
    pub(crate) fn on_block(&mut self, _peer: PeerId, _block: Block, _requested: bool) {}
}
