//! Standard mode (`docs/flows/mode-standard.md`) and its candidates.

use bitcoin::BlockHash;
use bitcoin::block::Header;
use interfaces::net::{Net, PeerId};

use crate::block_sync::BlockSync;

impl<N: Net> BlockSync<N> {
    /// The LCA search, which comes next.
    pub(crate) fn on_search_headers(
        &mut self,
        _peer: PeerId,
        _target: BlockHash,
        _collected: Vec<Header>,
        _headers: Vec<Header>,
    ) {
    }

    /// Processing headers, which comes next.
    pub(crate) fn on_announced_headers(&mut self, _peer: PeerId, _headers: Vec<Header>) {}

    /// Processing inventory, which comes next.
    pub(crate) fn on_inv(&mut self, _peer: PeerId, _hashes: Vec<BlockHash>) {}
}
