//! IBD (`docs/flows/mode-ibd.md`).

use bitcoin::block::Header;
use interfaces::net::{Net, PeerId};

use crate::block_sync::BlockSync;

impl<N: Net> BlockSync<N> {
    /// The IBD header download, which comes next.
    pub(crate) fn on_ibd_headers(&mut self, _peer: PeerId, _headers: Vec<Header>) {}
}
