//! IBD (`docs/flows/mode-ibd.md`): the headers from the sync peer, then their blocks, connected
//! in height order.

use std::collections::HashSet;

use bitcoin::BlockHash;
use bitcoin::block::Header;
use interfaces::chain::ChainCall;
use interfaces::net::{Net, PeerId};

use super::{BLOCKS_IN_FLIGHT, MAX_HEADERS};
use crate::block_sync::BlockSync;
use crate::error::BlockSyncError;
use crate::requests::Request;
use crate::state::{BlockDownload, HeadersPurpose, Ibd, IbdPhase, Status};

/// How far past the next block to connect blocks are fetched: Core's `BLOCK_DOWNLOAD_WINDOW`.
const DOWNLOAD_WINDOW: usize = 1024;

impl<N: Net> BlockSync<N> {
    /// Full answers continue from the last header; a shorter one ends the header download.
    pub(crate) async fn on_ibd_headers(
        &mut self,
        peer: PeerId,
        headers: Vec<Header>,
    ) -> Result<(), BlockSyncError> {
        let Status::Ibd(ibd) = &mut self.status else {
            return Ok(());
        };
        let IbdPhase::Headers { received } = &mut ibd.phase else {
            return Ok(());
        };
        if ibd.sync_peer != Some(peer) {
            return Ok(());
        }
        let full = headers.len() == MAX_HEADERS;
        received.extend(headers);
        if full && let Some(last) = received.last().map(Header::block_hash) {
            return self
                .send_getheaders(peer, vec![last], HeadersPurpose::Ibd)
                .await;
        }
        if received.is_empty() {
            return self.ibd_finished().await;
        }
        let headers = std::mem::take(received);
        ibd.phase = IbdPhase::StoringHeaders;
        let call = ChainCall::ValidateHeaders(headers.clone());
        self.requests
            .send_to_chain(call, Request::IbdValidateHeaders { peer, headers })?;
        Ok(())
    }

    pub(crate) fn on_ibd_headers_valid(
        &mut self,
        headers: Vec<Header>,
    ) -> Result<(), BlockSyncError> {
        if !self.storing_ibd_headers() {
            return Ok(());
        }
        let hashes = headers.iter().map(Header::block_hash).collect();
        self.requests.send_to_chain(
            ChainCall::StoreHeaders(headers),
            Request::IbdStoreHeaders { hashes },
        )?;
        Ok(())
    }

    /// The docs do not cover it: the valid headers are kept and another sync peer continues.
    pub(crate) async fn on_ibd_headers_invalid(
        &mut self,
        peer: PeerId,
        mut headers: Vec<Header>,
        valid: usize,
    ) -> Result<(), BlockSyncError> {
        if !self.storing_ibd_headers() {
            return Ok(());
        }
        headers.truncate(valid);
        let Status::Ibd(ibd) = &mut self.status else {
            return Ok(());
        };
        ibd.phase = IbdPhase::Headers { received: headers };
        if ibd.sync_peer == Some(peer) {
            self.sync_peer_failed(peer).await
        } else {
            self.continue_ibd_headers().await
        }
    }

    pub(crate) fn on_ibd_headers_stored(&mut self, hashes: Vec<BlockHash>) {
        if !self.storing_ibd_headers() {
            return;
        }
        if let Status::Ibd(ibd) = &mut self.status {
            ibd.phase = IbdPhase::Blocks(BlockDownload {
                hashes,
                requested_index: 0,
                connect_sent_index: 0,
                connected_index: 0,
                stored: HashSet::new(),
            });
        }
    }

    /// Sends `connect_block` for the stored blocks in height order, and fetches the next blocks
    /// from the sync peer.
    pub(crate) fn advance_ibd(&mut self) -> Result<(), BlockSyncError> {
        let Status::Ibd(Ibd {
            sync_peer,
            phase: IbdPhase::Blocks(download),
            ..
        }) = &mut self.status
        else {
            return Ok(());
        };
        // `chain` runs writes in request order, so a connect waits for no answer.
        while let Some(&hash) = download.hashes.get(download.connect_sent_index)
            && download.stored.contains(&hash)
            && self.requests.has_room(1)
        {
            download.stored.remove(&hash);
            self.requests.send_to_chain(
                ChainCall::ConnectBlock(hash),
                Request::ConnectBlock { hash },
            )?;
            download.connect_sent_index += 1;
        }
        let Some(peer) = *sync_peer else {
            return Ok(());
        };
        let mut fetches = Vec::new();
        loop {
            let fetching = download
                .requested_index
                .saturating_sub(download.connect_sent_index)
                .saturating_sub(download.stored.len());
            let Some(&hash) = download.hashes.get(download.requested_index) else {
                break;
            };
            if fetching >= BLOCKS_IN_FLIGHT
                || download.requested_index >= download.connected_index + DOWNLOAD_WINDOW
                || !self.requests.has_room(fetches.len() + 1)
            {
                break;
            }
            fetches.push(hash);
            download.requested_index += 1;
        }
        for hash in fetches {
            self.fetch_block(peer, hash)?;
        }
        Ok(())
    }

    /// Every block is connected, or there was nothing to download. IBD ends if the tip has the
    /// minimum chain work; otherwise the sync peer was behind and has failed.
    pub(crate) async fn ibd_finished(&mut self) -> Result<(), BlockSyncError> {
        let Status::Ibd(ibd) = &mut self.status else {
            return Ok(());
        };
        if self.tip.chain_work >= self.minimum_chain_work {
            self.status = Status::Standard;
            return Ok(());
        }
        ibd.phase = IbdPhase::Headers {
            received: Vec::new(),
        };
        match ibd.sync_peer {
            Some(peer) => self.sync_peer_failed(peer).await,
            None => self.choose_sync_peer().await,
        }
    }

    /// Sends the sync peer the next `getheaders` of the header download: after the last header
    /// received, or from the active chain.
    pub(crate) async fn continue_ibd_headers(&mut self) -> Result<(), BlockSyncError> {
        let Status::Ibd(Ibd {
            sync_peer: Some(peer),
            phase: IbdPhase::Headers { received },
            ..
        }) = &self.status
        else {
            return Ok(());
        };
        let peer = *peer;
        match received.last().map(Header::block_hash) {
            Some(last) => {
                self.send_getheaders(peer, vec![last], HeadersPurpose::Ibd)
                    .await
            }
            None => self.request_locator(peer, HeadersPurpose::Ibd),
        }
    }

    fn storing_ibd_headers(&self) -> bool {
        matches!(
            &self.status,
            Status::Ibd(Ibd {
                phase: IbdPhase::StoringHeaders,
                ..
            })
        )
    }
}
