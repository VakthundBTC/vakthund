//! Standard mode (`docs/flows/mode-standard.md`) and its candidates
//! (`docs/modules/block-sync/README.md`, "Candidates").

use bitcoin::block::Header;
use bitcoin::{BlockHash, BlockHeight};
use interfaces::chain::{ChainCall, HeaderEntry, ValidatedHeaders};
use interfaces::net::{Net, PeerId};

use super::{BLOCKS_IN_FLIGHT, MAX_HEADERS};
use crate::block_sync::BlockSync;
use crate::error::BlockSyncError;
use crate::requests::Request;
use crate::state::{Candidate, Download, HeadersPurpose, Resolution, Status};

impl<N: Net> BlockSync<N> {
    /// A newer announcement from the peer replaces its candidate. A parent `chain` does not know
    /// starts the LCA search.
    pub(crate) fn on_announced_headers(
        &mut self,
        peer: PeerId,
        headers: Vec<Header>,
    ) -> Result<(), BlockSyncError> {
        if headers.is_empty() {
            return Ok(());
        }
        self.set_candidate(peer, Candidate::Resolving);
        self.validate_candidate(peer, headers, false)
    }

    /// One candidate per peer: only the last hash is followed (`docs/flows/mode-standard.md`,
    /// "Processing inventory").
    pub(crate) fn on_inv(
        &mut self,
        peer: PeerId,
        hashes: Vec<BlockHash>,
    ) -> Result<(), BlockSyncError> {
        let Some(&hash) = hashes.last() else {
            return Ok(());
        };
        self.requests
            .send_to_chain(ChainCall::Header(hash), Request::InvHeader { peer, hash })?;
        Ok(())
    }

    pub(crate) fn on_inv_unknown(
        &mut self,
        peer: PeerId,
        hash: BlockHash,
    ) -> Result<(), BlockSyncError> {
        self.set_candidate(peer, Candidate::Resolving);
        self.search(peer, hash)
    }

    pub(crate) fn on_candidate_not_connected(
        &mut self,
        peer: PeerId,
        headers: Vec<Header>,
        searched: bool,
    ) -> Result<(), BlockSyncError> {
        match headers.last() {
            Some(last) if !searched => self.search(peer, last.block_hash()),
            _ => {
                self.drop_candidate(peer);
                Ok(())
            }
        }
    }

    /// The LCA search loop: full answers continue from the last header received.
    pub(crate) async fn on_search_headers(
        &mut self,
        peer: PeerId,
        target: BlockHash,
        mut collected: Vec<Header>,
        headers: Vec<Header>,
    ) -> Result<(), BlockSyncError> {
        if !self.is_resolving(peer) {
            return Ok(());
        }
        let full = headers.len() == MAX_HEADERS;
        collected.extend(headers);
        let last = collected.last().map(Header::block_hash);
        if last == Some(target) {
            return self.validate_candidate(peer, collected, true);
        }
        if full && let Some(last) = last {
            let purpose = HeadersPurpose::Search { target, collected };
            return self.send_getheaders(peer, vec![last], purpose).await;
        }
        // H is no longer on the peer's active chain.
        self.drop_candidate(peer);
        Ok(())
    }

    /// The parent is stored: walk back from it to the active chain. Headers on the tip need no
    /// walk.
    pub(crate) fn on_candidate_valid(
        &mut self,
        peer: PeerId,
        headers: Vec<Header>,
        validated: ValidatedHeaders,
    ) -> Result<(), BlockSyncError> {
        let parent_height = validated.first_height.to_u32().checked_sub(1);
        let (Some(first), Some(parent_height)) = (headers.first(), parent_height) else {
            self.drop_candidate(peer);
            return Ok(());
        };
        let cursor = first.prev_blockhash;
        let resolution = Resolution {
            headers,
            first_height: validated.first_height,
            last_chain_work: validated.last_chain_work,
            cursor,
            height: BlockHeight::from_u32(parent_height),
            path: Vec::new(),
            skip: 0,
        };
        if cursor == self.tip.hash {
            return self.resolved(peer, resolution);
        }
        self.ask_active(peer, resolution)
    }

    pub(crate) fn on_lca_active(
        &mut self,
        peer: PeerId,
        resolution: Resolution,
        active: Option<BlockHash>,
    ) -> Result<(), BlockSyncError> {
        if active != Some(resolution.cursor) {
            let call = ChainCall::Header(resolution.cursor);
            self.requests
                .send_to_chain(call, Request::LcaParent { peer, resolution })?;
            return Ok(());
        }
        // The cursor is the LCA. If it is the headers' parent, their first ones may be on the
        // active chain too: the answer to a sparse locator starts below the fork.
        if resolution.path.is_empty() && resolution.first_height <= self.tip.height {
            return self.ask_skip(peer, resolution);
        }
        self.resolved(peer, resolution)
    }

    pub(crate) fn on_lca_parent(
        &mut self,
        peer: PeerId,
        mut resolution: Resolution,
        parent: Option<HeaderEntry>,
    ) -> Result<(), BlockSyncError> {
        let height = resolution.height.to_u32().checked_sub(1);
        let (Some(parent), Some(height)) = (parent, height) else {
            self.drop_candidate(peer);
            return Ok(());
        };
        resolution.path.push(resolution.cursor);
        resolution.cursor = parent.header.prev_blockhash;
        resolution.height = BlockHeight::from_u32(height);
        self.ask_active(peer, resolution)
    }

    pub(crate) fn on_skip_active(
        &mut self,
        peer: PeerId,
        mut resolution: Resolution,
        active: Option<BlockHash>,
    ) -> Result<(), BlockSyncError> {
        let on_active = resolution
            .headers
            .get(resolution.skip)
            .is_some_and(|header| Some(header.block_hash()) == active);
        if !on_active {
            return self.resolved(peer, resolution);
        }
        resolution.skip += 1;
        if resolution.skip == resolution.headers.len() {
            // Every header is on the active chain already.
            self.drop_candidate(peer);
            return Ok(());
        }
        if skip_height(&resolution) > self.tip.height {
            return self.resolved(peer, resolution);
        }
        self.ask_skip(peer, resolution)
    }

    /// The candidate's LCA is known. A chain that does not extend the tip is kept only with more
    /// chain work than the tip (`docs/flows/mode-standard.md`, "Processing headers").
    fn resolved(&mut self, peer: PeerId, mut resolution: Resolution) -> Result<(), BlockSyncError> {
        let (lca, lca_height) = match resolution
            .skip
            .checked_sub(1)
            .and_then(|index| resolution.headers.get(index))
        {
            Some(header) => (
                header.block_hash(),
                BlockHeight::from_u32(skip_height(&resolution).to_u32().saturating_sub(1)),
            ),
            None => (resolution.cursor, resolution.height),
        };
        if lca != self.tip.hash && resolution.last_chain_work <= self.tip.chain_work {
            self.drop_candidate(peer);
            return Ok(());
        }
        let new = resolution.headers.split_off(resolution.skip);
        let mut chain: Vec<BlockHash> = resolution.path.into_iter().rev().collect();
        chain.extend(new.iter().map(Header::block_hash));
        if chain.is_empty() {
            self.drop_candidate(peer);
            return Ok(());
        }
        if !new.is_empty() {
            self.requests
                .send_to_chain(ChainCall::StoreHeaders(new), Request::StoreCandidateHeaders)?;
        }
        let download = Download {
            lca,
            lca_height,
            missing: chain.iter().copied().collect(),
            chain,
            requested_index: 0,
        };
        self.set_candidate(peer, Candidate::Downloading(download));
        Ok(())
    }

    /// Fetches the candidates' next blocks, and starts reorg mode with the first candidate whose
    /// blocks are all stored.
    pub(crate) fn advance_candidates(&mut self) -> Result<(), BlockSyncError> {
        let mut fetches = Vec::new();
        for (peer, data) in &mut self.peers {
            let Some(Candidate::Downloading(download)) = &mut data.candidate else {
                continue;
            };
            while let Some(&hash) = download.chain.get(download.requested_index) {
                let unrequested = download.chain.len() - download.requested_index;
                let fetching = download.missing.len().saturating_sub(unrequested);
                if fetching >= BLOCKS_IN_FLIGHT || !self.requests.has_room(fetches.len() + 1) {
                    break;
                }
                fetches.push((*peer, hash));
                download.requested_index += 1;
            }
        }
        for (peer, hash) in fetches {
            self.fetch_block(peer, hash)?;
        }
        let complete = self.peers.values().find_map(|data| match &data.candidate {
            Some(Candidate::Downloading(download))
                if download.missing.is_empty()
                    && download.requested_index == download.chain.len() =>
            {
                Some((download.lca, download.lca_height, download.chain.clone()))
            }
            _ => None,
        });
        if let Some((lca, lca_height, chain)) = complete {
            self.start_reorg(lca, lca_height, chain);
            return self.advance_reorg();
        }
        Ok(())
    }

    /// Another candidate that needs the block requests it from its own peer.
    pub(crate) fn refetch(&mut self, hash: BlockHash) -> Result<(), BlockSyncError> {
        if !matches!(self.status, Status::Standard) {
            return Ok(());
        }
        let peer = self
            .peers
            .iter()
            .find_map(|(peer, data)| match &data.candidate {
                Some(Candidate::Downloading(download)) if download.missing.contains(&hash) => {
                    Some(*peer)
                }
                _ => None,
            });
        match peer {
            Some(peer) => self.fetch_block(peer, hash),
            None => Ok(()),
        }
    }

    pub(crate) fn drop_candidate(&mut self, peer: PeerId) {
        if let Some(data) = self.peers.get_mut(&peer) {
            data.candidate = None;
        }
    }

    fn search(&mut self, peer: PeerId, target: BlockHash) -> Result<(), BlockSyncError> {
        let purpose = HeadersPurpose::Search {
            target,
            collected: Vec::new(),
        };
        self.request_locator(peer, purpose)
    }

    fn validate_candidate(
        &mut self,
        peer: PeerId,
        headers: Vec<Header>,
        searched: bool,
    ) -> Result<(), BlockSyncError> {
        let call = ChainCall::ValidateHeaders(headers.clone());
        let request = Request::ValidateCandidate {
            peer,
            headers,
            searched,
        };
        self.requests.send_to_chain(call, request)?;
        Ok(())
    }

    fn ask_active(&mut self, peer: PeerId, resolution: Resolution) -> Result<(), BlockSyncError> {
        let call = ChainCall::HeaderAt(resolution.height);
        self.requests
            .send_to_chain(call, Request::LcaActive { peer, resolution })?;
        Ok(())
    }

    fn ask_skip(&mut self, peer: PeerId, resolution: Resolution) -> Result<(), BlockSyncError> {
        let call = ChainCall::HeaderAt(skip_height(&resolution));
        self.requests
            .send_to_chain(call, Request::SkipActive { peer, resolution })?;
        Ok(())
    }

    fn set_candidate(&mut self, peer: PeerId, candidate: Candidate) {
        if let Some(data) = self.peers.get_mut(&peer) {
            data.candidate = Some(candidate);
        }
    }

    fn is_resolving(&self, peer: PeerId) -> bool {
        matches!(
            self.peers
                .get(&peer)
                .and_then(|data| data.candidate.as_ref()),
            Some(Candidate::Resolving)
        )
    }
}

/// The height of `headers[skip]`.
fn skip_height(resolution: &Resolution) -> BlockHeight {
    let skip = u32::try_from(resolution.skip).unwrap_or(u32::MAX);
    BlockHeight::from_u32(resolution.first_height.to_u32().saturating_add(skip))
}
