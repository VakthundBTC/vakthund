//! block-sync's in-memory state (`docs/modules/block-sync/README.md`, "State"). Only block-sync's
//! task reads and writes it, so nothing is behind a lock.

#![expect(
    dead_code,
    reason = "read and written by the IBD and standard-mode flows, which come next"
)]

use std::collections::HashSet;

use bitcoin::block::Header;
use bitcoin::{BlockHash, BlockHeight};
use interfaces::net::PeerId;
use tokio::time::Instant;

/// The mode (`docs/architecture/overview.md`, "Modes"), with the data only that mode uses.
pub enum Status {
    Ibd(Ibd),
    Standard,
    Reorg(Reorg),
}

/// `docs/flows/mode-ibd.md`.
pub struct Ibd {
    /// `None` while no peer is connected.
    pub sync_peer: Option<PeerId>,
    /// The peers that have failed as the sync peer, in the header or the block download, since
    /// every peer was last tried.
    pub tried: HashSet<PeerId>,
    /// Every connected peer has failed in turn: the next sync peer is chosen at this instant.
    pub restart_at: Option<Instant>,
    pub phase: IbdPhase,
}

pub enum IbdPhase {
    Headers {
        received: Vec<Header>,
    },
    /// `validate_headers` and `store_headers` of the whole header chain are in flight.
    StoringHeaders,
    Blocks(BlockDownload),
}

/// The blocks of the candidate headers `tip+1 ...`, connected in height order. Always
/// `connected_index <= connect_sent_index <= requested_index <= hashes.len()`.
pub struct BlockDownload {
    pub hashes: Vec<BlockHash>,
    /// The index of the next block to request: `hashes[..requested_index]` are requested from a
    /// peer or found stored.
    pub requested_index: usize,
    /// The index of the next block to send `connect_block` for:
    /// `hashes[..connect_sent_index]` already have a `connect_block` request on its way to
    /// `chain`.
    pub connect_sent_index: usize,
    /// The index of the next block to be connected: `chain` has answered `connect_block` with
    /// `Ok` for `hashes[..connected_index]`.
    pub connected_index: usize,
    /// Stored and not yet sent to `connect_block`.
    pub stored: HashSet<BlockHash>,
}

/// `docs/flows/mode-reorg.md`: every block of A+1..B is stored. Disconnecting is done when the
/// tip's height is `lca_height`, the reorg when `connected_index == chain.len()`.
pub struct Reorg {
    /// A.
    pub lca: BlockHash,
    pub lca_height: BlockHeight,
    /// The tip's height once every `disconnect_tip` sent so far is applied; requests are sent
    /// while it is above `lca_height`.
    pub disconnect_sent_height: BlockHeight,
    /// A+1..B in height order.
    pub chain: Vec<BlockHash>,
    /// The index of the next block to send `connect_block` for: `chain[..connect_sent_index]`
    /// already have a `connect_block` request on its way to `chain`.
    pub connect_sent_index: usize,
    /// The index of the next block to be connected: `chain` has answered `connect_block` with
    /// `Ok` for `chain[..connected_index]`.
    pub connected_index: usize,
}

/// The flows' data for a connected peer; its requests are in `Requests`.
#[derive(Default)]
pub struct PeerData {
    /// At most one per peer (`docs/modules/block-sync/README.md`, "Candidates").
    pub candidate: Option<Candidate>,
}

pub enum HeadersPurpose {
    /// The IBD header download from the sync peer.
    Ibd,
    /// The LCA search for H (`docs/flows/mode-standard.md`, "Processing headers").
    Search {
        target: BlockHash,
        collected: Vec<Header>,
    },
}

pub enum Candidate {
    /// The `getheaders` loop of the LCA search runs, or `chain` is asked about its headers.
    Resolving,
    /// Every block of LCA+1..H is stored once `missing` is empty.
    Downloading {
        lca: BlockHash,
        lca_height: BlockHeight,
        chain: Vec<BlockHash>,
        missing: HashSet<BlockHash>,
    },
}
