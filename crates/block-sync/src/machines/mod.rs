//! The state machines of block-sync (`docs/modules/block-sync/machines.md`). A machine takes one
//! [`Input`] with its context and returns [`Effect`]s; it does no I/O. `T` is the machine's own
//! token: what each of its requests was for, given back with the answer.

pub mod fetcher;
pub mod ibd;
pub mod peer;
pub mod reorg;

use bitcoin::block::Header;
use bitcoin::{Block, BlockHash, BlockHeight};
use interfaces::chain::{ChainCall, ChainResult};
use interfaces::net::PeerId;
use tokio::time::Instant;

/// Who sent a request, and who is told about a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    Ibd,
    Peer(PeerId),
    Reorg,
    Fetcher,
}

pub enum Input<T> {
    /// The peer is in the peer book.
    PeerConnected,
    PeerDisconnected(PeerId),
    /// `headers` that answer no request.
    Announced {
        headers: Vec<Header>,
    },
    Inv {
        hashes: Vec<BlockHash>,
    },
    /// A `block` that answers no request.
    Unsolicited {
        peer: PeerId,
        block: Block,
    },
    /// The block fetcher only: `requester` wants the blocks, from `peer`.
    Fetch {
        requester: Owner,
        peer: PeerId,
        hashes: Vec<BlockHash>,
    },
    Headers {
        token: T,
        headers: Vec<Header>,
    },
    Block {
        token: T,
        block: Block,
    },
    Chain {
        token: T,
        result: ChainResult,
    },
    /// A request to a peer failed: its deadline passed, it could not be sent, or `notfound`.
    Failed {
        token: T,
    },
    Wake {
        token: T,
    },
    Internal(Internal),
    /// After every event: send what can be sent now, within the room for requests to `chain`.
    Advance,
}

pub enum Effect<T> {
    /// `getheaders`, with a deadline.
    GetHeaders {
        peer: PeerId,
        locator: Vec<BlockHash>,
        stop: BlockHash,
        token: T,
    },
    /// `getdata` for one block, with a deadline.
    GetData {
        peer: PeerId,
        hash: BlockHash,
        token: T,
    },
    Chain {
        call: ChainCall,
        token: T,
    },
    /// Hands the blocks to the block fetcher, which tells the machine when each is stored or
    /// failed.
    FetchBlocks {
        peer: PeerId,
        hashes: Vec<BlockHash>,
    },
    /// `token` comes back as [`Input::Wake`] at `at`.
    Wake {
        at: Instant,
        token: T,
    },
    Mode(ModeChange),
    /// Delivered to `to` before the next event is read.
    Tell {
        to: Vec<Owner>,
        event: Internal,
    },
}

pub enum ModeChange {
    /// From IBD or reorg mode.
    EnterStandard,
    /// From standard mode, with A's height and A+1..B in height order.
    StartReorg {
        lca_height: BlockHeight,
        chain: Vec<BlockHash>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum Internal {
    /// A reorg ended; the context has the new tip.
    TipChanged,
    BlockStored(BlockHash),
    BlockFailed {
        hash: BlockHash,
        peer: PeerId,
    },
}

/// Every error stops the node.
pub type Effects<T> = Result<Vec<Effect<T>>, crate::error::BlockSyncError>;

pub trait Machine {
    type Token;
    /// What the machine reads, and nothing more; the shell builds it for every input.
    type Context<'a>;

    fn on_input(
        &mut self,
        context: &Self::Context<'_>,
        input: Input<Self::Token>,
    ) -> Effects<Self::Token>;
}

/// Headers in a full `headers` answer.
pub const MAX_HEADERS: usize = 2000;
/// Blocks a machine has fetched at a time from one peer. The docs leave the batch size TBD;
/// this is Core's `MAX_BLOCKS_IN_TRANSIT_PER_PEER`.
pub const BLOCKS_IN_FLIGHT: usize = 16;
/// All zeros: as many headers as allowed.
pub const NO_STOP: BlockHash = BlockHash::GENESIS_PREVIOUS_BLOCK_HASH;
