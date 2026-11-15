//! Interface of `chain` (`docs/modules/chain/README.md`, "Interface"): requests on the chain queue,
//! answers in the queue each request names.

use std::fmt;
use std::sync::Arc;

use bitcoin::block::Header;
use bitcoin::pow::Work;
use bitcoin::{Block, BlockHash, BlockHeight, OutPoint, TxOut};

use crate::queue::Queue;

/// Bitcoin Core's reject reason of the failed check, e.g. `high-hash`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RejectReason(pub String);

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderEntry {
    pub header: Header,
    pub height: BlockHeight,
    pub chain_work: Work,
    pub has_block: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tip {
    pub hash: BlockHash,
    pub height: BlockHeight,
    pub chain_work: Work,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coin {
    pub output: TxOut,
    /// Height of the block that created the output.
    pub height: BlockHeight,
    pub is_coinbase: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedHeaders {
    pub first_height: BlockHeight,
    pub last_height: BlockHeight,
    pub last_chain_work: Work,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChainError {
    #[error("header is not stored")]
    UnknownHeader,
    #[error("first header's parent is not stored")]
    NotConnected,
    /// `index` is the position of the invalid header in the input.
    #[error("header {index} is invalid: {reason}")]
    InvalidHeader { index: usize, reason: RejectReason },
    #[error("block is not stored")]
    NoBlock,
    #[error("block's parent is not the tip block")]
    NotSequential,
    /// The block's transactions do not match its header.
    #[error("block is mutated: {reason}")]
    Mutated { reason: RejectReason },
    #[error("block is invalid by consensus: {reason}")]
    Consensus { reason: RejectReason },
    #[error("genesis block cannot be disconnected")]
    TipIsGenesis,
    /// The store engine failed; the message is the engine's error.
    #[error("storage failure: {0}")]
    Storage(String),
}

/// Chosen by the client; `chain` echoes it in the answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChainRequestId(pub u64);

/// Requests that write run one at a time, in the order `chain` receives them. A request that reads
/// sees every write received before it. Answers may arrive in any order.
pub struct ChainRequest {
    pub id: ChainRequestId,
    pub reply: Arc<dyn Queue<ChainResponse>>,
    pub call: ChainCall,
}

/// Every call can also fail with `Storage`. Every other error is a rejection that writes nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainCall {
    /// Headers in chain order, each extending the previous. Writes nothing.
    ///
    /// Errors: `NotConnected`, `InvalidHeader`.
    ValidateHeaders(Vec<Header>),
    /// Only for headers that `ValidateHeaders` accepted.
    ///
    /// Errors: `NotConnected`.
    StoreHeaders(Vec<Header>),
    Header(BlockHash),
    /// The header at this height on the active chain.
    HeaderAt(BlockHeight),
    /// The block locator of the active chain (`docs/flows/mode-standard.md`, "Block locator"),
    /// built against one tip: newest first, ending with genesis.
    Locator,
    /// Runs the mutation and basic checks. Storing an already stored block succeeds.
    ///
    /// Errors: `UnknownHeader`, `Mutated`, `Consensus`.
    StoreBlock(Block),
    /// Runs the checks with context and moves the tip to the block.
    ///
    /// Errors: `UnknownHeader`, `NoBlock`, `NotSequential`, `Consensus`. `Consensus` here is
    /// always a consensus failure: the block passed the mutation checks when it was stored.
    ConnectBlock(BlockHash),
    Block(BlockHash),
    Tip,
    /// Reverts the tip block and moves the tip to its parent.
    ///
    /// Errors: `TipIsGenesis`.
    DisconnectTip,
    Coin(OutPoint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainResponse {
    pub id: ChainRequestId,
    pub result: ChainResult,
}

/// One variant per [`ChainCall`], so an answer cannot carry another call's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainResult {
    ValidateHeaders(Result<ValidatedHeaders, ChainError>),
    StoreHeaders(Result<(), ChainError>),
    Header(Result<Option<HeaderEntry>, ChainError>),
    HeaderAt(Result<Option<HeaderEntry>, ChainError>),
    Locator(Result<Vec<BlockHash>, ChainError>),
    StoreBlock(Result<(), ChainError>),
    /// The new tip.
    ConnectBlock(Result<Tip, ChainError>),
    Block(Result<Option<Block>, ChainError>),
    Tip(Result<Tip, ChainError>),
    /// The new tip.
    DisconnectTip(Result<Tip, ChainError>),
    Coin(Result<Option<Coin>, ChainError>),
}
