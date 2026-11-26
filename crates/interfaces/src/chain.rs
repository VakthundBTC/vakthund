//! Interface of `chain` (`docs/modules/chain/README.md`). Calls that write run one at a time.

use std::fmt;
use std::future::Future;

use bitcoin::block::Header;
use bitcoin::pow::Work;
use bitcoin::{Block, BlockHash, BlockHeight, OutPoint, TxOut};

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

/// Every call can also return `Storage`. Every other error is a rejection that writes nothing.
pub trait Chain: Send + Sync {
    /// Headers in chain order, each extending the previous. Writes nothing.
    ///
    /// Errors: `NotConnected`, `InvalidHeader`.
    fn validate_headers(
        &self,
        headers: Vec<Header>,
    ) -> impl Future<Output = Result<ValidatedHeaders, ChainError>> + Send;

    /// Only for headers that `validate_headers` accepted.
    ///
    /// Errors: `NotConnected`.
    fn store_headers(
        &self,
        headers: Vec<Header>,
    ) -> impl Future<Output = Result<(), ChainError>> + Send;

    fn header(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<HeaderEntry>, ChainError>> + Send;

    /// The header at this height on the active chain.
    fn header_at(
        &self,
        height: BlockHeight,
    ) -> impl Future<Output = Result<Option<HeaderEntry>, ChainError>> + Send;

    /// Runs the mutation and basic checks. Storing an already stored block succeeds.
    ///
    /// Errors: `UnknownHeader`, `Mutated`, `Consensus`.
    fn store_block(&self, block: Block) -> impl Future<Output = Result<(), ChainError>> + Send;

    /// Runs the checks with context and moves the tip to the block. Returns the new tip.
    ///
    /// Errors: `UnknownHeader`, `NoBlock`, `NotSequential`, `Consensus`. `Consensus` here is
    /// always a consensus failure: the block passed the mutation checks when it was stored.
    fn connect_block(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Tip, ChainError>> + Send;

    fn block(
        &self,
        hash: BlockHash,
    ) -> impl Future<Output = Result<Option<Block>, ChainError>> + Send;

    fn tip(&self) -> impl Future<Output = Result<Tip, ChainError>> + Send;

    /// Reverts the tip block and moves the tip to its parent. Returns the new tip.
    ///
    /// Errors: `TipIsGenesis`.
    fn disconnect_tip(&self) -> impl Future<Output = Result<Tip, ChainError>> + Send;

    fn coin(
        &self,
        outpoint: OutPoint,
    ) -> impl Future<Output = Result<Option<Coin>, ChainError>> + Send;
}
