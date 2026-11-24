use bitcoin::BlockHash;
use bitcoin_p2p_messages::message::CommandStringError;
use interfaces::chain::{ChainError, RejectReason};

/// Every error stops the node.
#[derive(Debug, thiserror::Error)]
pub enum BlockSyncError {
    #[error("chain: {0}")]
    Chain(#[from] ChainError),
    /// A block whose header is valid failed the consensus checks.
    #[error("consensus failure in block {hash}: {reason}")]
    Consensus {
        hash: BlockHash,
        reason: RejectReason,
    },
    #[error("command: {0}")]
    Command(#[from] CommandStringError),
    #[error("the chain queue is full")]
    ChainQueueFull,
    /// The answer does not match its request's call.
    #[error("unexpected answer from chain")]
    UnexpectedAnswer,
    /// Internal events kept causing each other: a bug.
    #[error("too many internal events for one event")]
    InternalLoop,
    #[error("a queue of block-sync has closed")]
    Closed,
}
