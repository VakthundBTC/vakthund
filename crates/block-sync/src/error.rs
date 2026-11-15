use interfaces::chain::ChainError;

/// Every error stops the node.
#[derive(Debug, thiserror::Error)]
pub enum BlockSyncError {
    #[error("chain: {0}")]
    Chain(#[from] ChainError),
    #[error("the chain queue is full")]
    ChainQueueFull,
    /// The answer does not match its request's call.
    #[error("unexpected answer from chain")]
    UnexpectedAnswer,
    #[error("a queue of block-sync has closed")]
    Closed,
}
