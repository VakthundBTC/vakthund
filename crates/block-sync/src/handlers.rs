//! What block-sync does for each `Handler` that `block_sync.rs` decides: the flows, grouped by
//! what they handle. They run one at a time on block-sync's task and own its state.

mod blocks;
mod failures;
mod ibd;
mod peers;
mod reorg;
mod sending;
mod standard;

/// Headers in a full `headers` answer.
const MAX_HEADERS: usize = 2000;
/// Blocks fetched at a time from one peer. The docs leave the batch size TBD; this is Core's
/// `MAX_BLOCKS_IN_TRANSIT_PER_PEER`.
const BLOCKS_IN_FLIGHT: usize = 16;
