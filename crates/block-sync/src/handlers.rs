//! What block-sync does for each `Handler` that `block_sync.rs` decides: the flows, grouped by
//! what they handle. They run one at a time on block-sync's task and own its state.

mod blocks;
mod failures;
mod ibd;
mod peers;
mod sending;
mod standard;
