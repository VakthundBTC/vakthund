//! `block-sync` (`docs/modules/block-sync/README.md`).

mod block_sync;
mod chain_client;
mod config;
mod error;
mod queue;
mod state;
mod timers;

pub use block_sync::BlockSync;
pub use config::BlockSyncConfig;
pub use error::BlockSyncError;
pub use queue::{QueueReceiver, QueueSender, queue};
