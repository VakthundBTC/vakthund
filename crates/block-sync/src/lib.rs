//! `block-sync` (`docs/modules/block-sync/README.md`).

mod block_sync;
mod book;
mod config;
mod error;
mod machines;
mod messages;
mod queue;
mod registry;
mod timers;

pub use block_sync::BlockSync;
pub use config::BlockSyncConfig;
pub use error::BlockSyncError;
pub use queue::{QueueReceiver, QueueSender, queue};
