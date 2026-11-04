//! Interfaces between the Vakthund modules (`docs/architecture/overview.md`).
//!
//! A module depends on this crate, never on another module's crate, so it reaches other modules
//! only through these interfaces. Every argument, result and queue item is owned data.

pub mod chain;
pub mod net;
pub mod queue;
