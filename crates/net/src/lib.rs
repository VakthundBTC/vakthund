//! `net` (`docs/modules/net/README.md`).

mod config;
#[expect(
    dead_code,
    reason = "used by the net service, which is not written yet"
)]
mod connection;
mod error;
mod frame;
mod peer_list;
mod ping;
mod routing;

pub use config::NetConfig;
