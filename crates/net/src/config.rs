use std::net::SocketAddr;

use serde::Deserialize;

/// The `[net]` table of the configuration file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetConfig {
    /// `ip:port`, the same form as the `peer-list` key. Host names are not resolved.
    pub peers: Vec<SocketAddr>,
    /// Empties `peer-list` at start-up, so only the configured peers remain.
    pub clear_peer_list: bool,
}
