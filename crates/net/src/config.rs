use std::net::SocketAddr;

use serde::Deserialize;

/// The `[net]` table of the configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetConfig {
    /// `ip:port`, the same form as the `peer-list` key. Host names are not resolved.
    pub peers: Vec<SocketAddr>,
    pub reconnect_delay_secs: u64,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            reconnect_delay_secs: 30,
        }
    }
}
