use serde::Deserialize;

/// The `[block-sync]` table of the configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlockSyncConfig {
    /// Deadline of a `getheaders` or `getdata` request.
    pub request_timeout_secs: u64,
}

impl Default for BlockSyncConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: 60,
        }
    }
}
