use serde::Deserialize;

/// The `[chain]` table of the configuration file. Each value is the RocksDB block cache of a store.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainConfig {
    pub chainstate_cache_mib: u64,
    pub headers_cache_mib: u64,
    pub blocks_cache_mib: u64,
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self {
            chainstate_cache_mib: 512,
            headers_cache_mib: 128,
            blocks_cache_mib: 128,
        }
    }
}
