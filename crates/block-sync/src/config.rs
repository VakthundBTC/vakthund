use bitcoin::pow::Work;
use bitcoin::{Network, TestnetVersion};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

/// The `[block-sync]` table of the configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlockSyncConfig {
    /// Deadline of a `getheaders` or `getdata` request.
    pub request_timeout_secs: u64,
    /// `None`: the network's default, see [`BlockSyncConfig::minimum_chain_work`].
    #[serde(deserialize_with = "deserialize_work")]
    pub minimum_chain_work: Option<Work>,
}

impl Default for BlockSyncConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: 60,
            minimum_chain_work: None,
        }
    }
}

impl BlockSyncConfig {
    /// The configured value, or Bitcoin Core's `nMinimumChainWork` for the network (v32.1,
    /// `src/kernel/chainparams.cpp`). Every value fits in 128 bits.
    pub fn minimum_chain_work(&self, network: Network) -> Work {
        self.minimum_chain_work.unwrap_or_else(|| {
            let work: u128 = match network {
                Network::Bitcoin => 0x1_45ec_036a_cc5b_a740_052a_f1a0,
                Network::Testnet(TestnetVersion::V3) => 0x17f4_9f70_2147_f10c_0eb6,
                Network::Testnet(TestnetVersion::V4) => 0x0e34_6a55_8455_ade8_eca9,
                Network::Signet => 0x1090_e9dc_1520,
                // A testnet version added later has no known value.
                Network::Testnet(_) | Network::Regtest => 0,
            };
            work_from_u128(work)
        })
    }
}

fn work_from_u128(work: u128) -> Work {
    let mut bytes = [0; 32];
    bytes[16..].copy_from_slice(&work.to_be_bytes());
    Work::from_be_bytes(bytes)
}

/// A `0x`-prefixed hex string of up to 32 bytes.
fn deserialize_work<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Work>, D::Error> {
    let hex = String::deserialize(deserializer)?;
    Work::from_hex(&hex).map(Some).map_err(D::Error::custom)
}
