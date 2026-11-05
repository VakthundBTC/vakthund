//! The configuration file (`docs/architecture/overview.md`, "Configuration"): top-level keys for
//! every module and one table per module, owned by that module's crate.

use std::fs;
use std::path::{Path, PathBuf};

use bitcoin::{Network, TestnetVersion};
use block_sync::BlockSyncConfig;
use chain::ChainConfig;
use net::NetConfig;
use serde::de::{Error as _, Unexpected};
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_network", deserialize_with = "deserialize_network")]
    pub network: Network,
    /// Required and absolute. Stores live under `<datadir>/<network>/`.
    pub datadir: PathBuf,
    #[serde(default)]
    pub net: NetConfig,
    #[serde(default)]
    pub chain: ChainConfig,
    #[serde(default, rename = "block-sync")]
    pub block_sync: BlockSyncConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {}: {source}", path.display())]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid configuration: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("datadir must be an absolute path: {}", .0.display())]
    RelativeDatadir(PathBuf),
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let config: Self = toml::from_str(&text)?;
        if !config.datadir.is_absolute() {
            return Err(ConfigError::RelativeDatadir(config.datadir));
        }
        Ok(config)
    }
}

fn default_network() -> Network {
    Network::Bitcoin
}

/// Bitcoin Core `-chain` names, without testnet3 (`test`).
fn deserialize_network<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Network, D::Error> {
    let name = String::deserialize(deserializer)?;
    match Network::from_core_arg(&name) {
        Ok(Network::Testnet(TestnetVersion::V3)) | Err(_) => Err(D::Error::invalid_value(
            Unexpected::Str(&name),
            &"main, testnet4, signet or regtest",
        )),
        Ok(network) => Ok(network),
    }
}
