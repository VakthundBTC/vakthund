//! The `peer-list` table of the `peer-list` store (`docs/architecture/storage.md`, "peer-list").

use std::net::SocketAddr;

use bitcoin::hashes::sha256;
use bitcoin_p2p_messages::ServiceFlags;
use interfaces::net::{PeerFeatures, PeerId};
use store::{DecodeError, Profile, Table};

pub struct PeerList;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRecord {
    pub keep_connected: bool,
    /// `None` until the first features exchange with the peer.
    pub features: Option<PeerFeatures>,
}

/// The first 64 bits of the SHA-256 of the address's key encoding. Changing that encoding would
/// change every id.
pub fn peer_id(address: &SocketAddr) -> PeerId {
    let hash = sha256::hash(&PeerList::encode_key(address)).to_byte_array();
    PeerId(u64::from_be_bytes([
        hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
    ]))
}

impl Table for PeerList {
    const NAME: &'static str = "peer-list";
    const PROFILE: Profile = Profile::PointLookup;
    type Key = SocketAddr;
    type Value = PeerRecord;

    /// The address as text, `ip:port`.
    fn encode_key(key: &SocketAddr) -> Vec<u8> {
        key.to_string().into_bytes()
    }

    fn decode_key(bytes: &[u8]) -> Result<SocketAddr, DecodeError> {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| DecodeError(format!("not an ip:port: {bytes:?}")))
    }

    /// `keep_connected` (1 byte, 0 or 1), then the features: nothing when none are recorded yet;
    /// otherwise services (8 bytes), protocol version (4 bytes), both little-endian, then the user
    /// agent as UTF-8.
    fn encode_value(value: &PeerRecord) -> Vec<u8> {
        let mut bytes = vec![u8::from(value.keep_connected)];
        let Some(features) = &value.features else {
            return bytes;
        };
        bytes.reserve(12 + features.user_agent.len());
        bytes.extend_from_slice(&features.services.to_u64().to_le_bytes());
        bytes.extend_from_slice(&features.version.to_le_bytes());
        bytes.extend_from_slice(features.user_agent.as_bytes());
        bytes
    }

    fn decode_value(bytes: &[u8]) -> Result<PeerRecord, DecodeError> {
        let invalid = || DecodeError(format!("invalid peer-list value: {bytes:?}"));
        let (keep_connected, bytes) = match bytes.split_first() {
            Some((0, rest)) => (false, rest),
            Some((1, rest)) => (true, rest),
            _ => return Err(invalid()),
        };
        Ok(PeerRecord {
            keep_connected,
            features: decode_features(bytes)?,
        })
    }
}

fn decode_features(bytes: &[u8]) -> Result<Option<PeerFeatures>, DecodeError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let invalid = || DecodeError(format!("invalid peer features: {bytes:?}"));
    let (services, rest) = bytes.split_first_chunk().ok_or_else(invalid)?;
    let (version, user_agent) = rest.split_first_chunk().ok_or_else(invalid)?;
    let user_agent = std::str::from_utf8(user_agent).map_err(|_| invalid())?;
    Ok(Some(PeerFeatures {
        services: ServiceFlags::from(u64::from_le_bytes(*services)),
        version: u32::from_le_bytes(*version),
        user_agent: user_agent.to_owned(),
    }))
}
