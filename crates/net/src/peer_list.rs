//! The `peer-list` table of the `peer-list` store (`docs/architecture/storage.md`, "peer-list").

use std::net::SocketAddr;

use interfaces::net::PeerFeatures;
use store::{DecodeError, Profile, Table};

#[expect(
    dead_code,
    reason = "used by the net service, which is not written yet"
)]
pub struct PeerList;

impl Table for PeerList {
    const NAME: &'static str = "peer-list";
    const PROFILE: Profile = Profile::PointLookup;
    type Key = SocketAddr;
    type Value = PeerFeatures;

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

    /// TBD-02: features have no fields yet, so the value is empty.
    fn encode_value(_: &PeerFeatures) -> Vec<u8> {
        Vec::new()
    }

    fn decode_value(_: &[u8]) -> Result<PeerFeatures, DecodeError> {
        Ok(PeerFeatures::default())
    }
}
