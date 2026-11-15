//! The P2P messages block-sync reads and sends (`docs/modules/block-sync/README.md`,
//! "Communication"). `net` passes their payloads undecoded.

use bitcoin::block::Header;
use bitcoin::encoding::{Decode, decode_from_slice, encode_to_vec};
use bitcoin::{Block, BlockHash};
use bitcoin_p2p_messages::ProtocolVersion;
use bitcoin_p2p_messages::message::{CommandString, HeadersMessage, InventoryPayload};
use bitcoin_p2p_messages::message_blockdata::{GetHeadersMessage, Inventory};

use crate::error::BlockSyncError;

/// The version `net` sends; Core ignores it in `getheaders`.
const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::WTXID_RELAY_VERSION;

pub enum PeerMessage {
    Headers(Vec<Header>),
    Block(Block),
    /// The block hashes it names.
    NotFound(Vec<BlockHash>),
    /// The `MSG_BLOCK` entries.
    Inv(Vec<BlockHash>),
}

/// `None` for a command block-sync does not read and for a payload that cannot be decoded: the
/// docs do not say what an invalid payload of block-sync's commands means, so it is dropped.
pub fn decode(command: &CommandString, payload: &[u8]) -> Option<PeerMessage> {
    match command.as_ref() {
        "headers" => {
            let message: HeadersMessage = decode_payload(payload)?;
            Some(PeerMessage::Headers(message.into_headers().collect()))
        }
        "block" => decode_payload(payload).map(PeerMessage::Block),
        "notfound" => decode_payload(payload).map(|inv| PeerMessage::NotFound(block_hashes(inv))),
        "inv" => decode_payload(payload).map(|inv| PeerMessage::Inv(block_hashes(inv))),
        _ => None,
    }
}

pub fn getheaders(
    locator: &[BlockHash],
    stop: BlockHash,
) -> Result<(CommandString, Vec<u8>), BlockSyncError> {
    let message = GetHeadersMessage {
        version: PROTOCOL_VERSION,
        locator_hashes: locator.to_vec().into(),
        stop_hash: stop,
    };
    Ok((
        CommandString::try_from("getheaders")?,
        encode_to_vec(&message),
    ))
}

fn decode_payload<T: Decode>(payload: &[u8]) -> Option<T> {
    decode_from_slice(payload).ok()
}

fn block_hashes(inventory: InventoryPayload) -> Vec<BlockHash> {
    inventory
        .0
        .into_iter()
        .filter_map(|item| match item {
            Inventory::Block(hash) | Inventory::WitnessBlock(hash) => Some(hash),
            _ => None,
        })
        .collect()
}
