//! Messages of the v1 transport: a 24-byte header (magic, command, payload length, checksum)
//! followed by the payload. Payloads are not decoded here.

use bitcoin::encoding::{decode_from_slice, encode_to_vec};
use bitcoin::hashes::sha256d;
use bitcoin_p2p_messages::Magic;
use bitcoin_p2p_messages::message::{CommandString, V1MessageHeader};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::Error;

const HEADER_LEN: usize = 24;

/// Bitcoin Core's `MAX_PROTOCOL_MESSAGE_LENGTH`.
const MAX_PAYLOAD_LEN: u32 = 4_000_000;

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    magic: Magic,
) -> Result<(CommandString, Vec<u8>), Error> {
    let mut header = [0; HEADER_LEN];
    reader.read_exact(&mut header).await?;
    let header: V1MessageHeader =
        decode_from_slice(&header).map_err(|error| Error::Header(format!("{error:?}")))?;
    if header.magic != magic {
        return Err(Error::Magic(header.magic));
    }
    if header.length > MAX_PAYLOAD_LEN {
        return Err(Error::TooLong(header.length as usize));
    }
    let mut payload = vec![0; header.length as usize];
    reader.read_exact(&mut payload).await?;
    if checksum(&payload) != header.checksum {
        return Err(Error::Checksum);
    }
    Ok((header.command, payload))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    magic: Magic,
    command: CommandString,
    payload: &[u8],
) -> Result<(), Error> {
    let length = u32::try_from(payload.len())
        .ok()
        .filter(|length| *length <= MAX_PAYLOAD_LEN)
        .ok_or(Error::TooLong(payload.len()))?;
    let header = V1MessageHeader {
        magic,
        command,
        length,
        checksum: checksum(payload),
    };
    let mut bytes = encode_to_vec(&header);
    bytes.extend_from_slice(payload);
    writer.write_all(&bytes).await?;
    Ok(())
}

/// The first 4 bytes of the payload's double SHA-256.
fn checksum(payload: &[u8]) -> [u8; 4] {
    let hash = sha256d::hash(payload).to_byte_array();
    [hash[0], hash[1], hash[2], hash[3]]
}
