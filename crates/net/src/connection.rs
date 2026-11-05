//! One connection to a peer (`docs/modules/net/README.md`, "Connection flow", "Pings"). The task
//! reports what happens as [`Event`]s and never changes peer state itself.

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bitcoin::encoding::{Decode, Decoder, decode_from_slice, encode_to_vec};
use bitcoin_p2p_messages::address::Address;
use bitcoin_p2p_messages::message::{CommandString, Ping, Pong};
use bitcoin_p2p_messages::message_network::{UserAgent, VersionMessage};
use bitcoin_p2p_messages::{Magic, ProtocolVersion, ServiceFlags};
use interfaces::net::{PeerFeatures, PeerId};
use interfaces::queue::{MessageQueue, RoutedMessage};
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;

use crate::error::Error;
use crate::frame::{read_frame, write_frame};
use crate::ping::PingManager;
use crate::routing::{Command, Owner};

pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);
/// 70016, the version Bitcoin Core sends.
const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::WTXID_RELAY_VERSION;
const USER_AGENT: &str = concat!("/vakthund:", env!("CARGO_PKG_VERSION"), "/");

type Frame = (CommandString, Vec<u8>);

#[derive(Debug)]
pub enum Event {
    /// The features exchange is complete.
    Connected(PeerFeatures),
    /// The task has ended.
    Disconnected(Error),
}

pub struct ConnectionHandle {
    outgoing: mpsc::UnboundedSender<Frame>,
    task: JoinHandle<()>,
}

impl ConnectionHandle {
    /// Queues the message; it is written after the features exchange.
    pub fn send(&self, command: CommandString, payload: Vec<u8>) -> Result<(), Error> {
        self.outgoing
            .send((command, payload))
            .map_err(|_| Error::Closed)
    }

    /// Ends the connection without a `Disconnected` event.
    pub fn stop(self) {
        self.task.abort();
    }
}

pub fn spawn<Q: MessageQueue + 'static>(
    peer: PeerId,
    address: SocketAddr,
    magic: Magic,
    queue: Arc<Q>,
    events: mpsc::UnboundedSender<(PeerId, Event)>,
) -> ConnectionHandle {
    let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
    let connection = Connection {
        peer,
        magic,
        queue,
        events,
    };
    let task = tokio::spawn(async move {
        let Err(reason) = connection.run(address, outgoing_rx).await;
        connection.report(Event::Disconnected(reason));
    });
    ConnectionHandle { outgoing, task }
}

struct Connection<Q> {
    peer: PeerId,
    magic: Magic,
    queue: Arc<Q>,
    events: mpsc::UnboundedSender<(PeerId, Event)>,
}

impl<Q: MessageQueue> Connection<Q> {
    async fn run(
        &self,
        address: SocketAddr,
        mut outgoing: mpsc::UnboundedReceiver<Frame>,
    ) -> Result<Infallible, Error> {
        let mut stream = TcpStream::connect(address).await?;
        let features = time::timeout(HANDSHAKE_TIMEOUT, self.handshake(&mut stream, address))
            .await
            .map_err(|_| Error::HandshakeTimeout)??;
        self.report(Event::Connected(features));

        let (reader, mut writer) = stream.into_split();
        self.write(&mut writer, Command::SendHeaders, &[]).await?;

        // One read lives across the loop iterations: a read cut short by `select!` would lose
        // its bytes.
        let mut reading = pin!(next_frame(reader, self.magic));
        let mut pings = PingManager::new();
        loop {
            tokio::select! {
                (reader, frame) = &mut reading => {
                    reading.set(next_frame(reader, self.magic));
                    let (command, payload) = frame?;
                    self.handle_command(&mut writer, &mut pings, command, payload).await?;
                }
                Some((command, payload)) = outgoing.recv() => {
                    write_frame(&mut writer, self.magic, command, &payload).await?;
                }
                ping = pings.next_ping() => {
                    self.write(&mut writer, Command::Ping, &encode_to_vec(&ping?)).await?;
                }
            }
        }
    }

    /// Sends our `version`, answers the peer's `version` with `verack`, and waits for the peer's
    /// `version` and `verack`. Other messages are dropped until then.
    async fn handshake(
        &self,
        stream: &mut TcpStream,
        address: SocketAddr,
    ) -> Result<PeerFeatures, Error> {
        let version = encode_to_vec(&our_version(address)?);
        self.write(stream, Command::Version, &version).await?;
        let mut features = None;
        let mut verack = false;
        loop {
            let (command, payload) = read_frame(stream, self.magic).await?;
            match Command::parse(&command) {
                Some(Command::Version) => {
                    let version: VersionMessage = decode(Command::Version, &payload)?;
                    features = Some(PeerFeatures {
                        services: version.services,
                        version: version.version.into(),
                        user_agent: version.user_agent.into(),
                    });
                    self.write(stream, Command::Verack, &[]).await?;
                }
                Some(Command::Verack) => verack = true,
                _ => {}
            }
            if verack && let Some(features) = features.take() {
                return Ok(features);
            }
        }
    }

    /// Answers pings, passes pongs to the [`PingManager`] and routes block-sync messages to its queue.
    async fn handle_command(
        &self,
        writer: &mut OwnedWriteHalf,
        pings: &mut PingManager,
        command_string: CommandString,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        let Some(command) = Command::parse(&command_string) else {
            return Ok(());
        };
        match command.owner() {
            Owner::Net => match command {
                Command::Ping => {
                    let ping: Ping = decode(Command::Ping, &payload)?;
                    let pong = encode_to_vec(&Pong::from_ping(&ping));
                    self.write(writer, Command::Pong, &pong).await?;
                }
                Command::Pong => {
                    let pong: Pong = decode(Command::Pong, &payload)?;
                    pings.pong_received(&pong);
                }
                // `version` and `verack` after the features exchange; `sendheaders` from the
                // peer is not acted on, the node does not announce blocks.
                _ => {}
            },
            Owner::BlockSync => {
                let message = RoutedMessage {
                    peer: self.peer,
                    command: command_string,
                    payload,
                };
                self.queue.push(message).map_err(|_| Error::QueueFull)?;
            }
        }
        Ok(())
    }

    async fn write<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        command: Command,
        payload: &[u8],
    ) -> Result<(), Error> {
        let command = command.to_command_string()?;
        write_frame(writer, self.magic, command, payload).await
    }

    fn report(&self, event: Event) {
        // The receiver is gone only when net is shutting down.
        let _ = self.events.send((self.peer, event));
    }
}

/// Reads the next frame and gives the reader back for the read after it.
async fn next_frame(
    mut reader: OwnedReadHalf,
    magic: Magic,
) -> (OwnedReadHalf, Result<Frame, Error>) {
    let frame = read_frame(&mut reader, magic).await;
    (reader, frame)
}

/// No services, `relay = false`, `start_height = 0`: `net` does not call `chain`.
fn our_version(receiver: SocketAddr) -> Result<VersionMessage, Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since_epoch| i64::try_from(since_epoch.as_secs()).ok())
        .ok_or(Error::Clock)?;
    let unspecified = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
    Ok(VersionMessage::new(
        PROTOCOL_VERSION,
        ServiceFlags::NONE,
        now,
        Address::new(&receiver, ServiceFlags::NONE),
        Address::new(&unspecified, ServiceFlags::NONE),
        rand::random(),
        UserAgent::from_nonstandard(&USER_AGENT),
        0,
    ))
}

fn decode<T>(command: Command, payload: &[u8]) -> Result<T, Error>
where
    T: Decode,
    <T::Decoder as Decoder>::Error: std::fmt::Debug,
{
    decode_from_slice(payload).map_err(|error| Error::Payload {
        command,
        reason: format!("{error:?}"),
    })
}
