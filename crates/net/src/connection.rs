//! One connection to a peer (`docs/modules/net/README.md`, "Connection flow", "Pings",
//! "Stopping"). The task reports what happens as [`Report`]s and never changes peer state itself.

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
use interfaces::queue::{MessageQueue, QueueError, RoutedMessage};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::error::Error;
use crate::frame::{read_frame, write_frame};
use crate::ping::PingManager;
use crate::routing::Command;

/// For the TCP connection and the features exchange together.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
/// 70016, the version Bitcoin Core sends.
const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::WTXID_RELAY_VERSION;
const USER_AGENT: &str = concat!("/vakthund:", env!("CARGO_PKG_VERSION"), "/");

type Frame = (CommandString, Vec<u8>);

#[derive(Debug)]
pub struct Report {
    pub peer: PeerId,
    pub event: Event,
}

#[derive(Debug)]
pub enum Event {
    /// The connection flow has started again after the delay; the first attempt is not reported.
    Reconnecting,
    /// The features exchange is complete.
    Connected(PeerFeatures),
    /// The connection has ended; the task reconnects after the delay.
    Disconnected(
        #[expect(dead_code, reason = "for logging, which the node does not have yet")] Error,
    ),
    /// The task has ended on the block-sync queue's error: a node failure.
    Queue(QueueError),
}

pub struct ConnectionHandle {
    outgoing: mpsc::UnboundedSender<Frame>,
    token: CancellationToken,
}

impl ConnectionHandle {
    /// Queues the message; it is written after the features exchange.
    pub fn send(&self, command: CommandString, payload: Vec<u8>) -> Result<(), Error> {
        self.outgoing
            .send((command, payload))
            .map_err(|_| Error::Closed)
    }

    /// Ends the connection and the reconnects without a `Disconnected` event; does not wait for
    /// the task to end.
    pub fn stop(self) {
        self.token.cancel();
    }
}

/// The task ends when `token` is cancelled; `tracker` lets [`interfaces::net::Net::stop`] wait for it.
#[expect(
    clippy::too_many_arguments,
    reason = "the connection's whole configuration"
)]
pub fn spawn<Q: MessageQueue + 'static>(
    peer: PeerId,
    address: SocketAddr,
    magic: Magic,
    reconnect_delay: Duration,
    queue: Arc<Q>,
    reports: mpsc::UnboundedSender<Report>,
    token: CancellationToken,
    tracker: &TaskTracker,
) -> ConnectionHandle {
    let (outgoing, mut outgoing_rx) = mpsc::unbounded_channel();
    let connection = Connection {
        peer,
        magic,
        queue,
        reports,
        token: token.clone(),
    };
    tracker.spawn(async move {
        loop {
            let error = match connection.run(address, &mut outgoing_rx).await {
                Ok(()) => return,
                Err(Error::Queue(error)) => {
                    connection.report(Event::Queue(error));
                    return;
                }
                Err(error) => error,
            };
            connection.report(Event::Disconnected(error));
            let delay = time::sleep(reconnect_delay);
            if connection.token.run_until_cancelled(delay).await.is_none() {
                return;
            }
            // Messages for the connection that ended are not sent on the next one.
            while outgoing_rx.try_recv().is_ok() {}
            connection.report(Event::Reconnecting);
        }
    });
    ConnectionHandle { outgoing, token }
}

struct Connection<Q> {
    peer: PeerId,
    magic: Magic,
    queue: Arc<Q>,
    reports: mpsc::UnboundedSender<Report>,
    token: CancellationToken,
}

impl<Q: MessageQueue> Connection<Q> {
    /// `Ok` only when the token is cancelled.
    async fn run(
        &self,
        address: SocketAddr,
        outgoing: &mut mpsc::UnboundedReceiver<Frame>,
    ) -> Result<(), Error> {
        let connecting = async {
            let mut stream = TcpStream::connect(address).await?;
            let features = self.handshake(&mut stream, address).await?;
            Ok::<_, Error>((stream, features))
        };
        let connecting = time::timeout(CONNECT_TIMEOUT, connecting);
        let Some(connected) = self.token.run_until_cancelled(connecting).await else {
            return Ok(());
        };
        let (stream, features) = connected.map_err(|_| Error::ConnectTimeout)??;
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
                () = self.token.cancelled() => {
                    // The connection is ending either way: a failed close changes nothing.
                    let _ = writer.shutdown().await;
                    return Ok(());
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

    /// Answers pings, passes pongs to the [`PingManager`] and routes block-sync messages to its
    /// queue.
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
        // Exhaustive, so a new command must be given an owner.
        match command {
            Command::Ping => {
                let ping: Ping = decode(Command::Ping, &payload)?;
                let pong = encode_to_vec(&Pong::from_ping(&ping));
                self.write(writer, Command::Pong, &pong).await?;
            }
            Command::Pong => {
                let pong: Pong = decode(Command::Pong, &payload)?;
                pings.pong_received(&pong);
            }
            // `version` and `verack` after the features exchange; `sendheaders` from the peer is
            // not acted on, the node does not announce blocks.
            Command::Version | Command::Verack | Command::SendHeaders => {}
            Command::Headers
            | Command::Block
            | Command::Inv
            | Command::NotFound
            | Command::GetHeaders
            | Command::GetData => {
                let message = RoutedMessage {
                    peer: self.peer,
                    command: command_string,
                    payload,
                };
                self.queue.push(message)?;
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
        let report = Report {
            peer: self.peer,
            event,
        };
        let _ = self.reports.send(report);
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
