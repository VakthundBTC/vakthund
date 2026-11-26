//! The owner of the peers (`docs/modules/net/README.md`, "Peers", "Stopping"): it starts and stops
//! the connections, applies their reports, and is the only writer of `peer-list`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::Network;
use bitcoin_p2p_messages::Magic;
use bitcoin_p2p_messages::message::CommandString;
use interfaces::net::{Net, Peer, PeerId, PeerState};
use interfaces::queue::MessageQueue;
use store::{Store, TableSpec};
use tokio::sync::{Mutex, mpsc};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::NetConfig;
use crate::connection::{self, ConnectionHandle, Event, Report};
use crate::error::Error;
use crate::peer_list::{PeerList, PeerRecord, peer_id};

/// How long [`Net::stop`] waits for the connections to close.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

pub struct NetManager<Q> {
    magic: Magic,
    reconnect_delay: Duration,
    queue: Arc<Q>,
    store: Store,
    peers: Mutex<PeerRegistry>,
    reports_tx: mpsc::UnboundedSender<Report>,
    /// Taken by [`NetManager::run`] for as long as it runs.
    reports: Mutex<mpsc::UnboundedReceiver<Report>>,
    /// Every connection's token is its child.
    stopped: CancellationToken,
    tracker: TaskTracker,
}

struct PeerRegistry {
    entries: BTreeMap<PeerId, PeerEntry>,
}

struct PeerEntry {
    address: SocketAddr,
    /// As stored in `peer-list`.
    record: PeerRecord,
    state: PeerState,
    /// Kept while the task waits to reconnect, so the peer is `Known` with a connection.
    connection: Option<ConnectionHandle>,
}

impl PeerRegistry {
    /// Whether the address is known. Its id taken by another address is an error.
    fn contains(&self, address: SocketAddr) -> Result<bool, Error> {
        match self.entries.get(&peer_id(&address)) {
            None => Ok(false),
            Some(entry) if entry.address == address => Ok(true),
            Some(entry) => Err(Error::IdCollision {
                address,
                existing: entry.address,
            }),
        }
    }

    /// For an address that is not known yet.
    fn insert(&mut self, address: SocketAddr, record: PeerRecord) -> PeerId {
        let id = peer_id(&address);
        let entry = PeerEntry {
            address,
            record,
            state: PeerState::Known,
            connection: None,
        };
        self.entries.insert(id, entry);
        id
    }

    fn get_mut(&mut self, id: PeerId) -> Result<&mut PeerEntry, Error> {
        self.entries.get_mut(&id).ok_or(Error::UnknownPeer(id))
    }
}

impl<Q: MessageQueue + 'static> NetManager<Q> {
    /// Opens `peer-list` under `<dir>/net/` and adds the configured peers to it. Nothing connects
    /// until [`NetManager::run`].
    pub fn open(
        config: &NetConfig,
        network: Network,
        dir: &Path,
        queue: Arc<Q>,
    ) -> Result<Self, Error> {
        let magic = Magic::try_from(network)?;
        let path = dir.join("net").join("peer-list");
        let store = Store::open(&path, &[TableSpec::of::<PeerList>()])?;
        if config.clear_peer_list {
            for (address, _) in store.load_all::<PeerList>()? {
                store.delete::<PeerList>(&address)?;
            }
        }
        let mut peers = PeerRegistry {
            entries: BTreeMap::new(),
        };
        for (address, record) in store.load_all::<PeerList>()? {
            if !peers.contains(address)? {
                peers.insert(address, record);
            }
        }
        // For a configured peer already in `peer-list`, its stored `keep_connected` wins.
        for address in &config.peers {
            if !peers.contains(*address)? {
                let record = PeerRecord {
                    keep_connected: true,
                    features: None,
                };
                store.put::<PeerList>(address, &record)?;
                peers.insert(*address, record);
            }
        }
        let (reports_tx, reports) = mpsc::unbounded_channel();
        Ok(Self {
            magic,
            reconnect_delay: Duration::from_secs(config.reconnect_delay_secs),
            queue,
            store,
            peers: Mutex::new(peers),
            reports_tx,
            reports: Mutex::new(reports),
            stopped: CancellationToken::new(),
            tracker: TaskTracker::new(),
        })
    }

    /// Connects to every peer with `keep_connected` and no connection yet, then applies the
    /// connections' reports. Returns `Ok` after [`Net::stop`], an error on a node failure.
    pub async fn run(&self) -> Result<(), Error> {
        let mut reports = self.reports.lock().await;
        {
            let mut peers = self.peers.lock().await;
            let ids: Vec<PeerId> = peers
                .entries
                .iter()
                .filter(|(_, entry)| entry.record.keep_connected && entry.connection.is_none())
                .map(|(id, _)| *id)
                .collect();
            for id in ids {
                self.start(&mut peers, id)?;
            }
        }
        loop {
            tokio::select! {
                report = reports.recv() => {
                    // The manager holds a sender, so the channel never closes.
                    self.apply(report.ok_or(Error::Closed)?).await?;
                }
                () = self.stopped.cancelled() => return Ok(()),
            }
        }
    }

    async fn apply(&self, report: Report) -> Result<(), Error> {
        let Report { peer: id, event } = report;
        let mut peers = self.peers.lock().await;
        // Reports of a stopped connection task may still be in the channel; they are dropped while
        // the peer has no connection. If `connect` starts a new task before they are applied, they
        // count as the new task's. Accepted: only the operator disconnects, so it takes a
        // `disconnect` and a `connect` within moments of each other.
        let Some(entry) = peers.entries.get_mut(&id) else {
            return Ok(());
        };
        if entry.connection.is_none() {
            return Ok(());
        }
        match event {
            Event::Reconnecting => entry.state = PeerState::Connecting,
            Event::Connected(features) => {
                let record = PeerRecord {
                    features: Some(features),
                    ..entry.record.clone()
                };
                self.store.put::<PeerList>(&entry.address, &record)?;
                entry.record = record;
                entry.state = PeerState::Connected;
            }
            Event::Disconnected(_) => entry.state = PeerState::Known,
            Event::Queue(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// Written to `peer-list` first, so memory never holds a choice that is not stored.
    fn keep_connected(&self, entry: &mut PeerEntry, keep_connected: bool) -> Result<(), Error> {
        if entry.record.keep_connected == keep_connected {
            return Ok(());
        }
        let record = PeerRecord {
            keep_connected,
            ..entry.record.clone()
        };
        self.store.put::<PeerList>(&entry.address, &record)?;
        entry.record = record;
        Ok(())
    }

    fn start(&self, peers: &mut PeerRegistry, id: PeerId) -> Result<(), Error> {
        let entry = peers.get_mut(id)?;
        let handle = connection::spawn(
            id,
            entry.address,
            self.magic,
            self.reconnect_delay,
            Arc::clone(&self.queue),
            self.reports_tx.clone(),
            self.stopped.child_token(),
            &self.tracker,
        );
        entry.connection = Some(handle);
        entry.state = PeerState::Connecting;
        Ok(())
    }
}

impl<Q: MessageQueue + 'static> Net for NetManager<Q> {
    type Error = Error;

    async fn get_peers(&self, state: Option<PeerState>) -> Result<Vec<Peer>, Error> {
        let peers = self.peers.lock().await;
        let peers = peers
            .entries
            .iter()
            .filter(|(_, entry)| state.is_none_or(|state| entry.state == state))
            .map(|(id, entry)| peer(*id, entry))
            .collect();
        Ok(peers)
    }

    async fn get_peer(&self, id: PeerId) -> Result<Option<Peer>, Error> {
        let peers = self.peers.lock().await;
        Ok(peers.entries.get(&id).map(|entry| peer(id, entry)))
    }

    async fn add_peer(&self, address: SocketAddr) -> Result<PeerId, Error> {
        let mut peers = self.peers.lock().await;
        if peers.contains(address)? {
            return Ok(peer_id(&address));
        }
        let record = PeerRecord {
            keep_connected: false,
            features: None,
        };
        self.store.put::<PeerList>(&address, &record)?;
        Ok(peers.insert(address, record))
    }

    async fn remove_peer(&self, id: PeerId) -> Result<(), Error> {
        let mut peers = self.peers.lock().await;
        let entry = peers.get_mut(id)?;
        // Also a peer waiting to reconnect: removing the entry would lose its task's handle.
        if entry.connection.is_some() {
            return Err(Error::StillConnected(id));
        }
        self.store.delete::<PeerList>(&entry.address)?;
        peers.entries.remove(&id);
        Ok(())
    }

    async fn connect(&self, id: PeerId) -> Result<(), Error> {
        let mut peers = self.peers.lock().await;
        let entry = peers.get_mut(id)?;
        self.keep_connected(entry, true)?;
        // A peer with a connection is connecting, connected, or waiting to reconnect.
        if entry.connection.is_none() {
            self.start(&mut peers, id)?;
        }
        Ok(())
    }

    async fn disconnect(&self, id: PeerId) -> Result<(), Error> {
        let mut peers = self.peers.lock().await;
        let entry = peers.get_mut(id)?;
        self.keep_connected(entry, false)?;
        if let Some(connection) = entry.connection.take() {
            connection.stop();
        }
        entry.state = PeerState::Known;
        Ok(())
    }

    async fn send(
        &self,
        id: PeerId,
        command: CommandString,
        payload: Vec<u8>,
    ) -> Result<(), Error> {
        let mut peers = self.peers.lock().await;
        let entry = peers.get_mut(id)?;
        match (&entry.state, &entry.connection) {
            (PeerState::Connected, Some(connection)) => connection.send(command, payload),
            _ => Err(Error::NotConnected(id)),
        }
    }

    /// `docs/modules/net/README.md`, "Stopping". Waits at most `STOP_TIMEOUT`: a connection
    /// still writing to a peer that does not read is left behind.
    async fn stop(&self) -> Result<(), Error> {
        self.stopped.cancel();
        self.tracker.close();
        // Not an error: the docs let `net` stop anyway.
        let _ = time::timeout(STOP_TIMEOUT, self.tracker.wait()).await;
        Ok(())
    }
}

fn peer(id: PeerId, entry: &PeerEntry) -> Peer {
    Peer {
        id,
        address: entry.address,
        keep_connected: entry.record.keep_connected,
        features: entry.record.features.clone(),
        state: entry.state,
    }
}
