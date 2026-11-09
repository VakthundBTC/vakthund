//! The owner of the peers (`docs/modules/net/README.md`, "Peers"): it starts and stops the
//! connections, applies their reports, and is the only writer of `peer-list`.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use bitcoin::Network;
use bitcoin_p2p_messages::Magic;
use bitcoin_p2p_messages::message::CommandString;
use interfaces::net::{Net, Peer, PeerFeatures, PeerId, PeerState};
use interfaces::queue::MessageQueue;
use store::{Store, TableSpec};
use tokio::sync::{Mutex, mpsc};

use crate::config::NetConfig;
use crate::connection::{self, ConnectionHandle, Event, Report};
use crate::error::Error;
use crate::peer_list::{PeerList, peer_id};

pub struct NetManager<Q> {
    magic: Magic,
    queue: Arc<Q>,
    store: Store,
    peers: Mutex<PeerRegistry>,
    reports_tx: mpsc::UnboundedSender<Report>,
    /// Taken by [`NetManager::run`] for as long as it runs.
    reports: Mutex<mpsc::UnboundedReceiver<Report>>,
}

struct PeerRegistry {
    entries: BTreeMap<PeerId, PeerEntry>,
    next_connection: u64,
    /// The peers known at [`NetManager::open`], which [`NetManager::run`] connects to.
    at_start_up: Vec<PeerId>,
}

struct PeerEntry {
    address: SocketAddr,
    features: Option<PeerFeatures>,
    state: PeerState,
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
    fn insert(&mut self, address: SocketAddr, features: Option<PeerFeatures>) -> PeerId {
        let id = peer_id(&address);
        let entry = PeerEntry {
            address,
            features,
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
            next_connection: 0,
            at_start_up: Vec::new(),
        };
        for (address, features) in store.load_all::<PeerList>()? {
            if !peers.contains(address)? {
                peers.insert(address, features);
            }
        }
        for address in &config.peers {
            if !peers.contains(*address)? {
                store.put::<PeerList>(address, &None)?;
                peers.insert(*address, None);
            }
        }
        peers.at_start_up = peers.entries.keys().copied().collect();
        let (reports_tx, reports) = mpsc::unbounded_channel();
        Ok(Self {
            magic,
            queue,
            store,
            peers: Mutex::new(peers),
            reports_tx,
            reports: Mutex::new(reports),
        })
    }

    /// Connects to the peers known at [`NetManager::open`], then applies the connections'
    /// reports. Returns only on a node failure.
    pub async fn run(&self) -> Result<Infallible, Error> {
        let mut reports = self.reports.lock().await;
        {
            let mut peers = self.peers.lock().await;
            // Since `open`, a peer may have been connected or removed through the calls.
            for id in std::mem::take(&mut peers.at_start_up) {
                if peers
                    .entries
                    .get(&id)
                    .is_some_and(|entry| entry.state == PeerState::Known)
                {
                    self.start(&mut peers, id)?;
                }
            }
        }
        while let Some(report) = reports.recv().await {
            self.apply(report).await?;
        }
        // Not reached: the manager holds a sender, so the channel never closes.
        Err(Error::Closed)
    }

    async fn apply(&self, report: Report) -> Result<(), Error> {
        let Report {
            peer: id,
            connection,
            event,
        } = report;
        let mut peers = self.peers.lock().await;
        // Reports of a stopped connection may still be in the channel, also when the peer has
        // been connected again, or removed and added again, since.
        let Some(entry) = peers.entries.get_mut(&id) else {
            return Ok(());
        };
        if entry.connection.as_ref().map(ConnectionHandle::number) != Some(connection) {
            return Ok(());
        }
        match event {
            Event::Connected(features) => {
                self.store
                    .put::<PeerList>(&entry.address, &Some(features.clone()))?;
                entry.features = Some(features);
                entry.state = PeerState::Connected;
            }
            Event::Disconnected(_) => {
                entry.state = PeerState::Known;
                entry.connection = None;
            }
            Event::QueueFull => return Err(Error::QueueFull),
        }
        Ok(())
    }

    fn start(&self, peers: &mut PeerRegistry, id: PeerId) -> Result<(), Error> {
        let number = peers.next_connection;
        peers.next_connection += 1;
        let entry = peers.get_mut(id)?;
        let handle = connection::spawn(
            id,
            number,
            entry.address,
            self.magic,
            Arc::clone(&self.queue),
            self.reports_tx.clone(),
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
        self.store.put::<PeerList>(&address, &None)?;
        Ok(peers.insert(address, None))
    }

    async fn remove_peer(&self, id: PeerId) -> Result<(), Error> {
        let mut peers = self.peers.lock().await;
        let entry = peers.get_mut(id)?;
        // Removing the entry would lose the running connection's handle.
        if entry.state != PeerState::Known {
            return Err(Error::StillConnected(id));
        }
        self.store.delete::<PeerList>(&entry.address)?;
        peers.entries.remove(&id);
        Ok(())
    }

    async fn connect(&self, id: PeerId) -> Result<(), Error> {
        let mut peers = self.peers.lock().await;
        if peers.get_mut(id)?.state == PeerState::Known {
            self.start(&mut peers, id)?;
        }
        Ok(())
    }

    async fn disconnect(&self, id: PeerId) -> Result<(), Error> {
        let mut peers = self.peers.lock().await;
        let entry = peers.get_mut(id)?;
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
}

fn peer(id: PeerId, entry: &PeerEntry) -> Peer {
    Peer {
        id,
        address: entry.address,
        features: entry.features.clone(),
        state: entry.state,
    }
}
