//! Every request block-sync is waiting for, with the token of the machine that sent it
//! (`docs/modules/block-sync/machines.md`, "Events and routing"). Requests to peers and wakes
//! have deadlines; a request to `chain` has none: `chain` always answers.

use std::collections::HashMap;
use std::time::Duration;

use bitcoin::BlockHash;
use bitcoin::block::Header;
use interfaces::chain::ChainRequestId;
use interfaces::net::PeerId;
use tokio::time::Instant;

use crate::timers::Timers;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(u64);

/// A request whose deadline has passed.
pub enum Expired<T> {
    /// A request to a peer: it has failed.
    Failed(T),
    Wake(T),
}

enum Kind {
    Chain,
    Headers {
        peer: PeerId,
        locator: Vec<BlockHash>,
    },
    Block {
        peer: PeerId,
        hash: BlockHash,
    },
    Wake,
}

struct Entry<T> {
    token: T,
    kind: Kind,
    deadline: Option<Instant>,
}

pub struct Registry<T> {
    timeout: Duration,
    next_id: u64,
    entries: HashMap<RequestId, Entry<T>>,
    deadlines: Timers<RequestId>,
    /// One `getheaders` per peer: a newer one replaces it, so a `headers` answers at most one.
    headers: HashMap<PeerId, RequestId>,
    /// One `getdata` per block.
    blocks: HashMap<BlockHash, RequestId>,
    chain_in_flight: usize,
}

impl<T> Registry<T> {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            next_id: 0,
            entries: HashMap::new(),
            deadlines: Timers::default(),
            headers: HashMap::new(),
            blocks: HashMap::new(),
            chain_in_flight: 0,
        }
    }

    pub fn chain_in_flight(&self) -> usize {
        self.chain_in_flight
    }

    /// The id to send the request to `chain` with.
    pub fn chain(&mut self, token: T) -> ChainRequestId {
        self.chain_in_flight += 1;
        let id = self.insert(token, Kind::Chain, None);
        ChainRequestId(id.0)
    }

    /// `sent: false`: the request could not be sent; its deadline passes at once, so it fails
    /// on the deadline's path.
    pub fn headers(&mut self, peer: PeerId, locator: Vec<BlockHash>, token: T, sent: bool) {
        if let Some(previous) = self.headers.remove(&peer) {
            self.remove(previous);
        }
        let at = self.deadline(sent);
        let id = self.insert(token, Kind::Headers { peer, locator }, Some(at));
        self.headers.insert(peer, id);
    }

    /// As [`Registry::headers`].
    pub fn block(&mut self, peer: PeerId, hash: BlockHash, token: T, sent: bool) {
        if let Some(previous) = self.blocks.remove(&hash) {
            self.remove(previous);
        }
        let at = self.deadline(sent);
        let id = self.insert(token, Kind::Block { peer, hash }, Some(at));
        self.blocks.insert(hash, id);
    }

    pub fn wake(&mut self, at: Instant, token: T) {
        self.insert(token, Kind::Wake, Some(at));
    }

    /// `None` for an id block-sync did not send.
    pub fn take_chain(&mut self, id: ChainRequestId) -> Option<T> {
        let id = RequestId(id.0);
        if !matches!(self.entries.get(&id)?.kind, Kind::Chain) {
            return None;
        }
        self.chain_in_flight = self.chain_in_flight.saturating_sub(1);
        self.remove(id)
    }

    /// The peer's `getheaders`, if `headers` answers it: empty, or the first header's parent is
    /// in its locator.
    pub fn take_headers(&mut self, peer: PeerId, headers: &[Header]) -> Option<T> {
        let id = *self.headers.get(&peer)?;
        let Kind::Headers { locator, .. } = &self.entries.get(&id)?.kind else {
            return None;
        };
        // An empty `headers` is an answer too: the peer has nothing after the locator.
        let answers = headers
            .first()
            .is_none_or(|first| locator.contains(&first.prev_blockhash));
        if !answers {
            return None;
        }
        self.remove(id)
    }

    /// The `getdata` for the block, if it was sent to this peer: a `block` or a `notfound`
    /// answers it.
    pub fn take_block(&mut self, peer: PeerId, hash: BlockHash) -> Option<T> {
        let id = *self.blocks.get(&hash)?;
        match &self.entries.get(&id)?.kind {
            Kind::Block { peer: asked, .. } if *asked == peer => self.remove(id),
            _ => None,
        }
    }

    /// The peer's requests end without an answer; its machines are told by the peer event.
    pub fn end_peer(&mut self, peer: PeerId) {
        let ids: Vec<RequestId> = self
            .entries
            .iter()
            .filter(|(_, entry)| match &entry.kind {
                Kind::Headers { peer: asked, .. } | Kind::Block { peer: asked, .. } => {
                    *asked == peer
                }
                Kind::Chain | Kind::Wake => false,
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.remove(id);
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.next_deadline()
    }

    pub fn expired(&mut self, now: Instant) -> Vec<Expired<T>> {
        let mut expired = Vec::new();
        for id in self.deadlines.expired(now) {
            let wake = matches!(
                self.entries.get(&id).map(|entry| &entry.kind),
                Some(Kind::Wake)
            );
            if let Some(token) = self.remove(id) {
                expired.push(if wake {
                    Expired::Wake(token)
                } else {
                    Expired::Failed(token)
                });
            }
        }
        expired
    }

    fn deadline(&self, sent: bool) -> Instant {
        if sent {
            Instant::now() + self.timeout
        } else {
            Instant::now()
        }
    }

    fn insert(&mut self, token: T, kind: Kind, deadline: Option<Instant>) -> RequestId {
        let id = RequestId(self.next_id);
        self.next_id += 1;
        if let Some(at) = deadline {
            self.deadlines.schedule(at, id);
        }
        self.entries.insert(
            id,
            Entry {
                token,
                kind,
                deadline,
            },
        );
        id
    }

    /// Removes the entry with its deadline and its index.
    fn remove(&mut self, id: RequestId) -> Option<T> {
        let entry = self.entries.remove(&id)?;
        if let Some(at) = entry.deadline {
            self.deadlines.cancel(at, id);
        }
        match entry.kind {
            Kind::Headers { peer, .. } if self.headers.get(&peer) == Some(&id) => {
                self.headers.remove(&peer);
            }
            Kind::Block { hash, .. } if self.blocks.get(&hash) == Some(&id) => {
                self.blocks.remove(&hash);
            }
            _ => {}
        }
        Some(entry.token)
    }
}
