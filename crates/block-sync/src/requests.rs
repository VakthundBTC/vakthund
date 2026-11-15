//! Everything block-sync waits for, to `chain` and to peers, in one registry. Each entry is a
//! [`Request`]: the handler of its answer and of its deadline, with the data they need. The
//! registry gives ids, keeps the deadlines, finds the request a peer's message answers, and
//! keeps one `getheaders` (or locator for it) per peer and one request per block.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::block::Header;
use bitcoin::{Block, BlockHash};
use interfaces::chain::{ChainCall, ChainRequest, ChainRequestId, ChainResponse, ChainResult};
use interfaces::net::PeerId;
use interfaces::queue::Queue;
use tokio::time::Instant;

use crate::error::BlockSyncError;
use crate::state::{HeadersPurpose, Resolution};
use crate::timers::Timers;

/// Also the [`ChainRequestId`] of a request to `chain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(u64);

/// A request and its handlers; a new variant needs an answer and a deadline handler in
/// `BlockSync`. All but `Headers` and `Block` go to `chain`.
pub enum Request {
    /// The tip, before the event loop starts.
    StartUpTip,
    /// The locator for the peer's next `getheaders`.
    Locator {
        peer: PeerId,
        purpose: HeadersPurpose,
    },
    /// To the peer: `getheaders`.
    Headers {
        peer: PeerId,
        locator: Vec<BlockHash>,
        purpose: HeadersPurpose,
    },
    /// To the peer: `getdata` for one block.
    Block {
        peer: PeerId,
        hash: BlockHash,
    },
    /// `validate_headers` of the whole IBD header chain from the sync peer.
    IbdValidateHeaders {
        peer: PeerId,
        headers: Vec<Header>,
    },
    /// `store_headers` of the validated IBD header chain.
    IbdStoreHeaders {
        hashes: Vec<BlockHash>,
    },
    /// `validate_headers` of a candidate's headers; `searched`: they come from the LCA search.
    ValidateCandidate {
        peer: PeerId,
        headers: Vec<Header>,
        searched: bool,
    },
    /// `header_at` for the walk back to the active chain.
    LcaActive {
        peer: PeerId,
        resolution: Resolution,
    },
    /// `header` of the cursor, for its parent.
    LcaParent {
        peer: PeerId,
        resolution: Resolution,
    },
    /// `header_at` for the walk forward past headers already on the active chain.
    SkipActive {
        peer: PeerId,
        resolution: Resolution,
    },
    /// `header` of the last block hash an `inv` announced.
    InvHeader {
        peer: PeerId,
        hash: BlockHash,
    },
    /// `store_headers` of a candidate's new headers.
    StoreCandidateHeaders,
    /// `header` of a block to fetch: stored blocks are not requested.
    HasBlock {
        peer: PeerId,
        hash: BlockHash,
    },
    /// `store_block` of a block received from the peer.
    StoreBlock {
        peer: PeerId,
        hash: BlockHash,
    },
    ConnectBlock {
        hash: BlockHash,
    },
    DisconnectTip,
}

/// What answered a request.
pub enum Answer {
    Chain(ChainResult),
    Headers(Vec<Header>),
    Block(Block),
    NotFound,
}

pub struct Answered {
    pub request: Request,
    /// `false` when a newer request took its place, or its peer has gone, while it was in
    /// flight. Only a request to `chain` can be answered then: `chain` always answers.
    pub current: bool,
}

/// One per peer for the next `getheaders`, one per peer for resolving its candidate, one per
/// block from its fetch to its storing.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum IndexKey {
    Headers(PeerId),
    Candidate(PeerId),
    Block(BlockHash),
}

struct Entry {
    request: Request,
    /// `None` once the deadline of a request to `chain` has passed.
    deadline: Option<Instant>,
}

pub struct Requests {
    chain: Arc<dyn Queue<ChainRequest>>,
    answers: Arc<dyn Queue<ChainResponse>>,
    timeout: Duration,
    next_id: u64,
    entries: HashMap<RequestId, Entry>,
    deadlines: Timers<RequestId>,
    /// The current request of each index key.
    request_index: HashMap<IndexKey, RequestId>,
    chain_in_flight: usize,
}

impl Request {
    fn to_chain(&self) -> bool {
        !matches!(self, Request::Headers { .. } | Request::Block { .. })
    }

    fn peer(&self) -> Option<PeerId> {
        match self {
            Request::Locator { peer, .. }
            | Request::Headers { peer, .. }
            | Request::Block { peer, .. }
            | Request::IbdValidateHeaders { peer, .. }
            | Request::ValidateCandidate { peer, .. }
            | Request::LcaActive { peer, .. }
            | Request::LcaParent { peer, .. }
            | Request::SkipActive { peer, .. }
            | Request::InvHeader { peer, .. }
            | Request::HasBlock { peer, .. }
            | Request::StoreBlock { peer, .. } => Some(*peer),
            Request::StartUpTip
            | Request::IbdStoreHeaders { .. }
            | Request::StoreCandidateHeaders
            | Request::ConnectBlock { .. }
            | Request::DisconnectTip => None,
        }
    }

    fn index_key(&self) -> Option<IndexKey> {
        match self {
            Request::Locator { peer, .. } | Request::Headers { peer, .. } => {
                Some(IndexKey::Headers(*peer))
            }
            Request::ValidateCandidate { peer, .. }
            | Request::LcaActive { peer, .. }
            | Request::LcaParent { peer, .. }
            | Request::SkipActive { peer, .. }
            | Request::InvHeader { peer, .. } => Some(IndexKey::Candidate(*peer)),
            Request::Block { hash, .. }
            | Request::HasBlock { hash, .. }
            | Request::StoreBlock { hash, .. } => Some(IndexKey::Block(*hash)),
            Request::StartUpTip
            | Request::IbdValidateHeaders { .. }
            | Request::IbdStoreHeaders { .. }
            | Request::StoreCandidateHeaders
            | Request::ConnectBlock { .. }
            | Request::DisconnectTip => None,
        }
    }

    /// The block a request is fetching, or storing.
    fn block(&self) -> Option<BlockHash> {
        match self.index_key() {
            Some(IndexKey::Block(hash)) => Some(hash),
            _ => None,
        }
    }
}

impl Requests {
    /// Requests to `chain` block-sync keeps in flight at most, so neither the chain queue nor
    /// block-sync's answer queue fills up. The chain queue must hold at least this many.
    pub const MAX_CHAIN_IN_FLIGHT: usize = 256;

    /// `answers` is block-sync's answer queue, named in every request to `chain`.
    pub fn new(
        chain: Arc<dyn Queue<ChainRequest>>,
        answers: Arc<dyn Queue<ChainResponse>>,
        timeout: Duration,
    ) -> Self {
        Self {
            chain,
            answers,
            timeout,
            next_id: 0,
            entries: HashMap::new(),
            deadlines: Timers::default(),
            request_index: HashMap::new(),
            chain_in_flight: 0,
        }
    }

    pub fn has_room(&self, requests: usize) -> bool {
        self.chain_in_flight + requests <= Self::MAX_CHAIN_IN_FLIGHT
    }

    /// `request` must be one to `chain`.
    pub fn send_to_chain(
        &mut self,
        call: ChainCall,
        request: Request,
    ) -> Result<RequestId, BlockSyncError> {
        let id = self.next_id();
        let message = ChainRequest {
            id: ChainRequestId(id.0),
            reply: Arc::clone(&self.answers),
            call,
        };
        self.chain
            .push(message)
            .map_err(|_| BlockSyncError::ChainQueueFull)?;
        self.chain_in_flight += 1;
        self.insert(id, request, true);
        Ok(id)
    }

    /// Records a request block-sync has sent to a peer. `sent: false`: it could not be sent and
    /// its deadline passes at once, so its failure takes the deadline's path.
    pub fn sent_to_peer(&mut self, request: Request, sent: bool) -> RequestId {
        let id = self.next_id();
        self.insert(id, request, sent);
        id
    }

    /// `None` for an id block-sync did not send.
    pub fn chain_answered(&mut self, id: ChainRequestId) -> Option<Answered> {
        let id = RequestId(id.0);
        let current = self.is_current(id);
        let request = self.remove(id)?;
        self.chain_in_flight = self.chain_in_flight.saturating_sub(1);
        Some(Answered { request, current })
    }

    /// The peer's `getheaders`, if `headers` answers it: empty, or the first header's parent is
    /// in its locator.
    pub fn take_headers_answer(&mut self, peer: PeerId, headers: &[Header]) -> Option<Answered> {
        let id = *self.request_index.get(&IndexKey::Headers(peer))?;
        let Some(Entry {
            request: Request::Headers { locator, .. },
            ..
        }) = self.entries.get(&id)
        else {
            return None;
        };
        // An empty `headers` is an answer too: the peer has nothing after the locator.
        let answers = headers
            .first()
            .is_none_or(|first| locator.contains(&first.prev_blockhash));
        if !answers {
            return None;
        }
        self.take_answered(id)
    }

    /// The request for the block, if it was asked from this peer: a `block` or a `notfound`
    /// answers it.
    pub fn take_block_answer(&mut self, hash: BlockHash, peer: PeerId) -> Option<Answered> {
        let id = *self.request_index.get(&IndexKey::Block(hash))?;
        match self.entries.get(&id) {
            Some(Entry {
                request: Request::Block { peer: asked, .. },
                ..
            }) if *asked == peer => self.take_answered(id),
            _ => None,
        }
    }

    /// Whether the block is being fetched or stored.
    pub fn is_block_wanted(&self, hash: &BlockHash) -> bool {
        self.request_index.contains_key(&IndexKey::Block(*hash))
    }

    /// Ends the peer's requests: its requests to peers are removed, its requests to `chain` stay
    /// until answered, no longer current. Returns the blocks no longer being fetched.
    pub fn end_peer(&mut self, peer: PeerId) -> Vec<BlockHash> {
        let ids: Vec<RequestId> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.request.peer() == Some(peer))
            .map(|(id, _)| *id)
            .collect();
        let mut blocks = Vec::new();
        for id in ids {
            let Some(entry) = self.entries.get(&id) else {
                continue;
            };
            // A block stored for a gone peer is still stored.
            if !matches!(entry.request, Request::StoreBlock { .. })
                && let Some(hash) = entry.request.block()
                && self.is_current(id)
            {
                blocks.push(hash);
            }
            if entry.request.to_chain() {
                self.unindex(id);
            } else {
                self.remove(id);
            }
        }
        blocks
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.next_deadline()
    }

    /// The requests to peers whose deadline has passed: removed and returned, as failed. A
    /// request to `chain` past its deadline stays: `chain` always answers, and a write may have
    /// happened.
    pub fn expired(&mut self, now: Instant) -> Vec<Request> {
        let mut failed = Vec::new();
        for id in self.deadlines.expired(now) {
            let Some(entry) = self.entries.get_mut(&id) else {
                continue;
            };
            if entry.request.to_chain() {
                entry.deadline = None;
            } else if let Some(request) = self.remove(id) {
                failed.push(request);
            }
        }
        failed
    }

    fn next_id(&mut self) -> RequestId {
        let id = RequestId(self.next_id);
        self.next_id += 1;
        id
    }

    /// A newer request takes the index key: a request to a peer that held it ends, one to `chain`
    /// stays until answered.
    fn insert(&mut self, id: RequestId, request: Request, wait: bool) {
        let previous = request
            .index_key()
            .and_then(|key| self.request_index.insert(key, id));
        if let Some(previous) = previous
            && self
                .entries
                .get(&previous)
                .is_some_and(|entry| !entry.request.to_chain())
        {
            self.remove(previous);
        }
        let at = if wait {
            Instant::now() + self.timeout
        } else {
            Instant::now()
        };
        self.deadlines.schedule(at, id);
        let entry = Entry {
            request,
            deadline: Some(at),
        };
        self.entries.insert(id, entry);
    }

    fn take_answered(&mut self, id: RequestId) -> Option<Answered> {
        let request = self.remove(id)?;
        Some(Answered {
            request,
            current: true,
        })
    }

    fn is_current(&self, id: RequestId) -> bool {
        match self
            .entries
            .get(&id)
            .and_then(|entry| entry.request.index_key())
        {
            Some(key) => self.request_index.get(&key) == Some(&id),
            None => true,
        }
    }

    fn unindex(&mut self, id: RequestId) {
        if let Some(key) = self
            .entries
            .get(&id)
            .and_then(|entry| entry.request.index_key())
            && self.request_index.get(&key) == Some(&id)
        {
            self.request_index.remove(&key);
        }
    }

    /// Removes the entry with its deadline and its index key.
    fn remove(&mut self, id: RequestId) -> Option<Request> {
        self.unindex(id);
        let entry = self.entries.remove(&id)?;
        if let Some(at) = entry.deadline {
            self.deadlines.cancel(at, id);
        }
        Some(entry.request)
    }
}
