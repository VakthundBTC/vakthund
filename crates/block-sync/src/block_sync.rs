//! One task owns the state and handles every event in turn: `net` events, `chain` answers and
//! deadlines. [`BlockSync::decide`] turns an event into the handlers it calls for;
//! [`BlockSync::handle`] runs them (`crate::handlers`).

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::block::Header;
use bitcoin::pow::Work;
use bitcoin::{Block, BlockHash, BlockHeight, Network};
use interfaces::chain::{ChainCall, ChainRequest, ChainResponse, ChainResult, Tip};
use interfaces::net::{Net, PeerId};
use interfaces::queue::{NetEvent, Queue, RoutedMessage};
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

use crate::config::BlockSyncConfig;
use crate::error::BlockSyncError;
use crate::messages::{self, PeerMessage};
use crate::queue::{self, QueueReceiver};
use crate::requests::{Answer, Answered, Request, Requests};
use crate::state::{HeadersPurpose, Ibd, IbdPhase, PeerData, Status};

pub struct BlockSync<N> {
    pub(crate) net: Arc<N>,
    /// Everything block-sync waits for, to `chain` and to peers.
    pub(crate) requests: Requests,
    events: mpsc::Receiver<NetEvent>,
    answers: mpsc::Receiver<ChainResponse>,
    pub(crate) timeout: Duration,
    /// IBD lasts while the tip has less chain work.
    #[expect(
        dead_code,
        reason = "read by the IBD flow when IBD ends, which comes next"
    )]
    pub(crate) minimum_chain_work: Work,
    /// The tip as `chain`'s answers have left it. Only block-sync changes the tip, so this is
    /// exact once its tip-changing requests are answered.
    #[expect(
        dead_code,
        reason = "read by the IBD and standard-mode flows, which come next"
    )]
    pub(crate) tip: Tip,
    pub(crate) status: Status,
    /// The connected peers.
    pub(crate) peers: HashMap<PeerId, PeerData>,
}

enum Event {
    Net(NetEvent),
    Chain(ChainResponse),
    Deadlines(Instant),
}

/// What block-sync does next, with exactly the data its handler takes.
enum Handler {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    /// The locator is ready: send the `getheaders`.
    LocatorReady {
        peer: PeerId,
        locator: Vec<BlockHash>,
        purpose: HeadersPurpose,
    },
    IbdHeaders {
        peer: PeerId,
        headers: Vec<Header>,
    },
    SearchHeaders {
        peer: PeerId,
        target: BlockHash,
        collected: Vec<Header>,
        headers: Vec<Header>,
    },
    AnnouncedHeaders {
        peer: PeerId,
        headers: Vec<Header>,
    },
    Inv {
        peer: PeerId,
        hashes: Vec<BlockHash>,
    },
    /// A requested block, or in standard mode a late or unsolicited one.
    Block {
        peer: PeerId,
        block: Block,
        requested: bool,
    },
    HeadersRequestFailed {
        peer: PeerId,
        purpose: HeadersPurpose,
    },
    BlockRequestFailed {
        peer: PeerId,
    },
    IbdRestart,
}

impl<N: Net> BlockSync<N> {
    /// `events` is the receiving end of the queue `net` pushes to; `chain` is the chain queue.
    /// Asks `chain` for the tip, which decides between IBD and standard mode.
    pub async fn new(
        config: &BlockSyncConfig,
        network: Network,
        net: Arc<N>,
        chain: Arc<dyn Queue<ChainRequest>>,
        events: QueueReceiver<NetEvent>,
    ) -> Result<Self, BlockSyncError> {
        let timeout = Duration::from_secs(config.request_timeout_secs);
        // As large as the requests in flight, so `chain` never finds it full.
        let (answers_tx, answers) = queue::channel(Requests::MAX_CHAIN_IN_FLIGHT);
        let mut answers = answers.0;
        let mut requests = Requests::new(chain, Arc::new(answers_tx), timeout);
        let tip = request_tip(&mut requests, &mut answers).await?;
        let minimum_chain_work = config.minimum_chain_work(network);
        Ok(Self {
            net,
            requests,
            events: events.0,
            answers,
            timeout,
            minimum_chain_work,
            tip,
            status: first_status(&tip, minimum_chain_work),
            peers: HashMap::new(),
        })
    }

    /// Returns only on a node failure.
    pub async fn run(mut self) -> Result<Infallible, BlockSyncError> {
        loop {
            let deadline = earliest(self.requests.next_deadline(), self.ibd_restart_at());
            let event = tokio::select! {
                // Answers first: they free room for requests and unblock the flows.
                biased;
                answer = self.answers.recv() => Event::Chain(answer.ok_or(BlockSyncError::Closed)?),
                event = self.events.recv() => Event::Net(event.ok_or(BlockSyncError::Closed)?),
                () = sleep_until(deadline) => Event::Deadlines(Instant::now()),
            };
            for handler in self.decide(event)? {
                self.handle(handler).await?;
            }
        }
    }

    /// Routing only: it may update `requests` (taking the request an answer ends), never the
    /// flows' state.
    fn decide(&mut self, event: Event) -> Result<Vec<Handler>, BlockSyncError> {
        match event {
            Event::Net(NetEvent::PeerConnected(peer)) => Ok(vec![Handler::PeerConnected(peer)]),
            Event::Net(NetEvent::PeerDisconnected(peer)) => {
                Ok(vec![Handler::PeerDisconnected(peer)])
            }
            Event::Net(NetEvent::Message(message)) => self.decide_message(message),
            Event::Chain(answer) => {
                let Some(answered) = self.requests.chain_answered(answer.id) else {
                    return Err(BlockSyncError::UnexpectedAnswer);
                };
                Ok(answer_handler(answered, Answer::Chain(answer.result))?
                    .into_iter()
                    .collect())
            }
            Event::Deadlines(now) => {
                let mut handlers: Vec<Handler> = self
                    .requests
                    .expired(now)
                    .into_iter()
                    .filter_map(timeout_handler)
                    .collect();
                if self.ibd_restart_at().is_some_and(|at| at <= now) {
                    handlers.push(Handler::IbdRestart);
                }
                Ok(handlers)
            }
        }
    }

    /// Matches the message to a request in flight (`docs/modules/block-sync/README.md`, "Request
    /// timeout"). In IBD and reorg mode the messages that answer no request are dropped.
    fn decide_message(&mut self, message: RoutedMessage) -> Result<Vec<Handler>, BlockSyncError> {
        let peer = message.peer;
        // `net` reports a peer connected before it routes the peer's messages.
        if !self.peers.contains_key(&peer) {
            return Ok(Vec::new());
        }
        let Some(decoded) = messages::decode(&message.command, &message.payload) else {
            return Ok(Vec::new());
        };
        let standard = matches!(self.status, Status::Standard);
        let mut handlers = Vec::new();
        match decoded {
            PeerMessage::Headers(headers) => {
                match self.requests.take_headers_answer(peer, &headers) {
                    Some(answered) => {
                        handlers.extend(answer_handler(answered, Answer::Headers(headers))?);
                    }
                    None if standard => handlers.push(Handler::AnnouncedHeaders { peer, headers }),
                    None => {}
                }
            }
            PeerMessage::Block(block) => {
                match self.requests.take_block_answer(block.block_hash(), peer) {
                    Some(answered) => {
                        handlers.extend(answer_handler(answered, Answer::Block(block))?)
                    }
                    None if standard => handlers.push(Handler::Block {
                        peer,
                        block,
                        requested: false,
                    }),
                    None => {}
                }
            }
            PeerMessage::NotFound(hashes) => {
                for hash in hashes {
                    if let Some(answered) = self.requests.take_block_answer(hash, peer) {
                        handlers.extend(answer_handler(answered, Answer::NotFound)?);
                    }
                }
            }
            PeerMessage::Inv(hashes) => {
                if standard {
                    handlers.push(Handler::Inv { peer, hashes });
                }
            }
        }
        Ok(handlers)
    }

    async fn handle(&mut self, handler: Handler) -> Result<(), BlockSyncError> {
        match handler {
            Handler::PeerConnected(peer) => self.on_peer_connected(peer).await,
            Handler::PeerDisconnected(peer) => self.on_peer_disconnected(peer).await,
            Handler::LocatorReady {
                peer,
                locator,
                purpose,
            } => self.send_getheaders(peer, locator, purpose).await,
            Handler::IbdHeaders { peer, headers } => {
                self.on_ibd_headers(peer, headers);
                Ok(())
            }
            Handler::SearchHeaders {
                peer,
                target,
                collected,
                headers,
            } => {
                self.on_search_headers(peer, target, collected, headers);
                Ok(())
            }
            Handler::AnnouncedHeaders { peer, headers } => {
                self.on_announced_headers(peer, headers);
                Ok(())
            }
            Handler::Inv { peer, hashes } => {
                self.on_inv(peer, hashes);
                Ok(())
            }
            Handler::Block {
                peer,
                block,
                requested,
            } => {
                self.on_block(peer, block, requested);
                Ok(())
            }
            Handler::HeadersRequestFailed { peer, purpose } => {
                self.on_headers_request_failed(peer, purpose).await
            }
            Handler::BlockRequestFailed { peer } => self.on_block_request_failed(peer).await,
            Handler::IbdRestart => self.on_ibd_restart().await,
        }
    }

    fn ibd_restart_at(&self) -> Option<Instant> {
        match &self.status {
            Status::Ibd(ibd) => ibd.restart_at,
            Status::Standard | Status::Reorg(_) => None,
        }
    }
}

/// `None`: the answer needs nothing more; a locator that is not current was replaced, or its
/// peer has gone.
fn answer_handler(answered: Answered, answer: Answer) -> Result<Option<Handler>, BlockSyncError> {
    let Answered { request, current } = answered;
    let handler = match (request, answer) {
        (Request::Locator { peer, purpose }, Answer::Chain(ChainResult::Locator(locator))) => {
            let locator = locator?;
            current.then_some(Handler::LocatorReady {
                peer,
                locator,
                purpose,
            })
        }
        (
            Request::Headers {
                peer,
                purpose: HeadersPurpose::Ibd,
                ..
            },
            Answer::Headers(headers),
        ) => Some(Handler::IbdHeaders { peer, headers }),
        (
            Request::Headers {
                peer,
                purpose: HeadersPurpose::Search { target, collected },
                ..
            },
            Answer::Headers(headers),
        ) => Some(Handler::SearchHeaders {
            peer,
            target,
            collected,
            headers,
        }),
        (Request::Block { peer, .. }, Answer::Block(block)) => Some(Handler::Block {
            peer,
            block,
            requested: true,
        }),
        (Request::Block { peer, .. }, Answer::NotFound) => {
            Some(Handler::BlockRequestFailed { peer })
        }
        _ => return Err(BlockSyncError::UnexpectedAnswer),
    };
    Ok(handler)
}

/// Only requests to peers expire (`Requests::expired`); they have failed.
fn timeout_handler(request: Request) -> Option<Handler> {
    match request {
        Request::Headers { peer, purpose, .. } => {
            Some(Handler::HeadersRequestFailed { peer, purpose })
        }
        Request::Block { peer, .. } => Some(Handler::BlockRequestFailed { peer }),
        Request::StartUpTip | Request::Locator { .. } => None,
    }
}

/// The only request in flight, so the first answer is its answer.
async fn request_tip(
    requests: &mut Requests,
    answers: &mut mpsc::Receiver<ChainResponse>,
) -> Result<Tip, BlockSyncError> {
    requests.send_to_chain(ChainCall::Tip, Request::StartUpTip)?;
    let answer = answers.recv().await.ok_or(BlockSyncError::Closed)?;
    let answered = requests.chain_answered(answer.id);
    match (answered.map(|answered| answered.request), answer.result) {
        (Some(Request::StartUpTip), ChainResult::Tip(tip)) => Ok(tip?),
        _ => Err(BlockSyncError::UnexpectedAnswer),
    }
}

/// `docs/flows/mode-ibd.md`: IBD when the tip is genesis or has less chain work than the minimum.
fn first_status(tip: &Tip, minimum_chain_work: Work) -> Status {
    if tip.height == BlockHeight::ZERO || tip.chain_work < minimum_chain_work {
        Status::Ibd(Ibd {
            sync_peer: None,
            tried: HashSet::new(),
            restart_at: None,
            phase: IbdPhase::Headers {
                received: Vec::new(),
            },
        })
    } else {
        Status::Standard
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn earliest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}
