//! One task owns the state and handles every event in turn: `net` events, `chain` answers and its
//! own timers.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::pow::Work;
use bitcoin::{BlockHash, BlockHeight, Network};
use interfaces::chain::{ChainCall, ChainRequest, ChainRequestId, ChainResponse, ChainResult, Tip};
use interfaces::net::{Net, PeerId};
use interfaces::queue::{NetEvent, Queue};
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

use crate::chain_client::ChainClient;
use crate::config::BlockSyncConfig;
use crate::error::BlockSyncError;
use crate::queue::{self, QueueReceiver};
use crate::state::{BlockFetch, HeadersRequest, Ibd, IbdPhase, PeerData, Pending, Status};
use crate::timers::{Timer, Timers};

pub struct BlockSync<N> {
    #[expect(
        dead_code,
        reason = "called by the IBD and standard-mode flows, which come next"
    )]
    net: Arc<N>,
    chain: ChainClient,
    events: mpsc::Receiver<NetEvent>,
    answers: mpsc::Receiver<ChainResponse>,
    #[expect(
        dead_code,
        reason = "used by the IBD and standard-mode flows, which come next"
    )]
    timeout: Duration,
    /// IBD lasts while the tip has less chain work.
    #[expect(
        dead_code,
        reason = "read by the IBD flow when IBD ends, which comes next"
    )]
    minimum_chain_work: Work,
    /// The tip as `chain`'s answers have left it. Only block-sync changes the tip, so this is
    /// exact once its tip-changing requests are answered.
    #[expect(
        dead_code,
        reason = "read by the IBD and standard-mode flows, which come next"
    )]
    tip: Tip,
    #[expect(
        dead_code,
        reason = "read by the IBD and standard-mode flows, which come next"
    )]
    status: Status,
    /// The connected peers.
    peers: HashMap<PeerId, PeerData>,
    /// The requests for blocks sent to peers, by block hash: from the `getdata` until `chain` has
    /// stored the block or the request has failed. One per block, shared by every flow that needs
    /// it, so a block is requested once. Holds no block data: an arrived block is handed to
    /// `store_block` at once.
    blocks: HashMap<BlockHash, BlockFetch>,
    /// The requests `chain` has not answered yet.
    pending: HashMap<ChainRequestId, Pending>,
    timers: Timers,
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
        // As large as the requests in flight, so `chain` never finds it full.
        let (answers_tx, answers) = queue::channel(ChainClient::MAX_IN_FLIGHT);
        let mut answers = answers.0;
        let mut chain = ChainClient::new(chain, Arc::new(answers_tx));
        let tip = request_tip(&mut chain, &mut answers).await?;
        let minimum_chain_work = config.minimum_chain_work(network);
        Ok(Self {
            net,
            chain,
            events: events.0,
            answers,
            timeout: Duration::from_secs(config.request_timeout_secs),
            minimum_chain_work,
            tip,
            status: first_status(&tip, minimum_chain_work),
            peers: HashMap::new(),
            blocks: HashMap::new(),
            pending: HashMap::new(),
            timers: Timers::default(),
        })
    }

    /// Returns only on a node failure.
    pub async fn run(mut self) -> Result<Infallible, BlockSyncError> {
        loop {
            let deadline = self.timers.next_deadline();
            tokio::select! {
                // Answers first: they free room for requests and unblock the flows.
                biased;
                answer = self.answers.recv() => {
                    self.on_chain_answer(answer.ok_or(BlockSyncError::Closed)?)?;
                }
                event = self.events.recv() => {
                    self.on_net_event(event.ok_or(BlockSyncError::Closed)?)?;
                }
                () = sleep_until(deadline) => self.on_timers(Instant::now())?,
            }
        }
    }

    fn on_net_event(&mut self, event: NetEvent) -> Result<(), BlockSyncError> {
        match event {
            NetEvent::PeerConnected(peer) => {
                self.peers.insert(peer, PeerData::default());
            }
            NetEvent::PeerDisconnected(peer) => {
                self.end_headers_request(peer);
                self.peers.remove(&peer);
            }
            // Handled by the IBD and standard-mode flows, which come next.
            NetEvent::Message(_) => {}
        }
        Ok(())
    }

    fn on_chain_answer(&mut self, answer: ChainResponse) -> Result<(), BlockSyncError> {
        self.chain.answered();
        // An answer to a request whose flow has ended since.
        let Some(pending) = self.pending.remove(&answer.id) else {
            return Ok(());
        };
        match pending {}
    }

    fn on_timers(&mut self, now: Instant) -> Result<(), BlockSyncError> {
        for (_key, timer) in self.timers.expired(now) {
            match timer {
                // Handled by the IBD and standard-mode flows, which come next.
                Timer::Headers { .. } | Timer::Block { .. } => {}
            }
        }
        Ok(())
    }

    /// Every end of a `getheaders` request goes through here, so its timer is cancelled.
    fn end_headers_request(&mut self, peer: PeerId) -> Option<HeadersRequest> {
        let request = self.peers.get_mut(&peer)?.headers.take()?;
        self.timers.cancel(request.timer);
        Some(request)
    }

    /// Every end of a block request goes through here, so its timer is cancelled.
    #[expect(
        dead_code,
        reason = "used by the IBD and standard-mode flows, which come next"
    )]
    fn end_block_request(&mut self, hash: BlockHash) -> Option<BlockFetch> {
        let fetch = self.blocks.remove(&hash)?;
        if let BlockFetch::Requested { timer, .. } = fetch {
            self.timers.cancel(timer);
        }
        Some(fetch)
    }
}

/// The only request in flight, so the first answer is its answer.
async fn request_tip(
    chain: &mut ChainClient,
    answers: &mut mpsc::Receiver<ChainResponse>,
) -> Result<Tip, BlockSyncError> {
    let id = chain.send(ChainCall::Tip)?;
    let answer = answers.recv().await.ok_or(BlockSyncError::Closed)?;
    chain.answered();
    match answer.result {
        ChainResult::Tip(tip) if answer.id == id => Ok(tip?),
        _ => Err(BlockSyncError::UnexpectedAnswer),
    }
}

/// `docs/flows/mode-ibd.md`: IBD when the tip is genesis or has less chain work than the minimum.
fn first_status(tip: &Tip, minimum_chain_work: Work) -> Status {
    if tip.height == BlockHeight::ZERO || tip.chain_work < minimum_chain_work {
        Status::Ibd(Ibd {
            sync_peer: None,
            tried: HashSet::new(),
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
