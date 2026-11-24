//! The shell around the machines (`docs/modules/block-sync/README.md`, "Internal structure"):
//! it reads events, routes them to the machines, and carries out their effects. One event, and
//! every internal event it causes, is handled before the next is read.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bitcoin::block::Header;
use bitcoin::pow::Work;
use bitcoin::{Block, BlockHeight, Network};
use interfaces::chain::{ChainCall, ChainRequest, ChainResponse, ChainResult, Tip};
use interfaces::net::{Net, PeerId};
use interfaces::queue::{NetEvent, Queue, RoutedMessage};
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

use crate::config::BlockSyncConfig;
use crate::error::BlockSyncError;
use crate::machines::fetcher::{self, BlockFetcher};
use crate::machines::ibd::{self, IbdMachine};
use crate::machines::peer::{self, PeerMachine};
use crate::machines::reorg::{self, ReorgMachine};
use crate::machines::{Effect, Input, Internal, Machine, ModeChange, Owner};
use crate::messages::{self, PeerMessage};
use crate::queue::{self, QueueReceiver};
use crate::registry::{Expired, Registry};

/// Requests to `chain` the machines that pipeline them keep in flight at most. The chain queue
/// must hold at least this many, and a few per peer more.
const MAX_CHAIN_IN_FLIGHT: usize = 256;
/// block-sync's answer queue: the pipelined requests and the peer machines' few.
const ANSWER_QUEUE: usize = 4 * MAX_CHAIN_IN_FLIGHT;
/// Internal events one event may cause; more is a bug.
const MAX_DELIVERIES: usize = 1_000_000;

pub struct BlockSync<N> {
    net: Arc<N>,
    chain: Arc<dyn Queue<ChainRequest>>,
    /// Named in every request to `chain`.
    answer_queue: Arc<dyn Queue<ChainResponse>>,
    events: mpsc::Receiver<NetEvent>,
    answers: mpsc::Receiver<ChainResponse>,
    timeout: Duration,
    minimum_chain_work: Work,
    /// The tip as `chain`'s answers have left it. Only block-sync changes the tip.
    tip: Tip,
    mode: Mode,
    /// One per connected peer, in every mode; Idle in IBD, where nothing is routed to them.
    peers: HashMap<PeerId, PeerMachine>,
    fetcher: BlockFetcher,
    registry: Registry<Token>,
}

enum Mode {
    Ibd(IbdMachine),
    Standard,
    Reorg(ReorgMachine),
}

/// A request's owner and the owner's token.
enum Token {
    StartUpTip,
    Ibd(ibd::Token),
    Peer(PeerId, peer::Token),
    Reorg(reorg::Token),
    Fetcher(fetcher::Token),
}

/// An input on its way to a machine.
enum Delivery {
    Ibd(Input<ibd::Token>),
    Peer(PeerId, Input<peer::Token>),
    Reorg(Input<reorg::Token>),
    Fetcher(Input<fetcher::Token>),
}

/// What answered a request, before it becomes the owner's input.
enum Answer {
    Headers(Vec<Header>),
    Block(Block),
    Chain(ChainResult),
    Failed,
    Wake,
}

enum Event {
    Net(NetEvent),
    Chain(ChainResponse),
    Deadlines(Instant),
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
        let (answers_tx, answers) = queue::channel(ANSWER_QUEUE);
        let answer_queue: Arc<dyn Queue<ChainResponse>> = Arc::new(answers_tx);
        let mut answers = answers.0;
        let mut registry = Registry::new(timeout);
        let id = registry.chain(Token::StartUpTip);
        let request = ChainRequest {
            id,
            reply: Arc::clone(&answer_queue),
            call: ChainCall::Tip,
        };
        chain
            .push(request)
            .map_err(|_| BlockSyncError::ChainQueueFull)?;
        // The only request in flight, so the first answer is its answer.
        let answer = answers.recv().await.ok_or(BlockSyncError::Closed)?;
        let tip = match (registry.take_chain(answer.id), answer.result) {
            (Some(Token::StartUpTip), ChainResult::Tip(tip)) => tip?,
            _ => return Err(BlockSyncError::UnexpectedAnswer),
        };
        let minimum_chain_work = config.minimum_chain_work(network);
        // `docs/flows/mode-ibd.md`: IBD when the tip is genesis or below the minimum chain work.
        let mode = if tip.height == BlockHeight::ZERO || tip.chain_work < minimum_chain_work {
            Mode::Ibd(IbdMachine::default())
        } else {
            Mode::Standard
        };
        Ok(Self {
            net,
            chain,
            answer_queue,
            events: events.0,
            answers,
            timeout,
            minimum_chain_work,
            tip,
            mode,
            peers: HashMap::new(),
            fetcher: BlockFetcher::default(),
            registry,
        })
    }

    /// Returns only on a node failure.
    pub async fn run(mut self) -> Result<Infallible, BlockSyncError> {
        loop {
            let deadline = self.registry.next_deadline();
            let event = tokio::select! {
                // Answers first: they free room for requests.
                biased;
                answer = self.answers.recv() => Event::Chain(answer.ok_or(BlockSyncError::Closed)?),
                event = self.events.recv() => Event::Net(event.ok_or(BlockSyncError::Closed)?),
                () = sleep_until(deadline) => Event::Deadlines(Instant::now()),
            };
            self.process(event).await?;
        }
    }

    /// The event, the internal events it causes, then [`Input::Advance`] for the machines that
    /// send within the room for requests to `chain`.
    async fn process(&mut self, event: Event) -> Result<(), BlockSyncError> {
        let mut queue = VecDeque::new();
        self.route(event, &mut queue)?;
        self.drain(&mut queue).await?;
        match self.mode {
            Mode::Ibd(_) => queue.push_back(Delivery::Ibd(Input::Advance)),
            Mode::Reorg(_) => queue.push_back(Delivery::Reorg(Input::Advance)),
            Mode::Standard => {}
        }
        queue.push_back(Delivery::Fetcher(Input::Advance));
        self.drain(&mut queue).await
    }

    /// `docs/modules/block-sync/machines.md`, "Events and routing".
    fn route(
        &mut self,
        event: Event,
        queue: &mut VecDeque<Delivery>,
    ) -> Result<(), BlockSyncError> {
        let ibd = matches!(self.mode, Mode::Ibd(_));
        match event {
            Event::Net(NetEvent::PeerConnected(peer)) => {
                self.peers.insert(peer, PeerMachine::new(peer));
                if ibd {
                    queue.push_back(Delivery::Ibd(Input::PeerConnected));
                }
            }
            Event::Net(NetEvent::PeerDisconnected(peer)) => {
                self.registry.end_peer(peer);
                self.peers.remove(&peer);
                queue.push_back(Delivery::Fetcher(Input::PeerDisconnected(peer)));
                if ibd {
                    queue.push_back(Delivery::Ibd(Input::PeerDisconnected(peer)));
                }
            }
            Event::Net(NetEvent::Message(message)) => self.route_message(message, queue),
            Event::Chain(answer) => {
                let token = self
                    .registry
                    .take_chain(answer.id)
                    .ok_or(BlockSyncError::UnexpectedAnswer)?;
                // The shell keeps the tip from every answer that moves it.
                match &answer.result {
                    ChainResult::ConnectBlock(Ok(tip)) | ChainResult::DisconnectTip(Ok(tip)) => {
                        self.tip = *tip;
                    }
                    _ => {}
                }
                queue.extend(answer_delivery(token, Answer::Chain(answer.result))?);
            }
            Event::Deadlines(now) => {
                for expired in self.registry.expired(now) {
                    let (token, answer) = match expired {
                        Expired::Failed(token) => (token, Answer::Failed),
                        Expired::Wake(token) => (token, Answer::Wake),
                    };
                    queue.extend(answer_delivery(token, answer)?);
                }
            }
        }
        Ok(())
    }

    fn route_message(&mut self, message: RoutedMessage, queue: &mut VecDeque<Delivery>) {
        let peer = message.peer;
        // `net` reports a peer connected before it routes the peer's messages.
        if !self.peers.contains_key(&peer) {
            return;
        }
        let Some(decoded) = messages::decode(&message.command, &message.payload) else {
            return;
        };
        let ibd = matches!(self.mode, Mode::Ibd(_));
        match decoded {
            PeerMessage::Headers(headers) => {
                if let Some(token) = self.registry.take_headers(peer, &headers) {
                    queue.extend(
                        answer_delivery(token, Answer::Headers(headers))
                            .ok()
                            .flatten(),
                    );
                } else if !ibd {
                    queue.push_back(Delivery::Peer(peer, Input::Announced { headers }));
                }
            }
            PeerMessage::Block(block) => {
                if let Some(token) = self.registry.take_block(peer, block.block_hash()) {
                    queue.extend(answer_delivery(token, Answer::Block(block)).ok().flatten());
                } else if !ibd {
                    queue.push_back(Delivery::Fetcher(Input::Unsolicited { peer, block }));
                }
            }
            PeerMessage::NotFound(hashes) => {
                for hash in hashes {
                    if let Some(token) = self.registry.take_block(peer, hash) {
                        queue.extend(answer_delivery(token, Answer::Failed).ok().flatten());
                    }
                }
            }
            PeerMessage::Inv(hashes) => {
                if !ibd {
                    queue.push_back(Delivery::Peer(peer, Input::Inv { hashes }));
                }
            }
        }
    }

    async fn drain(&mut self, queue: &mut VecDeque<Delivery>) -> Result<(), BlockSyncError> {
        let mut deliveries = 0;
        while let Some(delivery) = queue.pop_front() {
            deliveries += 1;
            if deliveries > MAX_DELIVERIES {
                return Err(BlockSyncError::InternalLoop);
            }
            self.deliver(delivery, queue).await?;
        }
        Ok(())
    }

    /// Builds the machine's context and runs it. An input for a machine that no longer exists
    /// is dropped.
    async fn deliver(
        &mut self,
        delivery: Delivery,
        queue: &mut VecDeque<Delivery>,
    ) -> Result<(), BlockSyncError> {
        let chain_room = MAX_CHAIN_IN_FLIGHT.saturating_sub(self.registry.chain_in_flight());
        match delivery {
            Delivery::Ibd(input) => {
                let Mode::Ibd(machine) = &mut self.mode else {
                    return Ok(());
                };
                let context = ibd::Context {
                    tip: self.tip,
                    minimum_chain_work: self.minimum_chain_work,
                    peers: &self.peers,
                    now: Instant::now(),
                    timeout: self.timeout,
                    chain_room,
                };
                let effects = machine.on_input(&context, input)?;
                self.perform(Owner::Ibd, effects, Token::Ibd, queue).await
            }
            Delivery::Peer(peer, input) => {
                let reorg_running = matches!(self.mode, Mode::Reorg(_));
                let Some(machine) = self.peers.get_mut(&peer) else {
                    return Ok(());
                };
                let context = peer::Context {
                    tip: self.tip,
                    reorg_running,
                };
                let effects = machine.on_input(&context, input)?;
                let wrap = |token| Token::Peer(peer, token);
                self.perform(Owner::Peer(peer), effects, wrap, queue).await
            }
            Delivery::Reorg(input) => {
                let Mode::Reorg(machine) = &mut self.mode else {
                    return Ok(());
                };
                let effects = machine.on_input(&reorg::Context { chain_room }, input)?;
                self.perform(Owner::Reorg, effects, Token::Reorg, queue)
                    .await
            }
            Delivery::Fetcher(input) => {
                let context = fetcher::Context { chain_room };
                let effects = self.fetcher.on_input(&context, input)?;
                self.perform(Owner::Fetcher, effects, Token::Fetcher, queue)
                    .await
            }
        }
    }

    async fn perform<T>(
        &mut self,
        owner: Owner,
        effects: Vec<Effect<T>>,
        wrap: impl Fn(T) -> Token,
        queue: &mut VecDeque<Delivery>,
    ) -> Result<(), BlockSyncError> {
        for effect in effects {
            match effect {
                Effect::GetHeaders {
                    peer,
                    locator,
                    stop,
                    token,
                } => {
                    let (command, payload) = messages::getheaders(&locator, stop)?;
                    let sent = self.net.send(peer, command, payload).await.is_ok();
                    self.registry.headers(peer, locator, wrap(token), sent);
                }
                Effect::GetData { peer, hash, token } => {
                    let (command, payload) = messages::getdata(hash)?;
                    let sent = self.net.send(peer, command, payload).await.is_ok();
                    self.registry.block(peer, hash, wrap(token), sent);
                }
                Effect::Chain { call, token } => {
                    let id = self.registry.chain(wrap(token));
                    let request = ChainRequest {
                        id,
                        reply: Arc::clone(&self.answer_queue),
                        call,
                    };
                    self.chain
                        .push(request)
                        .map_err(|_| BlockSyncError::ChainQueueFull)?;
                }
                Effect::FetchBlocks { peer, hashes } => {
                    queue.push_back(Delivery::Fetcher(Input::Fetch {
                        requester: owner,
                        peer,
                        hashes,
                    }));
                }
                Effect::Wake { at, token } => self.registry.wake(at, wrap(token)),
                Effect::Mode(change) => self.change_mode(change, queue),
                Effect::Tell { to, event } => {
                    queue.extend(to.into_iter().filter_map(|to| internal_delivery(to, event)));
                }
            }
        }
        Ok(())
    }

    /// The mode changes first, then the machines are told.
    fn change_mode(&mut self, change: ModeChange, queue: &mut VecDeque<Delivery>) {
        match (change, &self.mode) {
            (ModeChange::EnterStandard, Mode::Ibd(_)) => self.mode = Mode::Standard,
            (ModeChange::EnterStandard, Mode::Reorg(_)) => {
                self.mode = Mode::Standard;
                for &peer in self.peers.keys() {
                    queue.push_back(Delivery::Peer(peer, Input::Internal(Internal::TipChanged)));
                }
            }
            // Only one reorg runs at a time.
            (ModeChange::StartReorg { lca_height, chain }, Mode::Standard) => {
                self.mode = Mode::Reorg(ReorgMachine::new(self.tip.height, lca_height, chain));
            }
            _ => {}
        }
    }
}

/// The owner's input; `Err` for the start-up tip, which is answered before the loop runs.
fn answer_delivery(token: Token, answer: Answer) -> Result<Option<Delivery>, BlockSyncError> {
    fn input<T>(token: T, answer: Answer) -> Input<T> {
        match answer {
            Answer::Headers(headers) => Input::Headers { token, headers },
            Answer::Block(block) => Input::Block { token, block },
            Answer::Chain(result) => Input::Chain { token, result },
            Answer::Failed => Input::Failed { token },
            Answer::Wake => Input::Wake { token },
        }
    }
    Ok(Some(match token {
        Token::StartUpTip => return Err(BlockSyncError::UnexpectedAnswer),
        Token::Ibd(token) => Delivery::Ibd(input(token, answer)),
        Token::Peer(peer, token) => Delivery::Peer(peer, input(token, answer)),
        Token::Reorg(token) => Delivery::Reorg(input(token, answer)),
        Token::Fetcher(token) => Delivery::Fetcher(input(token, answer)),
    }))
}

fn internal_delivery(owner: Owner, event: Internal) -> Option<Delivery> {
    Some(match owner {
        Owner::Ibd => Delivery::Ibd(Input::Internal(event)),
        Owner::Peer(peer) => Delivery::Peer(peer, Input::Internal(event)),
        Owner::Reorg => Delivery::Reorg(Input::Internal(event)),
        Owner::Fetcher => Delivery::Fetcher(Input::Internal(event)),
    })
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
