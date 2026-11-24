//! The IBD machine (`docs/modules/block-sync/machines.md`, "IBD machine"): the headers from the
//! sync peer, then their blocks, connected in height order.

use std::collections::HashSet;
use std::time::Duration;

use bitcoin::BlockHash;
use bitcoin::block::Header;
use bitcoin::pow::Work;
use interfaces::chain::{ChainCall, ChainError, ChainResult, Tip};
use interfaces::net::PeerId;
use tokio::time::Instant;

use super::{
    BLOCKS_IN_FLIGHT, Effect, Effects, Input, Internal, MAX_HEADERS, Machine, ModeChange, NO_STOP,
};
use crate::book::PeerBook;
use crate::error::BlockSyncError;

/// How far past the next block to connect blocks are fetched: Core's `BLOCK_DOWNLOAD_WINDOW`.
const DOWNLOAD_WINDOW: usize = 1024;

pub struct Context<'a> {
    pub tip: Tip,
    pub minimum_chain_work: Work,
    /// The connected peers, to choose the sync peer from.
    pub book: &'a PeerBook,
    pub now: Instant,
    pub timeout: Duration,
    /// Requests to `chain` that may still be sent, so its queue never fills up.
    pub chain_room: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum Token {
    Locator(PeerId),
    Headers(PeerId),
    Validate,
    Store,
    Connect(BlockHash),
    Restart,
}

pub struct IbdMachine {
    /// `None` in WaitingForPeer.
    sync_peer: Option<PeerId>,
    /// The peers that have failed as the sync peer since every connected peer was last tried.
    tried: HashSet<PeerId>,
    phase: Phase,
}

enum Phase {
    /// WaitingForPeer while there is no sync peer.
    Headers {
        received: Vec<Header>,
    },
    Validating {
        headers: Vec<Header>,
    },
    StoringHeaders {
        hashes: Vec<BlockHash>,
    },
    Blocks(Blocks),
}

/// The blocks of the stored headers `tip+1 ...`, connected in height order. Always
/// `connected_index <= connect_sent_index <= requested_index <= hashes.len()`.
struct Blocks {
    hashes: Vec<BlockHash>,
    /// The index of the next block to fetch: `hashes[..requested_index]` are handed to the
    /// block fetcher.
    requested_index: usize,
    /// The index of the next block to send `connect_block` for.
    connect_sent_index: usize,
    /// The index of the next block to be connected.
    connected_index: usize,
    /// Stored and not yet sent to `connect_block`.
    stored: HashSet<BlockHash>,
}

impl Blocks {
    fn pending(&self) -> &[BlockHash] {
        self.hashes
            .get(self.connect_sent_index..self.requested_index)
            .unwrap_or_default()
    }
}

impl Default for IbdMachine {
    fn default() -> Self {
        Self {
            sync_peer: None,
            tried: HashSet::new(),
            phase: Phase::Headers {
                received: Vec::new(),
            },
        }
    }
}

impl Machine for IbdMachine {
    type Token = Token;
    type Context<'a> = Context<'a>;

    fn on_input(&mut self, context: &Context, input: Input<Token>) -> Effects<Token> {
        match input {
            Input::PeerConnected
            | Input::Wake {
                token: Token::Restart,
            } => Ok(self.choose_sync_peer(context)),
            Input::PeerDisconnected(peer) => Ok(self.sync_peer_failed(context, peer)),
            Input::Chain {
                token: Token::Locator(peer),
                result: ChainResult::Locator(locator),
            } => {
                let locator = locator?;
                if self.sync_peer != Some(peer) || !matches!(self.phase, Phase::Headers { .. }) {
                    return Ok(Vec::new());
                }
                Ok(vec![get_headers(peer, locator)])
            }
            Input::Headers {
                token: Token::Headers(peer),
                headers,
            } => Ok(self.on_headers(context, peer, headers)),
            Input::Failed {
                token: Token::Headers(peer),
            } => Ok(self.sync_peer_failed(context, peer)),
            Input::Chain {
                token: Token::Validate,
                result: ChainResult::ValidateHeaders(result),
            } => {
                let Phase::Validating { headers } = &mut self.phase else {
                    return Ok(Vec::new());
                };
                let valid = match result {
                    Ok(_) => {
                        let headers = std::mem::take(headers);
                        let hashes = headers.iter().map(Header::block_hash).collect();
                        self.phase = Phase::StoringHeaders { hashes };
                        return Ok(vec![Effect::Chain {
                            call: ChainCall::StoreHeaders(headers),
                            token: Token::Store,
                        }]);
                    }
                    Err(ChainError::InvalidHeader { index, .. }) => index,
                    Err(ChainError::NotConnected) => 0,
                    Err(error) => return Err(error.into()),
                };
                // The docs do not cover it: the valid headers are kept, another sync peer
                // continues from them.
                let mut received = std::mem::take(headers);
                received.truncate(valid);
                self.phase = Phase::Headers { received };
                match self.sync_peer {
                    Some(peer) => Ok(self.sync_peer_failed(context, peer)),
                    None => Ok(self.choose_sync_peer(context)),
                }
            }
            Input::Chain {
                token: Token::Store,
                result: ChainResult::StoreHeaders(result),
            } => {
                result?;
                let Phase::StoringHeaders { hashes } = &mut self.phase else {
                    return Ok(Vec::new());
                };
                self.phase = Phase::Blocks(Blocks {
                    hashes: std::mem::take(hashes),
                    requested_index: 0,
                    connect_sent_index: 0,
                    connected_index: 0,
                    stored: HashSet::new(),
                });
                Ok(self.advance(context))
            }
            Input::Internal(Internal::BlockStored(hash)) => {
                if let Phase::Blocks(blocks) = &mut self.phase
                    && blocks.pending().contains(&hash)
                {
                    blocks.stored.insert(hash);
                }
                Ok(Vec::new())
            }
            Input::Internal(Internal::BlockFailed { peer, .. }) => {
                if matches!(self.phase, Phase::Blocks(_)) {
                    return Ok(self.sync_peer_failed(context, peer));
                }
                Ok(Vec::new())
            }
            Input::Chain {
                token: Token::Connect(hash),
                result: ChainResult::ConnectBlock(result),
            } => match result {
                Ok(_) => {
                    let Phase::Blocks(blocks) = &mut self.phase else {
                        return Ok(Vec::new());
                    };
                    // A connect of a download that restarted only moves the tip.
                    if blocks.hashes.get(blocks.connected_index) == Some(&hash) {
                        blocks.connected_index += 1;
                        if blocks.connected_index == blocks.hashes.len() {
                            return Ok(self.finished(context));
                        }
                    }
                    Ok(Vec::new())
                }
                Err(ChainError::Consensus { reason }) => {
                    Err(BlockSyncError::Consensus { hash, reason })
                }
                Err(error) => Err(error.into()),
            },
            Input::Advance => Ok(self.advance(context)),
            Input::Chain { .. } => Err(BlockSyncError::UnexpectedAnswer),
            _ => Ok(Vec::new()),
        }
    }
}

impl IbdMachine {
    fn on_headers(
        &mut self,
        context: &Context,
        peer: PeerId,
        headers: Vec<Header>,
    ) -> Vec<Effect<Token>> {
        let Phase::Headers { received } = &mut self.phase else {
            return Vec::new();
        };
        if self.sync_peer != Some(peer) {
            return Vec::new();
        }
        let full = headers.len() == MAX_HEADERS;
        received.extend(headers);
        if full && let Some(last) = received.last() {
            return vec![get_headers(peer, vec![last.block_hash()])];
        }
        if received.is_empty() {
            return self.finished(context);
        }
        let headers = std::mem::take(received);
        let call = ChainCall::ValidateHeaders(headers.clone());
        self.phase = Phase::Validating { headers };
        vec![Effect::Chain {
            call,
            token: Token::Validate,
        }]
    }

    /// Connects the stored blocks in height order, and fetches the next ones from the sync peer.
    /// `chain` runs writes in request order: a connect waits for no answer.
    fn advance(&mut self, context: &Context) -> Vec<Effect<Token>> {
        let Phase::Blocks(blocks) = &mut self.phase else {
            return Vec::new();
        };
        let mut effects = Vec::new();
        while effects.len() < context.chain_room
            && let Some(&hash) = blocks.hashes.get(blocks.connect_sent_index)
            && blocks.stored.remove(&hash)
        {
            effects.push(Effect::Chain {
                call: ChainCall::ConnectBlock(hash),
                token: Token::Connect(hash),
            });
            blocks.connect_sent_index += 1;
        }
        let Some(peer) = self.sync_peer else {
            return effects;
        };
        let fetching = blocks.pending().len() - blocks.stored.len().min(blocks.pending().len());
        let room = BLOCKS_IN_FLIGHT.saturating_sub(fetching);
        let window_end = blocks.connected_index + DOWNLOAD_WINDOW;
        let end = blocks
            .hashes
            .len()
            .min(window_end)
            .min(blocks.requested_index + room);
        if let Some(hashes) = blocks.hashes.get(blocks.requested_index..end)
            && !hashes.is_empty()
        {
            effects.push(Effect::FetchBlocks {
                peer,
                hashes: hashes.to_vec(),
            });
            blocks.requested_index = end;
        }
        effects
    }

    /// Every block is connected, or there was nothing to download. IBD ends at the minimum chain
    /// work; below it the sync peer was behind.
    fn finished(&mut self, context: &Context) -> Vec<Effect<Token>> {
        if context.tip.chain_work >= context.minimum_chain_work {
            return vec![Effect::Mode(ModeChange::EnterStandard)];
        }
        self.phase = Phase::Headers {
            received: Vec::new(),
        };
        match self.sync_peer {
            Some(peer) => self.sync_peer_failed(context, peer),
            None => self.choose_sync_peer(context),
        }
    }

    /// Nothing unless `peer` is the sync peer. The header download continues after the last
    /// header received; from Blocks the headers start again from the tip. Headers being
    /// validated or stored go on.
    fn sync_peer_failed(&mut self, context: &Context, peer: PeerId) -> Vec<Effect<Token>> {
        if self.sync_peer != Some(peer) {
            return Vec::new();
        }
        self.sync_peer = None;
        self.tried.insert(peer);
        if matches!(self.phase, Phase::Blocks(_)) {
            self.phase = Phase::Headers {
                received: Vec::new(),
            };
        }
        self.choose_sync_peer(context)
    }

    /// A random connected peer that has not failed. When every connected peer has, the peers
    /// tried are cleared and IBD waits `request_timeout_secs`.
    fn choose_sync_peer(&mut self, context: &Context) -> Vec<Effect<Token>> {
        if self.sync_peer.is_some() {
            return Vec::new();
        }
        let untried: Vec<PeerId> = context
            .book
            .peers()
            .filter(|peer| !self.tried.contains(peer))
            .collect();
        let Some(&peer) = untried.get(rand::random_range(0..untried.len().max(1))) else {
            if context.book.peers().next().is_none() {
                return Vec::new();
            }
            self.tried.clear();
            return vec![Effect::Wake {
                at: context.now + context.timeout,
                token: Token::Restart,
            }];
        };
        self.sync_peer = Some(peer);
        match &self.phase {
            Phase::Headers { received } => match received.last() {
                Some(last) => vec![get_headers(peer, vec![last.block_hash()])],
                None => vec![Effect::Chain {
                    call: ChainCall::Locator,
                    token: Token::Locator(peer),
                }],
            },
            // The request in flight goes on; the new sync peer takes over after it.
            Phase::Validating { .. } | Phase::StoringHeaders { .. } | Phase::Blocks(_) => {
                Vec::new()
            }
        }
    }
}

fn get_headers(peer: PeerId, locator: Vec<BlockHash>) -> Effect<Token> {
    Effect::GetHeaders {
        peer,
        locator,
        stop: NO_STOP,
        token: Token::Headers(peer),
    }
}
