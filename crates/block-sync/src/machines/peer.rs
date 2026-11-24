//! The peer machine (`docs/modules/block-sync/machines.md`, "Peer machine"): one per connected
//! peer in standard and reorg mode, its candidate.

use std::collections::HashSet;

use bitcoin::block::Header;
use bitcoin::pow::Work;
use bitcoin::{BlockHash, BlockHeight};
use interfaces::chain::{ChainCall, ChainError, ChainResult, Tip};
use interfaces::net::PeerId;

use super::{BLOCKS_IN_FLIGHT, Effect, Effects, Input, Internal, MAX_HEADERS, Machine, ModeChange};

pub struct Context {
    pub tip: Tip,
    /// The decisions made against the tip wait while a reorg runs.
    pub reorg_running: bool,
}

/// `generation` tells the answers of the current candidate from those of one it replaced.
#[derive(Debug, Clone, Copy)]
pub struct Token {
    generation: u64,
    step: Step,
}

#[derive(Debug, Clone, Copy)]
enum Step {
    InvHeader,
    Validate,
    Locator,
    Headers,
    /// `header_at` on the walk back to the active chain.
    Active,
    /// `header` of the cursor, for its parent.
    Parent,
    /// `header_at` on the walk forward past headers already on the active chain.
    Skip,
    StoreHeaders,
}

pub struct PeerMachine {
    peer: PeerId,
    generation: u64,
    state: State,
}

enum State {
    Idle,
    CheckingInv {
        hash: BlockHash,
    },
    Validating {
        headers: Vec<Header>,
        searched: bool,
    },
    Searching {
        target: BlockHash,
        collected: Vec<Header>,
    },
    FindingLca(Resolution),
    WaitingForTip(Validated),
    Downloading(Download),
    Ready(Download),
}

/// Validated headers, kept to find the LCA again when the tip changes.
struct Validated {
    /// In chain order; `headers[0]` is at `first_height`.
    headers: Vec<Header>,
    first_height: BlockHeight,
    last_chain_work: Work,
}

/// `docs/flows/mode-standard.md`, "Processing headers": back from the headers' parent to the
/// active chain, then, if the parent is on it, forward past the headers that are on it too.
struct Resolution {
    validated: Validated,
    /// The stored header checked against the active chain, at `height`.
    cursor: BlockHash,
    height: BlockHeight,
    /// The stored headers walked past, newest first: not on the active chain.
    path: Vec<BlockHash>,
    /// The headers found on the active chain, from the start.
    skip: usize,
}

struct Download {
    validated: Validated,
    lca_height: BlockHeight,
    /// LCA+1..H in height order.
    chain: Vec<BlockHash>,
    /// `chain[..requested_index]` are handed to the block fetcher.
    requested_index: usize,
    missing: HashSet<BlockHash>,
}

impl Machine for PeerMachine {
    type Token = Token;
    type Context<'a> = Context;

    fn on_input(&mut self, context: &Context, input: Input<Token>) -> Effects<Token> {
        let generation = self.generation;
        let current = |token: &Token| token.generation == generation;
        match input {
            // A failed write stops the node, also for a replaced candidate.
            Input::Chain {
                token:
                    Token {
                        step: Step::StoreHeaders,
                        ..
                    },
                result: ChainResult::StoreHeaders(result),
            } => {
                result?;
                Ok(Vec::new())
            }
            // A newer announcement replaces the candidate.
            Input::Announced { headers } if !headers.is_empty() => {
                self.generation += 1;
                Ok(vec![self.validate(headers, false)])
            }
            Input::Inv { hashes } if matches!(self.state, State::Idle) => {
                let Some(&hash) = hashes.last() else {
                    return Ok(Vec::new());
                };
                self.generation += 1;
                self.state = State::CheckingInv { hash };
                Ok(vec![self.chain(ChainCall::Header(hash), Step::InvHeader)])
            }
            Input::Chain { token, result } if current(&token) => {
                self.on_chain(context, token, result)
            }
            Input::Headers { token, headers } if current(&token) => Ok(self.on_headers(headers)),
            Input::Failed { token } if current(&token) => Ok(self.drop()),
            Input::Internal(event) => Ok(self.on_internal(context, event)),
            _ => Ok(Vec::new()),
        }
    }
}

impl PeerMachine {
    pub fn new(peer: PeerId) -> Self {
        Self {
            peer,
            generation: 0,
            state: State::Idle,
        }
    }

    fn on_chain(&mut self, context: &Context, token: Token, result: ChainResult) -> Effects<Token> {
        let state = std::mem::replace(&mut self.state, State::Idle);
        match (token.step, state, result) {
            (Step::InvHeader, State::CheckingInv { hash }, ChainResult::Header(entry)) => {
                if entry?.is_some() {
                    return Ok(Vec::new());
                }
                Ok(vec![self.search(hash)])
            }
            (
                Step::Locator,
                State::Searching { target, collected },
                ChainResult::Locator(locator),
            ) => {
                let locator = locator?;
                self.state = State::Searching { target, collected };
                Ok(vec![self.get_headers(locator, target)])
            }
            (
                Step::Validate,
                State::Validating { headers, searched },
                ChainResult::ValidateHeaders(result),
            ) => match result {
                Ok(validated) => {
                    let validated = Validated {
                        headers,
                        first_height: validated.first_height,
                        last_chain_work: validated.last_chain_work,
                    };
                    Ok(self.find_lca(context, validated))
                }
                Err(ChainError::NotConnected) if !searched => match headers.last() {
                    Some(last) => Ok(vec![self.search(last.block_hash())]),
                    None => Ok(Vec::new()),
                },
                // The docs do not cover invalid announced headers: the candidate is dropped.
                Err(ChainError::NotConnected | ChainError::InvalidHeader { .. }) => Ok(Vec::new()),
                Err(error) => Err(error.into()),
            },
            (Step::Active, State::FindingLca(resolution), ChainResult::HeaderAt(entry)) => {
                let active = entry?.map(|entry| entry.header.block_hash());
                Ok(self.on_active(context, resolution, active))
            }
            (Step::Parent, State::FindingLca(mut resolution), ChainResult::Header(entry)) => {
                let height = resolution.height.to_u32().checked_sub(1);
                let (Some(parent), Some(height)) = (entry?, height) else {
                    return Ok(Vec::new());
                };
                resolution.path.push(resolution.cursor);
                resolution.cursor = parent.header.prev_blockhash;
                resolution.height = BlockHeight::from_u32(height);
                Ok(self.ask(Step::Active, resolution))
            }
            (Step::Skip, State::FindingLca(resolution), ChainResult::HeaderAt(entry)) => {
                let active = entry?.map(|entry| entry.header.block_hash());
                Ok(self.on_skip(context, resolution, active))
            }
            // Answers this state no longer expects: the state is kept.
            (_, state, _) => {
                self.state = state;
                Ok(Vec::new())
            }
        }
    }

    /// The LCA search: full answers continue from the last header received.
    fn on_headers(&mut self, headers: Vec<Header>) -> Vec<Effect<Token>> {
        let State::Searching { target, collected } = &mut self.state else {
            return Vec::new();
        };
        let target = *target;
        let full = headers.len() == MAX_HEADERS;
        collected.extend(headers);
        let last = collected.last().map(Header::block_hash);
        if last == Some(target) {
            let collected = std::mem::take(collected);
            return vec![self.validate(collected, true)];
        }
        match last {
            Some(last) if full => vec![self.get_headers(vec![last], target)],
            // H is no longer on the peer's active chain.
            _ => self.drop(),
        }
    }

    fn on_internal(&mut self, context: &Context, event: Internal) -> Vec<Effect<Token>> {
        match event {
            Internal::TipChanged => {
                let state = std::mem::replace(&mut self.state, State::Idle);
                let validated = match state {
                    State::WaitingForTip(validated) => validated,
                    State::FindingLca(resolution) => resolution.validated,
                    State::Downloading(download) | State::Ready(download) => download.validated,
                    other => {
                        self.state = other;
                        return Vec::new();
                    }
                };
                // Stored blocks are found stored by the block fetcher: nothing is downloaded twice.
                self.generation += 1;
                self.find_lca(context, validated)
            }
            Internal::BlockStored(hash) => {
                let State::Downloading(download) = &mut self.state else {
                    return Vec::new();
                };
                download.missing.remove(&hash);
                if download.missing.is_empty() && download.requested_index == download.chain.len() {
                    return self.complete(context);
                }
                self.fetch_more()
            }
            Internal::BlockFailed { hash, peer } => {
                let (State::Downloading(download) | State::Ready(download)) = &self.state else {
                    return Vec::new();
                };
                if peer == self.peer {
                    return self.drop();
                }
                // Another peer failed for it: this one asks its own peer.
                if download.missing.contains(&hash) {
                    return vec![Effect::FetchBlocks {
                        peer: self.peer,
                        hashes: vec![hash],
                    }];
                }
                Vec::new()
            }
        }
    }

    /// Headers on the tip need no walk; while a reorg runs, the walk waits for it to end.
    fn find_lca(&mut self, context: &Context, validated: Validated) -> Vec<Effect<Token>> {
        if context.reorg_running {
            self.state = State::WaitingForTip(validated);
            return Vec::new();
        }
        let parent_height = validated.first_height.to_u32().checked_sub(1);
        let (Some(first), Some(parent_height)) = (validated.headers.first(), parent_height) else {
            return self.drop();
        };
        let resolution = Resolution {
            cursor: first.prev_blockhash,
            height: BlockHeight::from_u32(parent_height),
            validated,
            path: Vec::new(),
            skip: 0,
        };
        if resolution.cursor == context.tip.hash {
            return self.resolved(context, resolution);
        }
        self.ask(Step::Active, resolution)
    }

    fn on_active(
        &mut self,
        context: &Context,
        resolution: Resolution,
        active: Option<BlockHash>,
    ) -> Vec<Effect<Token>> {
        if context.reorg_running {
            self.state = State::WaitingForTip(resolution.validated);
            return Vec::new();
        }
        if active != Some(resolution.cursor) {
            return self.ask(Step::Parent, resolution);
        }
        // The cursor is the LCA. If it is the headers' parent, their first ones may be on the
        // active chain too: the answer to a sparse locator starts below the fork.
        if resolution.path.is_empty() && resolution.validated.first_height <= context.tip.height {
            return self.ask(Step::Skip, resolution);
        }
        self.resolved(context, resolution)
    }

    fn on_skip(
        &mut self,
        context: &Context,
        mut resolution: Resolution,
        active: Option<BlockHash>,
    ) -> Vec<Effect<Token>> {
        if context.reorg_running {
            self.state = State::WaitingForTip(resolution.validated);
            return Vec::new();
        }
        let on_active = resolution
            .validated
            .headers
            .get(resolution.skip)
            .is_some_and(|header| Some(header.block_hash()) == active);
        if !on_active {
            return self.resolved(context, resolution);
        }
        resolution.skip += 1;
        if resolution.skip == resolution.validated.headers.len() {
            // Every header is on the active chain already.
            return self.drop();
        }
        if skip_height(&resolution) > context.tip.height {
            return self.resolved(context, resolution);
        }
        self.ask(Step::Skip, resolution)
    }

    /// A chain that does not extend the tip is kept only with more chain work than the tip.
    fn resolved(&mut self, context: &Context, resolution: Resolution) -> Vec<Effect<Token>> {
        let Resolution {
            validated,
            cursor,
            height,
            path,
            skip,
        } = resolution;
        let (lca, lca_height) = match skip.checked_sub(1).and_then(|i| validated.headers.get(i)) {
            Some(header) => {
                let height = validated.first_height.to_u32() + u32::try_from(skip).unwrap_or(0);
                (
                    header.block_hash(),
                    BlockHeight::from_u32(height.saturating_sub(1)),
                )
            }
            None => (cursor, height),
        };
        if lca != context.tip.hash && validated.last_chain_work <= context.tip.chain_work {
            return self.drop();
        }
        let new: Vec<Header> = validated.headers.get(skip..).unwrap_or_default().to_vec();
        let mut chain: Vec<BlockHash> = path.into_iter().rev().collect();
        chain.extend(new.iter().map(Header::block_hash));
        if chain.is_empty() {
            return self.drop();
        }
        let mut effects = Vec::new();
        if !new.is_empty() {
            effects.push(self.chain(ChainCall::StoreHeaders(new), Step::StoreHeaders));
        }
        self.state = State::Downloading(Download {
            validated,
            lca_height,
            missing: chain.iter().copied().collect(),
            chain,
            requested_index: 0,
        });
        // Asked after `store_headers`: `chain` runs a read after the writes before it.
        effects.extend(self.fetch_more());
        effects
    }

    /// At most `BLOCKS_IN_FLIGHT` of the candidate's blocks are being fetched.
    fn fetch_more(&mut self) -> Vec<Effect<Token>> {
        let State::Downloading(download) = &mut self.state else {
            return Vec::new();
        };
        let unrequested = download.chain.len() - download.requested_index;
        let fetching = download.missing.len().saturating_sub(unrequested);
        let end = download
            .chain
            .len()
            .min(download.requested_index + BLOCKS_IN_FLIGHT.saturating_sub(fetching));
        let Some(hashes) = download.chain.get(download.requested_index..end) else {
            return Vec::new();
        };
        if hashes.is_empty() {
            return Vec::new();
        }
        let hashes = hashes.to_vec();
        download.requested_index = end;
        vec![Effect::FetchBlocks {
            peer: self.peer,
            hashes,
        }]
    }

    /// Every block is stored: reorg mode starts now, or after the reorg that runs.
    fn complete(&mut self, context: &Context) -> Vec<Effect<Token>> {
        let state = std::mem::replace(&mut self.state, State::Idle);
        let State::Downloading(download) = state else {
            self.state = state;
            return Vec::new();
        };
        if context.reorg_running {
            self.state = State::Ready(download);
            return Vec::new();
        }
        vec![Effect::Mode(ModeChange::StartReorg {
            lca_height: download.lca_height,
            chain: download.chain,
        })]
    }

    fn drop(&mut self) -> Vec<Effect<Token>> {
        self.state = State::Idle;
        Vec::new()
    }

    fn validate(&mut self, headers: Vec<Header>, searched: bool) -> Effect<Token> {
        let call = ChainCall::ValidateHeaders(headers.clone());
        self.state = State::Validating { headers, searched };
        self.chain(call, Step::Validate)
    }

    fn search(&mut self, target: BlockHash) -> Effect<Token> {
        self.state = State::Searching {
            target,
            collected: Vec::new(),
        };
        self.chain(ChainCall::Locator, Step::Locator)
    }

    fn ask(&mut self, step: Step, resolution: Resolution) -> Vec<Effect<Token>> {
        let call = match step {
            Step::Parent => ChainCall::Header(resolution.cursor),
            Step::Skip => ChainCall::HeaderAt(skip_height(&resolution)),
            _ => ChainCall::HeaderAt(resolution.height),
        };
        self.state = State::FindingLca(resolution);
        vec![self.chain(call, step)]
    }

    fn get_headers(&self, locator: Vec<BlockHash>, stop: BlockHash) -> Effect<Token> {
        Effect::GetHeaders {
            peer: self.peer,
            locator,
            stop,
            token: self.token(Step::Headers),
        }
    }

    fn chain(&self, call: ChainCall, step: Step) -> Effect<Token> {
        Effect::Chain {
            call,
            token: self.token(step),
        }
    }

    fn token(&self, step: Step) -> Token {
        Token {
            generation: self.generation,
            step,
        }
    }
}

/// The height of `headers[skip]`.
fn skip_height(resolution: &Resolution) -> BlockHeight {
    let skip = u32::try_from(resolution.skip).unwrap_or(u32::MAX);
    BlockHeight::from_u32(
        resolution
            .validated
            .first_height
            .to_u32()
            .saturating_add(skip),
    )
}
