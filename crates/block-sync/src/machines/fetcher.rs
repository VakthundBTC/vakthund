//! The block fetcher (`docs/modules/block-sync/machines.md`, "Block fetcher"): one state per
//! block, so a block is requested once whichever machines need it. A failed request asks no
//! other peer.

use std::collections::{HashMap, VecDeque};

use bitcoin::BlockHash;
use interfaces::chain::{ChainCall, ChainError, ChainResult};
use interfaces::net::PeerId;

use super::{Effect, Effects, Input, Internal, Machine, Owner};
use crate::error::BlockSyncError;

pub struct Context {
    /// Requests to `chain` that may still be sent, so its queue never fills up.
    pub chain_room: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum Token {
    /// `header` of the block: is it stored already?
    Check(BlockHash),
    Request(BlockHash),
    Store(BlockHash),
}

#[derive(Default)]
pub struct BlockFetcher {
    blocks: HashMap<BlockHash, Fetch>,
    /// Asked for and not checked yet: checks wait for room for requests to `chain`.
    waiting: VecDeque<BlockHash>,
}

struct Fetch {
    /// Told when the block is stored or its request failed.
    requesters: Vec<Owner>,
    peer: PeerId,
    stage: Stage,
}

#[derive(PartialEq, Eq)]
enum Stage {
    Waiting,
    Checking,
    Requested,
    Storing,
}

impl Machine for BlockFetcher {
    type Token = Token;
    type Context<'a> = Context;

    fn on_input(&mut self, context: &Context, input: Input<Token>) -> Effects<Token> {
        match input {
            Input::Fetch {
                requester,
                peer,
                hashes,
            } => {
                for hash in hashes {
                    self.ask(requester, peer, hash);
                }
                Ok(self.check_waiting(context))
            }
            Input::Advance => Ok(self.check_waiting(context)),
            Input::Chain {
                token: Token::Check(hash),
                result: ChainResult::Header(entry),
            } => {
                if entry?.is_some_and(|entry| entry.has_block) {
                    return Ok(self.finish(hash, None));
                }
                let Some(fetch) = self.in_stage(hash, Stage::Checking) else {
                    return Ok(Vec::new());
                };
                fetch.stage = Stage::Requested;
                let peer = fetch.peer;
                Ok(vec![Effect::GetData {
                    peer,
                    hash,
                    token: Token::Request(hash),
                }])
            }
            Input::Block {
                token: Token::Request(hash),
                block,
            } => {
                let Some(fetch) = self.in_stage(hash, Stage::Requested) else {
                    return Ok(Vec::new());
                };
                fetch.stage = Stage::Storing;
                Ok(vec![Effect::Chain {
                    call: ChainCall::StoreBlock(block),
                    token: Token::Store(hash),
                }])
            }
            Input::Unsolicited { peer, block } => {
                let hash = block.block_hash();
                let fetch = self.blocks.entry(hash).or_insert(Fetch {
                    requesters: Vec::new(),
                    peer,
                    stage: Stage::Waiting,
                });
                if fetch.stage == Stage::Storing {
                    return Ok(Vec::new());
                }
                // A request for it from another peer is answered by this block.
                fetch.stage = Stage::Storing;
                fetch.peer = peer;
                self.waiting.retain(|waiting| *waiting != hash);
                Ok(vec![Effect::Chain {
                    call: ChainCall::StoreBlock(block),
                    token: Token::Store(hash),
                }])
            }
            Input::Chain {
                token: Token::Store(hash),
                result: ChainResult::StoreBlock(result),
            } => match result {
                Ok(()) => Ok(self.finish(hash, None)),
                Err(ChainError::Mutated { .. }) => {
                    let peer = self.blocks.get(&hash).map(|fetch| fetch.peer);
                    Ok(self.finish(hash, peer))
                }
                Err(ChainError::Consensus { reason }) => {
                    Err(BlockSyncError::Consensus { hash, reason })
                }
                // An unsolicited block whose header is not stored is dropped.
                Err(ChainError::UnknownHeader) => {
                    self.blocks.remove(&hash);
                    Ok(Vec::new())
                }
                Err(error) => Err(error.into()),
            },
            Input::Failed {
                token: Token::Request(hash),
            } => {
                let Some(fetch) = self.in_stage(hash, Stage::Requested) else {
                    return Ok(Vec::new());
                };
                let peer = fetch.peer;
                Ok(self.finish(hash, Some(peer)))
            }
            Input::PeerDisconnected(peer) => {
                let asked: Vec<BlockHash> = self
                    .blocks
                    .iter()
                    .filter(|(_, fetch)| fetch.peer == peer && fetch.stage == Stage::Requested)
                    .map(|(hash, _)| *hash)
                    .collect();
                Ok(asked
                    .into_iter()
                    .flat_map(|hash| self.finish(hash, Some(peer)))
                    .collect())
            }
            Input::Chain { .. } => Err(BlockSyncError::UnexpectedAnswer),
            _ => Ok(Vec::new()),
        }
    }
}

impl BlockFetcher {
    /// A block in progress only gets another requester.
    fn ask(&mut self, requester: Owner, peer: PeerId, hash: BlockHash) {
        if let Some(fetch) = self.blocks.get_mut(&hash) {
            if !fetch.requesters.contains(&requester) {
                fetch.requesters.push(requester);
            }
            return;
        }
        let fetch = Fetch {
            requesters: vec![requester],
            peer,
            stage: Stage::Waiting,
        };
        self.blocks.insert(hash, fetch);
        self.waiting.push_back(hash);
    }

    fn check_waiting(&mut self, context: &Context) -> Vec<Effect<Token>> {
        let mut effects = Vec::new();
        while effects.len() < context.chain_room
            && let Some(hash) = self.waiting.pop_front()
        {
            let Some(fetch) = self.in_stage(hash, Stage::Waiting) else {
                continue;
            };
            fetch.stage = Stage::Checking;
            effects.push(Effect::Chain {
                call: ChainCall::Header(hash),
                token: Token::Check(hash),
            });
        }
        effects
    }

    fn in_stage(&mut self, hash: BlockHash, stage: Stage) -> Option<&mut Fetch> {
        self.blocks
            .get_mut(&hash)
            .filter(|fetch| fetch.stage == stage)
    }

    /// `failed_from`: the peer whose request failed; `None`: the block is stored.
    fn finish(&mut self, hash: BlockHash, failed_from: Option<PeerId>) -> Vec<Effect<Token>> {
        let Some(fetch) = self.blocks.remove(&hash) else {
            return Vec::new();
        };
        if fetch.requesters.is_empty() {
            return Vec::new();
        }
        let event = match failed_from {
            Some(peer) => Internal::BlockFailed { hash, peer },
            None => Internal::BlockStored(hash),
        };
        vec![Effect::Tell {
            to: fetch.requesters,
            event,
        }]
    }
}
