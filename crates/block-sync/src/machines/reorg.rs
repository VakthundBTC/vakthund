//! The reorg machine (`docs/modules/block-sync/machines.md`, "Reorg machine"): disconnects down
//! to A, then connects A+1..B. Appending to the tip is a reorg with k = 0.

use bitcoin::{BlockHash, BlockHeight};
use interfaces::chain::{ChainCall, ChainError, ChainResult};

use super::{Effect, Effects, Input, Machine, ModeChange};
use crate::error::BlockSyncError;

pub struct Context {
    /// Requests to `chain` that may still be sent, so its queue never fills up.
    pub chain_room: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum Token {
    Disconnect,
    Connect(BlockHash),
}

/// Always `connected_index <= connect_sent_index <= chain.len()`.
pub struct ReorgMachine {
    lca_height: BlockHeight,
    /// The tip's height once every `disconnect_tip` sent so far is applied.
    disconnect_sent_height: BlockHeight,
    /// A+1..B in height order.
    chain: Vec<BlockHash>,
    /// The index of the next block to send `connect_block` for.
    connect_sent_index: usize,
    /// The index of the next block to be connected.
    connected_index: usize,
}

impl ReorgMachine {
    /// No other tip change runs, so the tip is exact.
    pub fn new(tip_height: BlockHeight, lca_height: BlockHeight, chain: Vec<BlockHash>) -> Self {
        Self {
            lca_height,
            disconnect_sent_height: tip_height,
            chain,
            connect_sent_index: 0,
            connected_index: 0,
        }
    }
}

impl Machine for ReorgMachine {
    type Token = Token;
    type Context<'a> = Context;

    fn on_input(&mut self, context: &Context, input: Input<Token>) -> Effects<Token> {
        match input {
            // `chain` runs writes in request order: none waits for an answer.
            Input::Advance => {
                let mut effects = Vec::new();
                while self.disconnect_sent_height > self.lca_height
                    && effects.len() < context.chain_room
                {
                    effects.push(Effect::Chain {
                        call: ChainCall::DisconnectTip,
                        token: Token::Disconnect,
                    });
                    let height = self.disconnect_sent_height.to_u32().saturating_sub(1);
                    self.disconnect_sent_height = BlockHeight::from_u32(height);
                }
                while self.disconnect_sent_height <= self.lca_height
                    && effects.len() < context.chain_room
                    && let Some(&hash) = self.chain.get(self.connect_sent_index)
                {
                    effects.push(Effect::Chain {
                        call: ChainCall::ConnectBlock(hash),
                        token: Token::Connect(hash),
                    });
                    self.connect_sent_index += 1;
                }
                Ok(effects)
            }
            // The shell keeps the new tip.
            Input::Chain {
                token: Token::Disconnect,
                result: ChainResult::DisconnectTip(result),
            } => {
                result?;
                Ok(Vec::new())
            }
            Input::Chain {
                token: Token::Connect(hash),
                result: ChainResult::ConnectBlock(result),
            } => match result {
                Ok(_) => {
                    if self.chain.get(self.connected_index) == Some(&hash) {
                        self.connected_index += 1;
                    }
                    if self.connected_index == self.chain.len() {
                        return Ok(vec![Effect::Mode(ModeChange::EnterStandard)]);
                    }
                    Ok(Vec::new())
                }
                Err(ChainError::Consensus { reason }) => {
                    Err(BlockSyncError::Consensus { hash, reason })
                }
                Err(error) => Err(error.into()),
            },
            Input::Chain { .. } => Err(BlockSyncError::UnexpectedAnswer),
            _ => Ok(Vec::new()),
        }
    }
}
