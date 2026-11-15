//! block-sync's end of the chain interface (`docs/modules/chain/README.md`, "Interface").

use std::sync::Arc;

use interfaces::chain::{ChainCall, ChainRequest, ChainRequestId, ChainResponse};
use interfaces::queue::Queue;

use crate::error::BlockSyncError;

pub struct ChainClient {
    requests: Arc<dyn Queue<ChainRequest>>,
    answers: Arc<dyn Queue<ChainResponse>>,
    next_id: u64,
    in_flight: usize,
}

impl ChainClient {
    /// Requests block-sync keeps in flight at most, so neither the chain queue nor block-sync's
    /// answer queue fills up. The chain queue must hold at least this many.
    pub const MAX_IN_FLIGHT: usize = 256;

    pub fn new(
        requests: Arc<dyn Queue<ChainRequest>>,
        answers: Arc<dyn Queue<ChainResponse>>,
    ) -> Self {
        Self {
            requests,
            answers,
            next_id: 0,
            in_flight: 0,
        }
    }

    #[expect(
        dead_code,
        reason = "used by the IBD and standard-mode flows, which come next"
    )]
    pub fn has_room(&self, requests: usize) -> bool {
        self.in_flight + requests <= Self::MAX_IN_FLIGHT
    }

    pub fn send(&mut self, call: ChainCall) -> Result<ChainRequestId, BlockSyncError> {
        let id = ChainRequestId(self.next_id);
        let request = ChainRequest {
            id,
            reply: Arc::clone(&self.answers),
            call,
        };
        self.requests
            .push(request)
            .map_err(|_| BlockSyncError::ChainQueueFull)?;
        self.next_id += 1;
        self.in_flight += 1;
        Ok(id)
    }

    pub fn answered(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }
}
