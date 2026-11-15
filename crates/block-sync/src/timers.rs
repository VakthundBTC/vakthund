//! block-sync's own scheduled events: the deadlines of its requests to peers
//! (`docs/modules/block-sync/README.md`, "Request timeout").

use std::collections::BTreeMap;

use bitcoin::BlockHash;
use interfaces::net::PeerId;
use tokio::time::Instant;

/// Identifies a scheduled timer, to cancel it. A request keeps its timer's key, and a timer that
/// fires acts only if its request still holds this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimerKey(Instant, u64);

#[expect(
    dead_code,
    reason = "scheduled by the IBD and standard-mode flows, which come next"
)]
pub enum Timer {
    Headers { peer: PeerId },
    Block { hash: BlockHash },
}

#[derive(Default)]
pub struct Timers {
    /// Ordered by deadline; the counter tells apart timers at the same instant.
    entries: BTreeMap<TimerKey, Timer>,
    next: u64,
}

impl Timers {
    /// Returns the key that cancels it.
    #[expect(
        dead_code,
        reason = "used by the IBD and standard-mode flows, which come next"
    )]
    pub fn schedule(&mut self, at: Instant, timer: Timer) -> TimerKey {
        let key = TimerKey(at, self.next);
        self.next += 1;
        self.entries.insert(key, timer);
        key
    }

    /// Does nothing for a timer that has fired or been cancelled.
    pub fn cancel(&mut self, key: TimerKey) {
        self.entries.remove(&key);
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.entries.keys().next().map(|TimerKey(at, _)| *at)
    }

    /// Removes and returns the timers due at `now`, earliest first, with their keys.
    pub fn expired(&mut self, now: Instant) -> Vec<(TimerKey, Timer)> {
        let later = self.entries.split_off(&TimerKey(now, u64::MAX));
        std::mem::replace(&mut self.entries, later)
            .into_iter()
            .collect()
    }
}
