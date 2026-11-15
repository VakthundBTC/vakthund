//! The deadlines of requests (`docs/modules/block-sync/README.md`, "Request timeout"), ordered by
//! time.

use std::collections::BTreeSet;

use tokio::time::Instant;

/// `T` identifies a deadline and tells apart deadlines at the same instant.
pub struct Timers<T> {
    entries: BTreeSet<(Instant, T)>,
}

impl<T> Default for Timers<T> {
    fn default() -> Self {
        Self {
            entries: BTreeSet::new(),
        }
    }
}

impl<T: Ord + Copy> Timers<T> {
    pub fn schedule(&mut self, at: Instant, id: T) {
        self.entries.insert((at, id));
    }

    /// Does nothing for a deadline that has passed or been cancelled.
    pub fn cancel(&mut self, at: Instant, id: T) {
        self.entries.remove(&(at, id));
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.entries.first().map(|(at, _)| *at)
    }

    /// Removes and returns the ids due at `now`, earliest first.
    pub fn expired(&mut self, now: Instant) -> Vec<T> {
        let mut due = Vec::new();
        while let Some(&(at, id)) = self.entries.first()
            && at <= now
        {
            self.entries.pop_first();
            due.push(id);
        }
        due
    }
}
