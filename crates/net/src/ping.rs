//! Pings (`docs/modules/net/README.md`, "Pings").

use std::time::Duration;

use bitcoin_p2p_messages::message::{Ping, Pong};
use tokio::time::{self, Instant, Interval};

use crate::error::Error;

const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Bitcoin Core's `TIMEOUT_INTERVAL`.
pub const PONG_TIMEOUT: Duration = Duration::from_secs(20 * 60);

pub struct PingManager {
    interval: Interval,
    last_ping: Option<Ping>,
    last_pong: Instant,
}

impl PingManager {
    pub fn new() -> Self {
        Self {
            interval: time::interval(PING_INTERVAL),
            last_ping: None,
            last_pong: Instant::now(),
        }
    }

    /// Waits for the time of the next ping and returns it. Cancel safe, for `select!`.
    pub async fn next_ping(&mut self) -> Result<Ping, Error> {
        self.interval.tick().await;
        if self.last_pong.elapsed() >= PONG_TIMEOUT {
            return Err(Error::PongTimeout);
        }
        let ping = Ping::new(rand::random());
        self.last_ping = Some(ping.clone());
        Ok(ping)
    }

    /// Only a pong that answers the last ping counts.
    pub fn pong_received(&mut self, pong: &Pong) {
        if self
            .last_ping
            .as_ref()
            .is_some_and(|ping| Pong::from_ping(ping) == *pong)
        {
            self.last_pong = Instant::now();
        }
    }
}
