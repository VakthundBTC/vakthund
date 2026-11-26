use std::env;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use interfaces::net::Net;
use interfaces::queue::{MessageQueue, QueueError, RoutedMessage};
use net::NetManager;
use node::config::Config;
use tokio::signal::unix::{SignalKind, signal};

#[derive(Debug, thiserror::Error)]
enum NodeError {
    #[error("net: {0}")]
    Net(#[from] net::Error),
    #[error("signal handler: {0}")]
    Signal(io::Error),
}

/// Stands in for the block-sync queue until `block-sync` exists: prints the messages and drops
/// them.
struct PrintQueue;

impl MessageQueue for PrintQueue {
    fn push(&self, message: RoutedMessage) -> Result<(), QueueError> {
        println!(
            "{:?}: {} ({} bytes)",
            message.peer,
            message.command,
            message.payload.len()
        );
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let Some(path) = env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("usage: vakthund <config.toml>");
        return ExitCode::FAILURE;
    };
    let config = match Config::load(&path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// Runs until SIGINT or SIGTERM, or a node failure.
async fn run(config: Config) -> Result<(), NodeError> {
    let dir = config.datadir.join(config.network.to_core_arg());
    let net = NetManager::open(&config.net, config.network, &dir, Arc::new(PrintQueue))?;
    let mut terminate = signal(SignalKind::terminate()).map_err(NodeError::Signal)?;
    let result = tokio::select! {
        result = net.run() => result.map_err(NodeError::from),
        result = tokio::signal::ctrl_c() => result.map_err(NodeError::Signal),
        _ = terminate.recv() => Ok(()),
    };
    // Also on a node failure: the connections are closed before the process exits.
    let stopped = net.stop().await.map_err(NodeError::from);
    result.and(stopped)
}
