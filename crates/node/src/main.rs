use std::convert::Infallible;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use interfaces::queue::{MessageQueue, QueueFull, RoutedMessage};
use net::NetManager;
use node::config::Config;

#[derive(Debug, thiserror::Error)]
enum NodeError {
    #[error("net: {0}")]
    Net(#[from] net::Error),
}

/// Stands in for the block-sync queue until `block-sync` exists: prints the messages and drops
/// them.
struct PrintQueue;

impl MessageQueue for PrintQueue {
    fn push(&self, message: RoutedMessage) -> Result<(), QueueFull> {
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
    let Err(error) = run(config).await;
    eprintln!("{error}");
    ExitCode::FAILURE
}

/// Returns only on a node failure.
async fn run(config: Config) -> Result<Infallible, NodeError> {
    let dir = config.datadir.join(config.network.to_core_arg());
    let net = NetManager::open(&config.net, config.network, &dir, Arc::new(PrintQueue))?;
    Ok(net.run().await?)
}
