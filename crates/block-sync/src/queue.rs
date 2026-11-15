//! block-sync's queues: the one `net` pushes to and the answer queue `chain` pushes to
//! (`docs/modules/block-sync/README.md`, "Communication").

use interfaces::queue::{NetEvent, Queue, QueueFull};
use tokio::sync::mpsc;

/// The docs give no size: block-sync reads its queue in every turn of its loop.
const NET_EVENTS: usize = 1024;

pub struct QueueSender<T>(mpsc::Sender<T>);

pub struct QueueReceiver<T>(pub(crate) mpsc::Receiver<T>);

/// The queue `net` pushes to.
pub fn queue() -> (QueueSender<NetEvent>, QueueReceiver<NetEvent>) {
    channel(NET_EVENTS)
}

pub(crate) fn channel<T>(capacity: usize) -> (QueueSender<T>, QueueReceiver<T>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (QueueSender(sender), QueueReceiver(receiver))
}

impl<T: Send> Queue<T> for QueueSender<T> {
    /// A closed queue also fails: block-sync has stopped, so the node is stopping.
    fn push(&self, item: T) -> Result<(), QueueFull> {
        self.0.try_send(item).map_err(|_| QueueFull)
    }
}
