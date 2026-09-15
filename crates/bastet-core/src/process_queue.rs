//! Nonblocking output handoff with count and retained-wire-byte limits.
//! Overflow is sticky and discards the stream rather than silently losing events.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    Arc,
};
use std::time::Duration;

pub const OUTPUT_QUEUE_BYTES: usize = 16 * 1024 * 1024;
pub const OUTPUT_QUEUE_ITEMS: usize = 128;

struct Envelope<T> {
    value: Option<T>,
    size: usize,
    used: Arc<AtomicUsize>,
}

impl<T> Drop for Envelope<T> {
    fn drop(&mut self) {
        self.used.fetch_sub(self.size, Ordering::AcqRel);
    }
}

pub struct OutputSender<T> {
    sender: SyncSender<Envelope<T>>,
    used: Arc<AtomicUsize>,
    failed: Arc<AtomicBool>,
}

pub struct OutputReceiver<T> {
    receiver: Receiver<Envelope<T>>,
    failed: Arc<AtomicBool>,
}

pub fn output_queue<T>() -> (OutputSender<T>, OutputReceiver<T>) {
    let (sender, receiver) = mpsc::sync_channel(OUTPUT_QUEUE_ITEMS);
    let used = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    (
        OutputSender {
            sender,
            used,
            failed: failed.clone(),
        },
        OutputReceiver { receiver, failed },
    )
}

impl<T> OutputSender<T> {
    /// Never blocks the stdout reader. `wire_bytes` is the original UTF-8
    /// frame length, not an estimate of parsed object heap usage.
    pub fn send(&self, value: T, wire_bytes: usize) -> bool {
        if self.failed.load(Ordering::Acquire) {
            return false;
        }
        if self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(wire_bytes)
                    .filter(|total| *total <= OUTPUT_QUEUE_BYTES)
            })
            .is_err()
        {
            self.failed.store(true, Ordering::Release);
            return false;
        }
        let envelope = Envelope {
            value: Some(value),
            size: wire_bytes,
            used: self.used.clone(),
        };
        if self.sender.try_send(envelope).is_err() {
            self.failed.store(true, Ordering::Release);
            return false;
        }
        true
    }
}

impl<T> OutputReceiver<T> {
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        if self.is_failed() {
            return Err(RecvTimeoutError::Disconnected);
        }
        let mut envelope = self.receiver.recv_timeout(timeout)?;
        if self.is_failed() {
            return Err(RecvTimeoutError::Disconnected);
        }
        Ok(envelope.value.take().expect("queue envelope has one value"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_budget_is_released_only_on_consumption_and_overflow_is_sticky() {
        let (sender, receiver) = output_queue();
        assert!(sender.send(1, OUTPUT_QUEUE_BYTES));
        assert_eq!(receiver.recv_timeout(Duration::ZERO), Ok(1));
        assert!(sender.send(2, OUTPUT_QUEUE_BYTES));
        assert!(!sender.send(3, 1));
        assert!(receiver.is_failed());
        assert_eq!(
            receiver.recv_timeout(Duration::ZERO),
            Err(RecvTimeoutError::Disconnected)
        );
        assert!(!sender.send(4, 0));
    }

    #[test]
    fn count_budget_also_bounds_empty_frames_and_close_never_blocks() {
        let (sender, receiver) = output_queue();
        for _ in 0..OUTPUT_QUEUE_ITEMS {
            assert!(sender.send((), 0));
        }
        assert!(!sender.send((), 0));
        drop(receiver);
        assert_eq!(sender.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn disconnected_receiver_releases_reserved_bytes() {
        let (sender, receiver) = output_queue();
        drop(receiver);
        assert!(!sender.send((), OUTPUT_QUEUE_BYTES));
        assert_eq!(sender.used.load(Ordering::Acquire), 0);
    }
}
