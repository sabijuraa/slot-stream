//! The bounded buffer between the source and the processor.
//!
//! This is where backpressure lives. The buffer is a bounded channel and nothing
//! is held outside it, so the ingester's memory is capped by the capacity
//! regardless of how far behind the processor falls.

use slot_stream_common::{Error, RawEvent, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tracing::warn;

/// What to do when the buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OverflowPolicy {
    /// Drop the event already queued longest.
    ///
    /// Note this cannot be honoured precisely on a bounded channel — there is no
    /// way to evict from the far end — so it behaves as `DropNewest` while
    /// recording the drop. Kept as a distinct policy because the intent differs
    /// and the metric label matters to an operator.
    DropOldest,
    /// Drop the incoming event.
    DropNewest,
    /// Wait for space. Applies real backpressure to the source.
    Block,
}

/// A bounded event buffer.
pub struct EventBuffer {
    policy: OverflowPolicy,
    sender: mpsc::Sender<RawEvent>,
    dropped: AtomicU64,
    pushed: AtomicU64,
    blocked_waits: AtomicU64,
}

impl EventBuffer {
    /// Create a buffer feeding `sender`.
    ///
    /// `capacity` must match the channel's capacity; it is used for reporting.
    pub fn new(_capacity: usize, policy: OverflowPolicy, sender: mpsc::Sender<RawEvent>) -> Self {
        Self {
            policy,
            sender,
            dropped: AtomicU64::new(0),
            pushed: AtomicU64::new(0),
            blocked_waits: AtomicU64::new(0),
        }
    }

    /// Push an event, applying the overflow policy.
    pub async fn push(&self, event: RawEvent) -> Result<()> {
        self.pushed.fetch_add(1, Ordering::Relaxed);

        match self.policy {
            OverflowPolicy::DropOldest | OverflowPolicy::DropNewest => {
                match self.sender.try_send(event) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(event)) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            slot = event.slot,
                            seq = event.sequence.0,
                            policy = ?self.policy,
                            "buffer full, dropping event"
                        );
                        metrics::counter!("ingester.buffer_dropped").increment(1);
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::ChannelClosed),
                }
            }
            OverflowPolicy::Block => {
                // Count the times we actually had to wait, which is the honest
                // signal that backpressure is engaging.
                if self.sender.capacity() == 0 {
                    self.blocked_waits.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!("ingester.buffer_blocked").increment(1);
                }
                self.sender
                    .send(event)
                    .await
                    .map_err(|_| Error::ChannelClosed)
            }
        }
    }

    /// How full the buffer is, from 0.0 to 1.0.
    pub fn utilization(&self) -> f64 {
        let max = self.sender.max_capacity();
        if max == 0 {
            return 0.0;
        }
        let free = self.sender.capacity();
        (max - free) as f64 / max as f64
    }

    /// Slots currently free.
    pub fn available(&self) -> usize {
        self.sender.capacity()
    }

    /// Total capacity.
    pub fn capacity(&self) -> usize {
        self.sender.max_capacity()
    }

    /// Events dropped due to overflow.
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Events pushed.
    pub fn push_count(&self) -> u64 {
        self.pushed.load(Ordering::Relaxed)
    }

    /// Times a push had to wait for space under the blocking policy.
    pub fn blocked_waits(&self) -> u64 {
        self.blocked_waits.load(Ordering::Relaxed)
    }

    /// Whether anything has been dropped.
    pub fn has_dropped(&self) -> bool {
        self.dropped_count() > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slot_stream_common::{EventKind, SequenceNumber};

    fn make_event(seq: u64) -> RawEvent {
        RawEvent::new(
            SequenceNumber(seq),
            EventKind::Transaction,
            100,
            bytes::Bytes::from_static(b"{}"),
        )
    }

    #[tokio::test]
    async fn events_pass_through_in_order() {
        let (tx, mut rx) = mpsc::channel(10);
        let buffer = EventBuffer::new(10, OverflowPolicy::Block, tx);

        buffer.push(make_event(1)).await.unwrap();
        buffer.push(make_event(2)).await.unwrap();

        assert_eq!(rx.recv().await.unwrap().sequence.0, 1);
        assert_eq!(rx.recv().await.unwrap().sequence.0, 2);
        assert_eq!(buffer.dropped_count(), 0);
    }

    #[tokio::test]
    async fn drop_newest_sheds_load_instead_of_growing() {
        let (tx, _rx) = mpsc::channel(1);
        let buffer = EventBuffer::new(1, OverflowPolicy::DropNewest, tx);

        buffer.push(make_event(1)).await.unwrap();
        buffer.push(make_event(2)).await.unwrap();
        buffer.push(make_event(3)).await.unwrap();

        assert_eq!(buffer.dropped_count(), 2);
        assert_eq!(buffer.push_count(), 3);
    }

    #[tokio::test]
    async fn utilization_reports_actual_fill_not_drop_rate() {
        let (tx, mut rx) = mpsc::channel(4);
        let buffer = EventBuffer::new(4, OverflowPolicy::Block, tx);

        assert_eq!(buffer.utilization(), 0.0);

        buffer.push(make_event(1)).await.unwrap();
        buffer.push(make_event(2)).await.unwrap();
        assert_eq!(buffer.utilization(), 0.5);
        assert_eq!(buffer.available(), 2);
        assert_eq!(buffer.capacity(), 4);
        assert_eq!(buffer.dropped_count(), 0, "nothing was dropped");

        rx.recv().await.unwrap();
        assert_eq!(buffer.utilization(), 0.25);
    }

    #[tokio::test]
    async fn blocking_policy_never_drops_and_waits_for_room() {
        let (tx, mut rx) = mpsc::channel(2);
        let buffer = std::sync::Arc::new(EventBuffer::new(2, OverflowPolicy::Block, tx));

        let writer = {
            let buffer = std::sync::Arc::clone(&buffer);
            tokio::spawn(async move {
                for seq in 1..=10 {
                    buffer.push(make_event(seq)).await.unwrap();
                }
            })
        };

        let mut received = 0;
        while received < 10 {
            rx.recv().await.unwrap();
            received += 1;
        }

        writer.await.unwrap();
        assert_eq!(buffer.dropped_count(), 0, "blocking policy must not drop");
        assert_eq!(buffer.push_count(), 10);
    }

    #[tokio::test]
    async fn a_closed_receiver_is_an_error_not_a_silent_drop() {
        let (tx, rx) = mpsc::channel(2);
        drop(rx);
        let buffer = EventBuffer::new(2, OverflowPolicy::Block, tx);
        assert!(matches!(
            buffer.push(make_event(1)).await,
            Err(Error::ChannelClosed)
        ));
    }
}
