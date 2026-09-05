//! The bounded buffer between the source and the processor.
//!
//! This is where backpressure lives. The buffer is a bounded channel and nothing
//! is held outside it, so the ingester's memory is capped by the capacity
//! regardless of how far behind the processor falls.
//!
//! The sending half is held in an `Option` so it can be released on demand.
//! Downstream, the processor's loop ends when the channel closes, and the
//! channel closes when the last sender goes away. Leaving that to `Drop` makes
//! clean shutdown depend on nobody else holding a handle to the ingester —
//! which is exactly the kind of invariant that silently turns into a hang the
//! first time something reasonable, like a metrics sampler, keeps a reference.
//! `close()` makes it explicit instead.

use parking_lot::Mutex;
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
    capacity: usize,
    sender: Mutex<Option<mpsc::Sender<RawEvent>>>,
    dropped: AtomicU64,
    pushed: AtomicU64,
    blocked_waits: AtomicU64,
}

impl EventBuffer {
    /// Create a buffer feeding `sender`.
    ///
    /// The channel's own capacity is authoritative; `capacity` is only the
    /// fallback used for reporting once the sender has been released.
    pub fn new(capacity: usize, policy: OverflowPolicy, sender: mpsc::Sender<RawEvent>) -> Self {
        let capacity = sender.max_capacity().max(capacity);
        Self {
            policy,
            capacity,
            sender: Mutex::new(Some(sender)),
            dropped: AtomicU64::new(0),
            pushed: AtomicU64::new(0),
            blocked_waits: AtomicU64::new(0),
        }
    }

    /// Release the sending half, closing the channel for the receiver.
    ///
    /// A push already in flight holds its own clone, so it finishes normally and
    /// the channel closes once it lands. Pushes after this point report
    /// `ChannelClosed` rather than blocking forever.
    pub fn close(&self) {
        let _ = self.sender.lock().take();
    }

    /// Whether the sending half is still held.
    pub fn is_open(&self) -> bool {
        self.sender.lock().is_some()
    }

    /// A cheap clone of the sender, if the buffer is still open.
    ///
    /// Held only for the duration of a single push, so it cannot keep the
    /// channel alive past a `close()` by more than one in-flight event.
    fn sender(&self) -> Result<mpsc::Sender<RawEvent>> {
        self.sender.lock().clone().ok_or(Error::ChannelClosed)
    }

    /// Push an event, applying the overflow policy.
    ///
    /// Returns whether the event was accepted. A shedding policy reports `false`
    /// rather than an error — dropping is the configured behaviour, not a
    /// failure — but the caller needs to know, or its "emitted" count silently
    /// becomes "attempted" and the two disagree with the database.
    pub async fn push(&self, event: RawEvent) -> Result<bool> {
        let sender = self.sender()?;
        self.pushed.fetch_add(1, Ordering::Relaxed);

        match self.policy {
            OverflowPolicy::DropOldest | OverflowPolicy::DropNewest => {
                match sender.try_send(event) {
                    Ok(()) => Ok(true),
                    Err(mpsc::error::TrySendError::Full(event)) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            slot = event.slot,
                            seq = event.sequence.0,
                            policy = ?self.policy,
                            "buffer full, dropping event"
                        );
                        metrics::counter!("ingester.buffer_dropped").increment(1);
                        Ok(false)
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::ChannelClosed),
                }
            }
            OverflowPolicy::Block => {
                // Count the times we actually had to wait, which is the honest
                // signal that backpressure is engaging.
                if sender.capacity() == 0 {
                    self.blocked_waits.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!("ingester.buffer_blocked").increment(1);
                }
                sender
                    .send(event)
                    .await
                    .map(|()| true)
                    .map_err(|_| Error::ChannelClosed)
            }
        }
    }

    /// How full the buffer is, from 0.0 to 1.0.
    pub fn utilization(&self) -> f64 {
        let max = self.capacity();
        if max == 0 {
            return 0.0;
        }
        let free = self.available();
        (max - free) as f64 / max as f64
    }

    /// Slots currently free.
    ///
    /// A closed buffer reports itself as empty rather than full: there is no
    /// producer left, so "how far behind is the consumer" has no meaning.
    pub fn available(&self) -> usize {
        match self.sender.lock().as_ref() {
            Some(sender) => sender.capacity(),
            None => self.capacity,
        }
    }

    /// Total capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
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

        assert!(buffer.push(make_event(1)).await.unwrap());
        assert!(buffer.push(make_event(2)).await.unwrap());

        assert_eq!(rx.recv().await.unwrap().sequence.0, 1);
        assert_eq!(rx.recv().await.unwrap().sequence.0, 2);
        assert_eq!(buffer.dropped_count(), 0);
    }

    #[tokio::test]
    async fn drop_newest_sheds_load_instead_of_growing() {
        let (tx, _rx) = mpsc::channel(1);
        let buffer = EventBuffer::new(1, OverflowPolicy::DropNewest, tx);

        assert!(buffer.push(make_event(1)).await.unwrap(), "the first fits");
        assert!(
            !buffer.push(make_event(2)).await.unwrap(),
            "the rest are shed"
        );
        assert!(!buffer.push(make_event(3)).await.unwrap());

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
    async fn closing_the_buffer_ends_the_stream_for_the_receiver() {
        let (tx, mut rx) = mpsc::channel(4);
        let buffer = EventBuffer::new(4, OverflowPolicy::Block, tx);

        buffer.push(make_event(1)).await.unwrap();
        assert!(buffer.is_open());

        buffer.close();

        assert!(!buffer.is_open());
        assert_eq!(
            rx.recv().await.unwrap().sequence.0,
            1,
            "queued work survives"
        );
        assert!(rx.recv().await.is_none(), "then the channel is closed");
        assert!(matches!(
            buffer.push(make_event(2)).await,
            Err(Error::ChannelClosed)
        ));
        assert_eq!(buffer.capacity(), 4, "capacity is still reportable");
    }

    #[tokio::test]
    async fn closing_releases_a_receiver_even_while_a_handle_is_held() {
        // The point of close(): the channel must end without relying on every
        // holder of the buffer dropping their reference first.
        let (tx, mut rx) = mpsc::channel(4);
        let buffer = std::sync::Arc::new(EventBuffer::new(4, OverflowPolicy::Block, tx));
        let observer = std::sync::Arc::clone(&buffer);

        buffer.close();

        assert!(rx.recv().await.is_none());
        assert_eq!(observer.push_count(), 0);
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
