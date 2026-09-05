//! # slot-stream-ingester
//!
//! Consumes a stream source and hands events to the pipeline through a bounded
//! buffer.
//!
//! ## Backpressure
//!
//! The buffer between the source and the processor is bounded, and the policy for
//! what happens when it fills is a configuration choice rather than an accident:
//!
//! - `Block` applies real backpressure. The ingester stops reading the source,
//!   which for a gRPC stream means the flow-control window closes and the server
//!   slows down. Nothing is lost and memory stays flat. This is the default for
//!   an indexer, where a gap is worse than a delay.
//! - `DropNewest` / `DropOldest` keep reading and shed load. Appropriate when
//!   freshness beats completeness, which for a chain indexer it does not.
//!
//! Either way memory is bounded by the channel capacity, because the ingester
//! never buffers anything outside it.
//!
//! ## Sequence tracking
//!
//! Gaps and duplicates are detected as events arrive, not after they are written,
//! so a gap can be queued for backfill immediately. A duplicate is dropped here
//! rather than downstream; the persister would also handle it, but doing it early
//! keeps it out of the batch.

pub mod buffer;
pub mod config;
pub mod grpc;
pub mod scripted;
pub mod source;

use slot_stream_common::{
    Error, RawEvent, Result, SequenceNumber, SequenceRange, SequenceResult, SequenceTracker,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

pub use buffer::{EventBuffer, OverflowPolicy};
pub use config::IngesterConfig;
pub use grpc::{ChainSourceServer, GrpcEventSource};
pub use scripted::{ChainScript, ScriptedEvent, ScriptedSlot, ScriptedSource};
pub use source::EventSource;

/// Statistics about ingestion.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IngesterStats {
    pub events_received: u64,
    pub events_emitted: u64,
    pub events_dropped: u64,
    pub duplicates_skipped: u64,
    pub bytes_received: u64,
    pub reconnects: u64,
    pub sequence_gaps: u64,
    pub last_sequence: Option<SequenceNumber>,
}

/// Reads a source into a bounded buffer.
pub struct Ingester {
    config: IngesterConfig,
    buffer: Arc<EventBuffer>,
    sequence_tracker: Arc<parking_lot::Mutex<SequenceTracker>>,
    stats: Arc<parking_lot::Mutex<IngesterStats>>,
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl Ingester {
    /// Create an ingester and the receiver the processor reads from.
    pub fn new(config: IngesterConfig) -> (Self, mpsc::Receiver<RawEvent>) {
        Self::build(config, None)
    }

    /// Create an ingester resuming from a known stream position.
    pub fn from_checkpoint(
        config: IngesterConfig,
        last_sequence: SequenceNumber,
    ) -> (Self, mpsc::Receiver<RawEvent>) {
        Self::build(config, Some(last_sequence))
    }

    fn build(
        config: IngesterConfig,
        checkpoint: Option<SequenceNumber>,
    ) -> (Self, mpsc::Receiver<RawEvent>) {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let (shutdown, _) = tokio::sync::broadcast::channel(1);

        let tracker = match checkpoint {
            Some(seq) => SequenceTracker::from_checkpoint(seq),
            None => SequenceTracker::new(),
        };

        let ingester = Self {
            buffer: Arc::new(EventBuffer::new(
                config.channel_capacity,
                config.overflow_policy,
                tx,
            )),
            sequence_tracker: Arc::new(parking_lot::Mutex::new(tracker)),
            stats: Arc::new(parking_lot::Mutex::new(IngesterStats {
                last_sequence: checkpoint,
                ..Default::default()
            })),
            shutdown,
            config,
        };

        (ingester, rx)
    }

    /// Statistics snapshot.
    pub fn stats(&self) -> IngesterStats {
        let mut stats = self.stats.lock().clone();
        stats.events_dropped = self.buffer.dropped_count();
        stats
    }

    /// Pending sequence gaps that warrant a backfill.
    pub fn pending_gaps(&self) -> Vec<SequenceRange> {
        self.sequence_tracker.lock().pending_gaps().to_vec()
    }

    /// Whether any gaps are outstanding.
    pub fn has_gaps(&self) -> bool {
        self.sequence_tracker.lock().has_gaps()
    }

    /// Ask the ingester to stop after the current event.
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(());
    }

    /// Read `source` until it is exhausted or shutdown is requested.
    ///
    /// A resumable source that errors is reconnected with exponential backoff,
    /// resuming from the last sequence actually accepted, so a reconnect does not
    /// open a gap.
    #[instrument(skip_all, fields(source = %source.describe()))]
    pub async fn run<S: EventSource>(&self, mut source: S) -> Result<()> {
        info!(
            overflow_policy = ?self.config.overflow_policy,
            channel_capacity = self.config.channel_capacity,
            "ingester started"
        );

        let mut shutdown_rx = self.shutdown.subscribe();
        let mut backoff = self.config.initial_backoff;
        let mut attempts = 0u32;

        loop {
            let event = tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("shutdown requested");
                    break;
                }
                event = source.next_event() => event,
            };

            match event {
                Some(Ok(event)) => {
                    backoff = self.config.initial_backoff;
                    attempts = 0;
                    self.handle_event(event).await?;
                }
                Some(Err(e)) => {
                    if !source.is_resumable() {
                        warn!(error = %e, "source failed and cannot be resumed");
                        return Err(e);
                    }

                    attempts += 1;
                    if attempts > self.config.max_reconnect_attempts {
                        warn!(error = %e, attempts, "giving up on reconnection");
                        return Err(e);
                    }

                    warn!(error = %e, attempts, backoff_ms = backoff.as_millis(), "stream error");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);

                    let resume_from = self
                        .sequence_tracker
                        .lock()
                        .last_seen()
                        .map(|s| s.0)
                        .unwrap_or(0);

                    if let Err(reconnect_err) = source.reconnect(resume_from).await {
                        warn!(error = %reconnect_err, "reconnect failed, will retry");
                        continue;
                    }

                    self.stats.lock().reconnects += 1;
                }
                None => {
                    info!("source exhausted");
                    break;
                }
            }
        }

        info!(stats = ?self.stats(), "ingester stopped");
        Ok(())
    }

    /// Track and forward one event.
    async fn handle_event(&self, event: RawEvent) -> Result<()> {
        let sequence_result = {
            let mut tracker = self.sequence_tracker.lock();
            match tracker.process(event.sequence) {
                Ok(result) => result,
                Err(Error::SequenceRegression { current, received }) => {
                    // A source that rewinds is normal after a reconnect: it
                    // replays from our resume point and some of it overlaps.
                    // Downstream writes are idempotent, so forward it.
                    tracing::debug!(current, received, "sequence rewound, forwarding anyway");
                    SequenceResult::Duplicate
                }
                Err(e) => return Err(e),
            }
        };

        {
            let mut stats = self.stats.lock();
            stats.events_received += 1;
            stats.bytes_received += event.payload.len() as u64;
            stats.last_sequence = Some(event.sequence);
        }

        match sequence_result {
            SequenceResult::Duplicate => {
                self.stats.lock().duplicates_skipped += 1;
                metrics::counter!("ingester.duplicates").increment(1);
                return Ok(());
            }
            SequenceResult::Gap { missing } => {
                warn!(
                    start = missing.start.0,
                    end = missing.end.0,
                    "sequence gap detected"
                );
                self.stats.lock().sequence_gaps += 1;
                metrics::counter!("ingester.sequence_gaps").increment(1);
            }
            SequenceResult::Processed => {}
        }

        self.buffer.push(event).await?;
        self.stats.lock().events_emitted += 1;
        metrics::counter!("ingester.events_emitted").increment(1);
        Ok(())
    }
}

/// How long to wait before the first reconnection attempt.
pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripted::{ChainScript, ScriptedSource};

    #[tokio::test]
    async fn ingester_forwards_every_scripted_event() {
        let script = ChainScript::new().extend_from(100, 4, 3);
        let expected = script.event_count();

        let config = IngesterConfig {
            channel_capacity: 256,
            overflow_policy: OverflowPolicy::Block,
            ..Default::default()
        };
        let (ingester, mut rx) = Ingester::new(config);

        let source = ScriptedSource::new(script);
        let collector = tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Some(event) = rx.recv().await {
                seen.push(event);
            }
            seen
        });

        ingester.run(source).await.unwrap();
        let emitted = ingester.stats().events_emitted;
        drop(ingester);

        let seen = collector.await.unwrap();
        assert_eq!(emitted as usize, expected);
        assert_eq!(seen.len(), expected);
    }

    #[tokio::test]
    async fn duplicate_sequences_are_dropped_before_the_buffer() {
        let config = IngesterConfig {
            channel_capacity: 16,
            overflow_policy: OverflowPolicy::Block,
            ..Default::default()
        };
        let (ingester, mut rx) = Ingester::new(config);

        let event = |seq: u64| {
            RawEvent::new(
                SequenceNumber(seq),
                slot_stream_common::EventKind::Transaction,
                10,
                bytes::Bytes::from_static(b"{}"),
            )
            .with_parent(9)
        };

        ingester.handle_event(event(1)).await.unwrap();
        ingester.handle_event(event(1)).await.unwrap();
        ingester.handle_event(event(2)).await.unwrap();

        let stats = ingester.stats();
        assert_eq!(stats.events_received, 3);
        assert_eq!(stats.events_emitted, 2);
        assert_eq!(stats.duplicates_skipped, 1);

        drop(ingester);
        let mut seen = 0;
        while rx.recv().await.is_some() {
            seen += 1;
        }
        assert_eq!(seen, 2);
    }

    #[tokio::test]
    async fn a_gap_is_recorded_but_the_event_still_flows() {
        let config = IngesterConfig {
            channel_capacity: 16,
            overflow_policy: OverflowPolicy::Block,
            ..Default::default()
        };
        let (ingester, _rx) = Ingester::new(config);

        let event = |seq: u64| {
            RawEvent::new(
                SequenceNumber(seq),
                slot_stream_common::EventKind::Transaction,
                10,
                bytes::Bytes::from_static(b"{}"),
            )
        };

        ingester.handle_event(event(1)).await.unwrap();
        ingester.handle_event(event(5)).await.unwrap();

        assert!(ingester.has_gaps());
        let gaps = ingester.pending_gaps();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].start.0, 2);
        assert_eq!(gaps[0].end.0, 4);
        assert_eq!(ingester.stats().events_emitted, 2);
    }
}
