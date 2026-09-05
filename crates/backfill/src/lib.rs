//! # slot-stream-backfill
//!
//! Fetches historical slots over RPC and feeds them into the live pipeline.
//!
//! ## How backfilled data merges with live data
//!
//! Backfilled events are pushed into the same channel the ingester feeds, so they
//! take the identical path: parsing, fork detection, idempotent write. There is
//! no second write path that could drift from the first.
//!
//! Row identity is `(slot, source_seq)`, and RPC has no stream sequence to offer,
//! so a backfilled event cannot dedupe against the live copy of the same event by
//! identity alone. Merging is therefore done at slot granularity: before fetching
//! a slot, the engine asks whether that slot already holds valid events and skips
//! it if so. That is the honest boundary — backfill fills gaps, it does not
//! re-derive slots the live stream already delivered.
//!
//! Within backfill itself, identity is deterministic: `source_seq` is
//! `slot * SLOT_SEQUENCE_STRIDE + transaction_index`, so re-running a backfill
//! over the same range updates rows rather than duplicating them.

pub mod rpc;

use slot_stream_common::{
    Error, EventKind, EventOrigin, RawEvent, Result, SequenceNumber, SequenceRange,
};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

pub use rpc::{BlockData, RpcClient, TransactionData};

/// Sequence numbers reserved per slot for backfilled events.
///
/// Caps a backfilled slot at ~1M transactions, comfortably above any real block,
/// and keeps each slot's range disjoint so backfill is deterministic.
pub const SLOT_SEQUENCE_STRIDE: u64 = 1 << 20;

/// Deterministic source sequence for a backfilled transaction.
pub fn backfill_sequence(slot: u64, index: u32) -> SequenceNumber {
    SequenceNumber(slot.saturating_mul(SLOT_SEQUENCE_STRIDE) + index as u64)
}

/// Status of a backfill run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum BackfillStatus {
    Idle,
    Running,
    Completed,
    Failed,
}

/// Progress of a backfill run.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BackfillStats {
    pub slots_examined: u64,
    pub slots_fetched: u64,
    pub slots_skipped_present: u64,
    pub slots_skipped_empty: u64,
    pub events_emitted: u64,
    pub rpc_errors: u64,
    pub current_slot: Option<u64>,
    pub target_slot: Option<u64>,
}

impl BackfillStats {
    /// Fraction of the current range completed, 0.0 to 1.0.
    pub fn progress(&self) -> f64 {
        match (self.current_slot, self.target_slot) {
            (Some(current), Some(target)) if target > 0 => {
                (current as f64 / target as f64).clamp(0.0, 1.0)
            }
            _ => 0.0,
        }
    }
}

/// A range of slots to backfill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillRange {
    pub start_slot: u64,
    pub end_slot: u64,
    /// Lower runs first.
    pub priority: u32,
    /// Whether this range came from an observed gap rather than a bootstrap.
    pub is_gap_fill: bool,
}

impl BackfillRange {
    /// A normal backfill range.
    pub fn new(start_slot: u64, end_slot: u64) -> Self {
        Self {
            start_slot,
            end_slot,
            priority: 10,
            is_gap_fill: false,
        }
    }

    /// A high-priority range covering an observed gap.
    pub fn gap_fill(start_slot: u64, end_slot: u64) -> Self {
        Self {
            start_slot,
            end_slot,
            priority: 1,
            is_gap_fill: true,
        }
    }

    /// Number of slots covered.
    pub fn len(&self) -> u64 {
        self.end_slot.saturating_sub(self.start_slot) + 1
    }

    /// Whether the range covers nothing.
    pub fn is_empty(&self) -> bool {
        self.start_slot > self.end_slot
    }

    /// Whether a slot falls in the range.
    pub fn contains(&self, slot: u64) -> bool {
        slot >= self.start_slot && slot <= self.end_slot
    }
}

/// Priority queue of ranges awaiting backfill.
#[derive(Debug, Default)]
pub struct BackfillQueue {
    ranges: Vec<BackfillRange>,
}

impl BackfillQueue {
    /// An empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a range.
    pub fn push(&mut self, range: BackfillRange) {
        if range.is_empty() {
            return;
        }
        self.ranges.push(range);
        self.ranges.sort_by_key(|r| (r.priority, r.start_slot));
    }

    /// Take the highest-priority range.
    pub fn pop(&mut self) -> Option<BackfillRange> {
        if self.ranges.is_empty() {
            None
        } else {
            Some(self.ranges.remove(0))
        }
    }

    /// Whether anything is queued.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// How many ranges are queued.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Total slots queued.
    pub fn total_slots(&self) -> u64 {
        self.ranges.iter().map(|r| r.len()).sum()
    }
}

/// How the backfill engine behaves.
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// Slots per `getBlocks` call.
    pub batch_size: u64,
    /// Pause between batches, to stay inside RPC rate limits.
    pub batch_delay: Duration,
    /// Retries per RPC call.
    pub max_retries: u32,
    /// Skip slots that already hold valid events.
    pub skip_present_slots: bool,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_delay: Duration::from_millis(100),
            max_retries: 3,
            skip_present_slots: true,
        }
    }
}

/// Fetches historical slots and feeds them into the pipeline.
pub struct Backfiller {
    client: RpcClient,
    pool: PgPool,
    config: BackfillConfig,
    queue: parking_lot::Mutex<BackfillQueue>,
    status: parking_lot::RwLock<BackfillStatus>,
    stats: parking_lot::RwLock<BackfillStats>,
}

impl Backfiller {
    /// Create a backfiller.
    pub fn new(config: BackfillConfig, rpc_endpoint: &str, pool: PgPool) -> Result<Self> {
        Ok(Self {
            client: RpcClient::new(rpc_endpoint)?.with_retries(config.max_retries),
            pool,
            config,
            queue: parking_lot::Mutex::new(BackfillQueue::new()),
            status: parking_lot::RwLock::new(BackfillStatus::Idle),
            stats: parking_lot::RwLock::new(BackfillStats::default()),
        })
    }

    /// Queue a range.
    pub fn add_range(&self, range: BackfillRange) {
        self.queue.lock().push(range);
    }

    /// Queue a gap observed in the live stream.
    pub fn fill_gap(&self, start_slot: u64, end_slot: u64) {
        self.add_range(BackfillRange::gap_fill(start_slot, end_slot));
    }

    /// Queue the slot ranges implied by sequence gaps.
    ///
    /// Sequence gaps and slot gaps are not the same thing, so this is a
    /// conservative widening: the affected slots are re-examined and any that
    /// already hold data are skipped.
    pub fn queue_sequence_gaps(&self, gaps: &[SequenceRange], slot_hint: u64) {
        for gap in gaps {
            let span = gap.len().min(1_000);
            self.fill_gap(slot_hint.saturating_sub(span), slot_hint);
        }
    }

    /// Current status.
    pub fn status(&self) -> BackfillStatus {
        *self.status.read()
    }

    /// Progress snapshot.
    pub fn stats(&self) -> BackfillStats {
        self.stats.read().clone()
    }

    /// How many ranges are still queued.
    pub fn pending_ranges(&self) -> usize {
        self.queue.lock().len()
    }

    /// Drain the queue, feeding events into `sink`.
    #[instrument(skip_all)]
    pub async fn run(&self, sink: mpsc::Sender<RawEvent>) -> Result<()> {
        *self.status.write() = BackfillStatus::Running;

        while let Some(range) = {
            let mut queue = self.queue.lock();
            queue.pop()
        } {
            info!(
                start = range.start_slot,
                end = range.end_slot,
                gap_fill = range.is_gap_fill,
                "backfilling range"
            );

            if let Err(e) = self.process_range(&range, &sink).await {
                warn!(error = %e, "backfill range failed");
                *self.status.write() = BackfillStatus::Failed;
                return Err(e);
            }
        }

        *self.status.write() = BackfillStatus::Completed;
        info!(stats = ?self.stats(), "backfill complete");
        Ok(())
    }

    /// Backfill one range.
    pub async fn process_range(
        &self,
        range: &BackfillRange,
        sink: &mpsc::Sender<RawEvent>,
    ) -> Result<()> {
        if range.is_empty() {
            return Err(Error::BackfillRange {
                start: range.start_slot,
                end: range.end_slot,
            });
        }

        self.stats.write().target_slot = Some(range.end_slot);
        let mut cursor = range.start_slot;

        while cursor <= range.end_slot {
            let batch_end = (cursor + self.config.batch_size - 1).min(range.end_slot);

            let slots = self.client.get_blocks(cursor, batch_end).await?;
            self.stats.write().slots_examined += batch_end - cursor + 1;

            for slot in slots {
                if self.config.skip_present_slots && self.slot_already_indexed(slot).await? {
                    self.stats.write().slots_skipped_present += 1;
                    continue;
                }

                match self.client.get_block(slot).await? {
                    Some(block) => {
                        let emitted = self.emit_block(&block, sink).await?;
                        let mut stats = self.stats.write();
                        stats.slots_fetched += 1;
                        stats.events_emitted += emitted;
                        stats.current_slot = Some(slot);
                    }
                    None => {
                        self.stats.write().slots_skipped_empty += 1;
                    }
                }
            }

            cursor = batch_end + 1;

            if self.config.batch_delay > Duration::ZERO {
                tokio::time::sleep(self.config.batch_delay).await;
            }
        }

        Ok(())
    }

    /// Whether a slot already holds valid events.
    async fn slot_already_indexed(&self, slot: u64) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS (SELECT 1 FROM events WHERE slot = $1 AND is_valid = true) AS present",
        )
        .bind(slot as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(row.get::<bool, _>("present"))
    }

    /// Turn a block into stream events and push them.
    async fn emit_block(&self, block: &BlockData, sink: &mpsc::Sender<RawEvent>) -> Result<u64> {
        let mut emitted = 0;

        for tx in &block.transactions {
            let payload = serde_json::to_vec(&tx.to_body())
                .map_err(|e| Error::Serialization(e.to_string()))?;

            // Marked as backfill so the processor writes it without reading it
            // as a claim about where the chain currently is.
            let event = RawEvent::new(
                backfill_sequence(block.slot, tx.index),
                EventKind::Transaction,
                block.slot,
                bytes::Bytes::from(payload),
            )
            .with_parent(block.parent_slot)
            .with_origin(EventOrigin::Backfill);

            sink.send(event).await.map_err(|_| Error::ChannelClosed)?;
            emitted += 1;
        }

        Ok(emitted)
    }
}

/// Shared handle to a backfiller.
pub type SharedBackfiller = Arc<Backfiller>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_sequences_are_deterministic_and_disjoint_per_slot() {
        assert_eq!(backfill_sequence(5, 0), backfill_sequence(5, 0));
        assert_ne!(backfill_sequence(5, 0), backfill_sequence(5, 1));

        // Slot 5's highest possible sequence is below slot 6's lowest.
        let slot5_max = backfill_sequence(5, SLOT_SEQUENCE_STRIDE as u32 - 1);
        let slot6_min = backfill_sequence(6, 0);
        assert!(slot5_max < slot6_min);
    }

    #[test]
    fn the_queue_runs_gap_fills_before_bootstrap_ranges() {
        let mut queue = BackfillQueue::new();
        queue.push(BackfillRange::new(1_000, 2_000));
        queue.push(BackfillRange::gap_fill(500, 510));
        queue.push(BackfillRange::new(3_000, 4_000));

        let first = queue.pop().unwrap();
        assert!(first.is_gap_fill);
        assert_eq!(first.start_slot, 500);
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn empty_ranges_are_never_queued() {
        let mut queue = BackfillQueue::new();
        queue.push(BackfillRange::new(100, 50));
        assert!(queue.is_empty());
    }

    #[test]
    fn range_arithmetic() {
        let range = BackfillRange::new(100, 104);
        assert_eq!(range.len(), 5);
        assert!(range.contains(100));
        assert!(range.contains(104));
        assert!(!range.contains(105));
        assert!(!range.is_empty());
    }

    #[test]
    fn progress_is_bounded_to_the_unit_interval() {
        let stats = BackfillStats {
            current_slot: Some(500),
            target_slot: Some(1_000),
            ..Default::default()
        };
        assert!((stats.progress() - 0.5).abs() < 1e-9);

        let overshoot = BackfillStats {
            current_slot: Some(2_000),
            target_slot: Some(1_000),
            ..Default::default()
        };
        assert_eq!(overshoot.progress(), 1.0);
    }
}
