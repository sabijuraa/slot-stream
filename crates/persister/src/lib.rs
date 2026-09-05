//! # slot-stream-persister
//!
//! Postgres persistence: idempotent writes, reorg rollback, and the commit
//! cursor that makes restarts safe.
//!
//! ## Why writes and rollbacks share one channel
//!
//! The persister consumes a single stream of [`PersistCommand`]s. It would be
//! natural to give rollbacks their own channel — they are rarer and more urgent —
//! but that is exactly the bug. A rollback and the events of the branch that
//! replaces it are ordered with respect to each other, and two channels have no
//! order between them. The rollback could then land *after* the new branch's
//! writes and invalidate them, leaving the database showing neither chain.
//!
//! So: one channel, and a flush of everything pending before any rollback runs.
//!
//! ## Idempotency and the commit cursor
//!
//! Rows are keyed `(slot, source_seq)` and the cursor is written in the same
//! transaction as the batch it describes. The cursor therefore never points past
//! durable data, and re-delivering events at or below it is a no-op rather than a
//! duplicate. Together that gives an exactly-once *effect* on persisted state
//! over an at-least-once stream.

pub mod cursor;
pub mod pool;
pub mod rollback;
pub mod writer;

use slot_stream_common::{
    Error, IndexedEvent, Result, RollbackPlan, SequenceNumber, SlotInfo, SlotStatus,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, instrument, warn};

pub use cursor::{Cursor, CursorManager, LIVE_CURSOR};
pub use pool::PoolConfig;
pub use rollback::{ReorgRecord, RollbackExecutor, RollbackOutcome};
pub use writer::{BatchOutcome, EventWriter, WriteResult};

/// Run the embedded migrations against `pool`.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await
        .map_err(|e| Error::DatabaseQuery(format!("migration failed: {e}")))?;
    Ok(())
}

/// One unit of work for the persister, in stream order.
#[derive(Debug)]
pub enum PersistCommand {
    /// Persist an event.
    Event(Box<IndexedEvent>),
    /// Record chain structure for a slot.
    Slot(Box<SlotInfo>),
    /// Undo the orphaned slots of a fork. Flushes pending writes first.
    Rollback(Box<RollbackPlan>),
    /// Force a flush and commit, then signal. Used at shutdown and by tests.
    Flush(oneshot::Sender<Result<()>>),
}

impl PersistCommand {
    /// Wrap an event.
    pub fn event(event: IndexedEvent) -> Self {
        Self::Event(Box::new(event))
    }
    /// Wrap a slot.
    pub fn slot(info: SlotInfo) -> Self {
        Self::Slot(Box::new(info))
    }
    /// Wrap a rollback plan.
    pub fn rollback(plan: RollbackPlan) -> Self {
        Self::Rollback(Box::new(plan))
    }
}

/// Statistics about persistence operations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PersisterStats {
    pub events_written: u64,
    pub events_updated: u64,
    pub batches_written: u64,
    pub slots_written: u64,
    pub rollbacks_executed: u64,
    pub events_invalidated: u64,
    pub write_errors: u64,
}

/// Where the pipeline should pick up after a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeState {
    /// The committed cursor, if the pipeline has run before.
    pub cursor: Option<Cursor>,
    /// Highest assigned sequence in the events table.
    pub max_seq: SequenceNumber,
    /// Canonical slots still retained, ascending, with their parents.
    pub canonical_slots: Vec<(u64, u64)>,
}

impl ResumeState {
    /// The stream sequence to resume from; everything at or below is durable.
    pub fn resume_source_seq(&self) -> SequenceNumber {
        self.cursor
            .as_ref()
            .map(|c| c.source_seq)
            .unwrap_or(SequenceNumber::ZERO)
    }
}

/// How the persister batches and commits.
#[derive(Debug, Clone)]
pub struct PersisterConfig {
    /// Flush once this many events are pending.
    pub batch_size: usize,
    /// Flush after this long even if the batch is not full.
    pub batch_timeout: Duration,
    /// Cursor to commit against.
    pub cursor_name: String,
}

impl Default for PersisterConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            batch_timeout: Duration::from_millis(100),
            cursor_name: LIVE_CURSOR.to_string(),
        }
    }
}

/// Writes events, slots, and rollbacks to Postgres.
pub struct Persister {
    pool: PgPool,
    writer: EventWriter,
    cursor_manager: CursorManager,
    rollback_executor: RollbackExecutor,
    config: PersisterConfig,
    stats: Arc<parking_lot::Mutex<PersisterStats>>,
}

impl Persister {
    /// Create a new persister.
    pub fn new(pool: PgPool, config: PersisterConfig) -> Self {
        Self {
            writer: EventWriter::new(pool.clone()),
            cursor_manager: CursorManager::new(pool.clone()),
            rollback_executor: RollbackExecutor::new(pool.clone()),
            pool,
            config,
            stats: Arc::new(parking_lot::Mutex::new(PersisterStats::default())),
        }
    }

    /// The underlying pool, for readers that share it.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Statistics snapshot.
    pub fn stats(&self) -> PersisterStats {
        self.stats.lock().clone()
    }

    /// Rollback history, most recent first.
    pub async fn reorg_history(&self, limit: i64) -> Result<Vec<ReorgRecord>> {
        self.rollback_executor.history(limit).await
    }

    /// Read everything needed to resume after a restart.
    #[instrument(skip(self))]
    pub async fn resume_state(&self) -> Result<ResumeState> {
        let cursor = self.cursor_manager.get(&self.config.cursor_name).await?;

        let row = sqlx::query("SELECT COALESCE(MAX(seq), 0) AS max_seq FROM events")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
        let max_seq = SequenceNumber(row.get::<i64, _>("max_seq") as u64);

        let rows = sqlx::query(
            "SELECT slot, parent_slot FROM slots WHERE is_canonical = true ORDER BY slot ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        let canonical_slots = rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<i64, _>("slot") as u64,
                    r.get::<i64, _>("parent_slot") as u64,
                )
            })
            .collect();

        let state = ResumeState {
            cursor,
            max_seq,
            canonical_slots,
        };

        info!(
            resume_source_seq = state.resume_source_seq().0,
            max_seq = state.max_seq.0,
            canonical_slots = state.canonical_slots.len(),
            "resume state loaded"
        );

        Ok(state)
    }

    /// Write a single event. Mostly for tests and the DLQ replay path.
    pub async fn write(&self, event: &IndexedEvent) -> Result<WriteResult> {
        let result = self.writer.write(event).await?;
        let mut stats = self.stats.lock();
        match result {
            WriteResult::Inserted => stats.events_written += 1,
            WriteResult::Updated => stats.events_updated += 1,
        }
        Ok(result)
    }

    /// Execute a rollback plan directly.
    pub async fn rollback(&self, plan: &RollbackPlan) -> Result<RollbackOutcome> {
        let outcome = self.rollback_executor.execute(plan).await?;
        let mut stats = self.stats.lock();
        stats.rollbacks_executed += 1;
        stats.events_invalidated += outcome.events_invalidated;
        Ok(outcome)
    }

    /// Consume commands until the channel closes, then flush and return.
    ///
    /// This is the only writer to the events table in normal operation, which is
    /// what lets ordering be reasoned about at all.
    pub async fn run(&self, mut rx: mpsc::Receiver<PersistCommand>) -> Result<()> {
        info!(
            batch_size = self.config.batch_size,
            batch_timeout_ms = self.config.batch_timeout.as_millis(),
            "persister started"
        );

        let mut pending = PendingBatch::default();
        let mut ticker = tokio::time::interval(self.config.batch_timeout);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                command = rx.recv() => {
                    match command {
                        Some(PersistCommand::Event(event)) => {
                            pending.push_event(*event);
                            if pending.events.len() >= self.config.batch_size {
                                self.flush(&mut pending).await?;
                            }
                        }
                        Some(PersistCommand::Slot(info)) => {
                            pending.push_slot(*info);
                        }
                        Some(PersistCommand::Rollback(plan)) => {
                            // Everything queued belongs to the branch being
                            // abandoned, so it must land before we invalidate it.
                            // Flushing here is what keeps the two orderable.
                            self.flush(&mut pending).await?;
                            self.rollback(&plan).await?;
                        }
                        Some(PersistCommand::Flush(ack)) => {
                            let result = self.flush(&mut pending).await;
                            let _ = ack.send(result);
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    if !pending.is_empty() {
                        self.flush(&mut pending).await?;
                    }
                }
            }
        }

        self.flush(&mut pending).await?;
        info!(stats = ?self.stats(), "persister stopped");
        Ok(())
    }

    /// Commit the pending batch and its cursor in one transaction.
    async fn flush(&self, pending: &mut PendingBatch) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }

        let events = std::mem::take(&mut pending.events);
        let slots = std::mem::take(&mut pending.slots);
        let watermark = pending.take_watermark();

        match self.commit(&events, &slots, watermark).await {
            Ok(outcome) => {
                let mut stats = self.stats.lock();
                stats.events_written += outcome.inserted;
                stats.events_updated += outcome.updated;
                stats.slots_written += slots.len() as u64;
                stats.batches_written += 1;
                Ok(())
            }
            Err(e) => {
                self.stats.lock().write_errors += 1;
                error!(error = %e, events = events.len(), "batch commit failed");
                Err(e)
            }
        }
    }

    async fn commit(
        &self,
        events: &[IndexedEvent],
        slots: &[SlotInfo],
        watermark: Option<(u64, SequenceNumber, SequenceNumber)>,
    ) -> Result<BatchOutcome> {
        let mut tx: Transaction<'_, Postgres> = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::DatabaseConnection(e.to_string()))?;

        for info in slots {
            Self::upsert_slot(&mut tx, info).await?;
        }

        let mut outcome = BatchOutcome::default();
        for event in events {
            let row = sqlx::query(writer::UPSERT_SQL_PUB)
                .bind(event.id)
                .bind(event.seq.0 as i64)
                .bind(event.source_seq.0 as i64)
                .bind(event.slot as i64)
                .bind(event.parent_slot.map(|s| s as i64))
                .bind(event.kind.as_str())
                .bind(&event.data)
                .bind(&event.event_hash)
                .bind(event.received_at)
                .bind(event.indexed_at)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

            if row.get::<bool, _>("inserted") {
                outcome.inserted += 1;
            } else {
                outcome.updated += 1;
            }
        }

        // The cursor rides the same transaction as the data it describes. If the
        // commit fails, both are absent; there is no window where the cursor
        // claims durability the events do not have.
        if let Some((slot, seq, source_seq)) = watermark {
            let cursor = Cursor::new(&self.config.cursor_name, slot, seq, source_seq);
            CursorManager::update_in_tx(&mut tx, &cursor).await?;
        }

        tx.commit()
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(outcome)
    }

    async fn upsert_slot(tx: &mut Transaction<'_, Postgres>, info: &SlotInfo) -> Result<()> {
        let status = format!("{:?}", info.status);
        let is_canonical = info.status != SlotStatus::Orphaned;

        sqlx::query(
            r#"
            INSERT INTO slots (
                slot, parent_slot, block_hash, status, is_canonical,
                block_time, transaction_count, received_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (slot) DO UPDATE SET
                parent_slot       = EXCLUDED.parent_slot,
                block_hash        = COALESCE(EXCLUDED.block_hash, slots.block_hash),
                status            = EXCLUDED.status,
                is_canonical      = EXCLUDED.is_canonical,
                block_time        = COALESCE(EXCLUDED.block_time, slots.block_time),
                transaction_count = EXCLUDED.transaction_count,
                updated_at        = NOW()
            "#,
        )
        .bind(info.slot as i64)
        .bind(info.parent_slot as i64)
        .bind(&info.block_hash)
        .bind(status)
        .bind(is_canonical)
        .bind(info.block_time)
        .bind(info.transaction_count as i32)
        .bind(info.received_at)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(())
    }
}

/// Work accumulated since the last flush.
#[derive(Debug, Default)]
struct PendingBatch {
    events: Vec<IndexedEvent>,
    slots: Vec<SlotInfo>,
    watermark: Option<(u64, SequenceNumber, SequenceNumber)>,
}

impl PendingBatch {
    fn push_event(&mut self, event: IndexedEvent) {
        let mark = (event.slot, event.seq, event.source_seq);
        self.watermark = Some(match self.watermark {
            None => mark,
            Some((slot, seq, source)) => (
                slot.max(mark.0),
                seq.max(mark.1),
                source.max(mark.2),
            ),
        });
        self.events.push(event);
    }

    fn push_slot(&mut self, info: SlotInfo) {
        self.slots.push(info);
    }

    fn take_watermark(&mut self) -> Option<(u64, SequenceNumber, SequenceNumber)> {
        self.watermark.take()
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.slots.is_empty()
    }
}

/// Warn loudly if a plan reaches a bounded divergence; callers may want backfill.
pub fn warn_on_bounded_divergence(plan: &RollbackPlan) {
    if plan.divergence_is_bound {
        warn!(
            fork_slot = plan.fork_slot,
            divergence_point = plan.divergence_point,
            "fork resolved against a bounded divergence point"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slot_stream_common::{EventKind, RawEvent};

    fn event(slot: u64, seq: u64, source: u64) -> IndexedEvent {
        let raw = RawEvent::new(
            SequenceNumber(source),
            EventKind::Transaction,
            slot,
            bytes::Bytes::from_static(b"x"),
        );
        IndexedEvent::from_raw(raw, serde_json::json!({}), SequenceNumber(seq))
    }

    #[test]
    fn pending_batch_watermark_tracks_the_maximum() {
        let mut pending = PendingBatch::default();
        pending.push_event(event(10, 5, 7));
        pending.push_event(event(12, 9, 11));
        pending.push_event(event(11, 6, 8));

        assert_eq!(
            pending.take_watermark(),
            Some((12, SequenceNumber(9), SequenceNumber(11)))
        );
    }

    #[test]
    fn pending_batch_is_empty_until_something_is_pushed() {
        let mut pending = PendingBatch::default();
        assert!(pending.is_empty());
        pending.push_slot(SlotInfo::new(1, 0));
        assert!(!pending.is_empty());
    }
}
