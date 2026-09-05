//! Event writer with idempotent upserts.

use slot_stream_common::{Error, IndexedEvent, Result};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tracing::debug;

/// Result of a write operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteResult {
    /// A new row was inserted.
    Inserted,
    /// The row already existed and was refreshed in place.
    Updated,
}

/// Owns the event upsert.
///
/// The statement and the parameter binding live here and nowhere else. The batch
/// commit in the crate root runs inside a transaction it also uses for slots and
/// the cursor, so it calls [`EventWriter::write_in_tx`] rather than restating the
/// query — two copies of a twelve-parameter bind is two things to keep in step.
///
/// # Idempotency
///
/// Rows are keyed on `(slot, source_seq)`, which is stable for a given stream
/// event, so replaying one updates the existing row instead of inserting a
/// second copy. That is what makes crash recovery safe to overlap.
///
/// The conflict branch resets `is_valid` and clears `invalidated_at`. This is not
/// incidental: after a reorg has soft-deleted a slot, the canonical chain may
/// re-deliver the very same events, and without the reset those rows would stay
/// invisible forever and the persisted state would no longer match a replay of
/// the canonical chain.
pub struct EventWriter {
    pool: PgPool,
}

const UPSERT_SQL: &str = r#"
INSERT INTO events (
    id, seq, source_seq, slot, parent_slot, kind,
    data, event_hash, received_at, indexed_at, is_valid, invalidated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true, NULL)
-- Note what is absent: seq is not updated. It is the order readers page
-- through, so a redelivery must not move an existing row to a new position
-- and slip past a reader's cursor. The row keeps the place it was first
-- given; only its content and validity are refreshed.
ON CONFLICT (slot, source_seq) DO UPDATE SET
    data           = EXCLUDED.data,
    parent_slot    = EXCLUDED.parent_slot,
    event_hash     = EXCLUDED.event_hash,
    -- Bring a previously orphaned row back. Without this a re-applied
    -- canonical event stays invalid and the table diverges from a replay.
    is_valid       = true,
    invalidated_at = NULL,
    updated_at     = NOW()
RETURNING (xmax = 0) AS inserted
"#;

impl EventWriter {
    /// Create a new event writer.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Write a single event.
    pub async fn write(&self, event: &IndexedEvent) -> Result<WriteResult> {
        let row = Self::bind(sqlx::query(UPSERT_SQL), event)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        let inserted: bool = row.get("inserted");

        debug!(
            id = %event.id,
            slot = event.slot,
            seq = event.seq.0,
            source_seq = event.source_seq.0,
            inserted,
            "event written"
        );

        Ok(if inserted {
            WriteResult::Inserted
        } else {
            WriteResult::Updated
        })
    }

    /// Write one event inside a caller's transaction.
    ///
    /// The batch commit path uses this: its transaction also carries the slot
    /// rows and the cursor, so it cannot be handed one that commits itself.
    pub async fn write_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        event: &IndexedEvent,
    ) -> Result<WriteResult> {
        let row = Self::bind(sqlx::query(UPSERT_SQL), event)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(if row.get::<bool, _>("inserted") {
            WriteResult::Inserted
        } else {
            WriteResult::Updated
        })
    }

    fn bind<'q>(
        query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
        event: &'q IndexedEvent,
    ) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
        query
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
    }
}

/// How a batch write landed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchOutcome {
    /// Rows that did not previously exist.
    pub inserted: u64,
    /// Rows that already existed and were refreshed (replay, or reorg re-apply).
    pub updated: u64,
}

impl BatchOutcome {
    /// Total rows touched.
    pub fn total(&self) -> u64 {
        self.inserted + self.updated
    }

    /// Fold one write's result in.
    pub fn record(&mut self, result: WriteResult) {
        match result {
            WriteResult::Inserted => self.inserted += 1,
            WriteResult::Updated => self.updated += 1,
        }
    }
}
