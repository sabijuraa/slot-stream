//! Cursor management: where the pipeline has committed up to.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use slot_stream_common::{Error, Result, SequenceNumber};
use sqlx::{PgPool, Postgres, Row, Transaction};

/// The name of the cursor the live pipeline commits against.
pub const LIVE_CURSOR: &str = "live";

/// A committed position in the stream.
///
/// The cursor is written in the same transaction as the batch it describes, so
/// it can never point past data that is not durable. On restart the pipeline
/// resumes from `source_seq`; anything at or below it has already landed, and
/// because writes are idempotent, re-delivering some of it is harmless.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// Cursor name (e.g. "live", "backfill").
    pub name: String,

    /// Highest slot committed.
    pub slot: u64,

    /// Highest assigned sequence committed.
    pub seq: SequenceNumber,

    /// Highest stream sequence committed. This is the resume point.
    pub source_seq: SequenceNumber,

    /// When the cursor was last updated.
    pub updated_at: DateTime<Utc>,

    /// Additional metadata.
    pub metadata: Option<serde_json::Value>,
}

impl Cursor {
    /// Create a new cursor.
    pub fn new(
        name: impl Into<String>,
        slot: u64,
        seq: SequenceNumber,
        source_seq: SequenceNumber,
    ) -> Self {
        Self {
            name: name.into(),
            slot,
            seq,
            source_seq,
            updated_at: Utc::now(),
            metadata: None,
        }
    }

    /// A cursor positioned before anything has been committed.
    pub fn start(name: impl Into<String>) -> Self {
        Self::new(name, 0, SequenceNumber::ZERO, SequenceNumber::ZERO)
    }

    /// Move the cursor forward. Never moves it backwards.
    pub fn advance(&mut self, slot: u64, seq: SequenceNumber, source_seq: SequenceNumber) {
        self.slot = self.slot.max(slot);
        self.seq = self.seq.max(seq);
        self.source_seq = self.source_seq.max(source_seq);
        self.updated_at = Utc::now();
    }

    /// Set metadata on the cursor.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Reads and writes cursors.
pub struct CursorManager {
    pool: PgPool,
}

impl CursorManager {
    /// Create a new cursor manager.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get a cursor by name.
    pub async fn get(&self, name: &str) -> Result<Option<Cursor>> {
        let row = sqlx::query(
            "SELECT name, slot, seq, source_seq, updated_at, metadata FROM cursors WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(row.map(Self::row_to_cursor))
    }

    /// Insert or update a cursor.
    pub async fn update(&self, cursor: &Cursor) -> Result<()> {
        Self::upsert(&self.pool, cursor).await
    }

    /// Insert or update a cursor inside an existing transaction.
    ///
    /// Used by the persister so the cursor commits atomically with its batch.
    pub async fn update_in_tx(tx: &mut Transaction<'_, Postgres>, cursor: &Cursor) -> Result<()> {
        sqlx::query(CURSOR_UPSERT)
            .bind(&cursor.name)
            .bind(cursor.slot as i64)
            .bind(cursor.seq.0 as i64)
            .bind(cursor.source_seq.0 as i64)
            .bind(cursor.updated_at)
            .bind(&cursor.metadata)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
        Ok(())
    }

    async fn upsert(pool: &PgPool, cursor: &Cursor) -> Result<()> {
        sqlx::query(CURSOR_UPSERT)
            .bind(&cursor.name)
            .bind(cursor.slot as i64)
            .bind(cursor.seq.0 as i64)
            .bind(cursor.source_seq.0 as i64)
            .bind(cursor.updated_at)
            .bind(&cursor.metadata)
            .execute(pool)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
        Ok(())
    }

    /// Delete a cursor.
    pub async fn delete(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM cursors WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
        Ok(())
    }

    /// List all cursors.
    pub async fn list(&self) -> Result<Vec<Cursor>> {
        let rows = sqlx::query(
            "SELECT name, slot, seq, source_seq, updated_at, metadata FROM cursors ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(rows.into_iter().map(Self::row_to_cursor).collect())
    }

    fn row_to_cursor(r: sqlx::postgres::PgRow) -> Cursor {
        Cursor {
            name: r.get("name"),
            slot: r.get::<i64, _>("slot") as u64,
            seq: SequenceNumber(r.get::<i64, _>("seq") as u64),
            source_seq: SequenceNumber(r.get::<i64, _>("source_seq") as u64),
            updated_at: r.get("updated_at"),
            metadata: r.get("metadata"),
        }
    }
}

const CURSOR_UPSERT: &str = r#"
INSERT INTO cursors (name, slot, seq, source_seq, updated_at, metadata)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (name) DO UPDATE SET
    -- GREATEST, not EXCLUDED: a cursor must never travel backwards, or a
    -- late-committing batch would re-expose data we have already passed.
    slot       = GREATEST(cursors.slot, EXCLUDED.slot),
    seq        = GREATEST(cursors.seq, EXCLUDED.seq),
    source_seq = GREATEST(cursors.source_seq, EXCLUDED.source_seq),
    updated_at = EXCLUDED.updated_at,
    metadata   = EXCLUDED.metadata
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_never_moves_a_cursor_backwards() {
        let mut cursor = Cursor::start("live");
        cursor.advance(100, SequenceNumber(50), SequenceNumber(70));
        assert_eq!(cursor.slot, 100);

        cursor.advance(90, SequenceNumber(40), SequenceNumber(60));
        assert_eq!(cursor.slot, 100);
        assert_eq!(cursor.seq, SequenceNumber(50));
        assert_eq!(cursor.source_seq, SequenceNumber(70));
    }
}
