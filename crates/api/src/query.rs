//! Query layer for the read API.
//!
//! Every query filters on `is_valid = true`. That single predicate is what makes
//! the API reorg-aware: rows orphaned by a fork stay in the table for audit but
//! never appear in a response, so what a caller sees is always the canonical
//! chain as currently understood.

use serde::{Deserialize, Serialize};
use slot_stream_common::{Error, Result, SequenceNumber};
use sqlx::{PgPool, Row};

/// A stored event, as returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventRecord {
    pub id: uuid::Uuid,
    pub seq: u64,
    pub source_seq: u64,
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub kind: String,
    pub data: serde_json::Value,
    pub event_hash: String,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub indexed_at: chrono::DateTime<chrono::Utc>,
}

impl EventRecord {
    fn from_row(row: sqlx::postgres::PgRow) -> Self {
        Self {
            id: row.get("id"),
            seq: row.get::<i64, _>("seq") as u64,
            source_seq: row.get::<i64, _>("source_seq") as u64,
            slot: row.get::<i64, _>("slot") as u64,
            parent_slot: row.get::<Option<i64>, _>("parent_slot").map(|s| s as u64),
            kind: row.get("kind"),
            data: row.get("data"),
            event_hash: row.get("event_hash"),
            received_at: row.get("received_at"),
            indexed_at: row.get("indexed_at"),
        }
    }
}

/// A slot as recorded by the chain tracker.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SlotRecord {
    pub slot: u64,
    pub parent_slot: u64,
    pub block_hash: Option<String>,
    pub status: String,
    pub is_canonical: bool,
    pub transaction_count: i32,
    pub received_at: chrono::DateTime<chrono::Utc>,
}

/// Filters accepted by the event listing endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct EventQuery {
    /// Restrict to a single slot.
    pub slot: Option<u64>,
    /// Inclusive lower bound on slot.
    pub from_slot: Option<u64>,
    /// Inclusive upper bound on slot.
    pub to_slot: Option<u64>,
    /// Inclusive lower bound on assigned sequence.
    pub from_seq: Option<u64>,
    /// Inclusive upper bound on assigned sequence.
    pub to_seq: Option<u64>,
    /// Restrict to one event kind.
    pub kind: Option<String>,
    /// Maximum rows to return.
    pub limit: Option<u32>,
    /// Rows to skip.
    pub offset: Option<u32>,
}

/// Reads indexed data.
#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
    max_page_size: u32,
    default_page_size: u32,
}

impl EventStore {
    /// Create a store over `pool`.
    pub fn new(pool: PgPool, max_page_size: u32, default_page_size: u32) -> Self {
        Self {
            pool,
            max_page_size: max_page_size.max(1),
            default_page_size: default_page_size.max(1),
        }
    }

    fn page_size(&self, requested: Option<u32>) -> i64 {
        requested
            .unwrap_or(self.default_page_size)
            .clamp(1, self.max_page_size) as i64
    }

    /// List events matching `query`, ordered by the assigned sequence.
    pub async fn list_events(&self, query: &EventQuery) -> Result<Vec<EventRecord>> {
        // Built with a fixed set of optional predicates rather than string
        // concatenation, so every value stays a bound parameter.
        let sql = r#"
            SELECT id, seq, source_seq, slot, parent_slot, kind, data,
                   event_hash, received_at, indexed_at
            FROM events
            WHERE is_valid = true
              AND ($1::bigint IS NULL OR slot = $1)
              AND ($2::bigint IS NULL OR slot >= $2)
              AND ($3::bigint IS NULL OR slot <= $3)
              AND ($4::bigint IS NULL OR seq >= $4)
              AND ($5::bigint IS NULL OR seq <= $5)
              AND ($6::text   IS NULL OR kind = $6)
            ORDER BY slot ASC, seq ASC
            LIMIT $7 OFFSET $8
        "#;

        let rows = sqlx::query(sql)
            .bind(query.slot.map(|v| v as i64))
            .bind(query.from_slot.map(|v| v as i64))
            .bind(query.to_slot.map(|v| v as i64))
            .bind(query.from_seq.map(|v| v as i64))
            .bind(query.to_seq.map(|v| v as i64))
            .bind(query.kind.as_deref())
            .bind(self.page_size(query.limit))
            .bind(query.offset.unwrap_or(0) as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(rows.into_iter().map(EventRecord::from_row).collect())
    }

    /// All valid events in one slot.
    pub async fn events_by_slot(&self, slot: u64, limit: Option<u32>) -> Result<Vec<EventRecord>> {
        self.list_events(&EventQuery {
            slot: Some(slot),
            limit,
            ..Default::default()
        })
        .await
    }

    /// Events whose payload carries `signature`.
    pub async fn events_by_signature(&self, signature: &str) -> Result<Vec<EventRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, seq, source_seq, slot, parent_slot, kind, data,
                   event_hash, received_at, indexed_at
            FROM events
            WHERE is_valid = true
              AND data -> 'body' ->> 'signature' = $1
            ORDER BY slot ASC, seq ASC
            LIMIT $2
            "#,
        )
        .bind(signature)
        .bind(self.max_page_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(rows.into_iter().map(EventRecord::from_row).collect())
    }

    /// Events whose payload mentions `account` among its keys.
    pub async fn events_by_account(&self, account: &str, limit: Option<u32>) -> Result<Vec<EventRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, seq, source_seq, slot, parent_slot, kind, data,
                   event_hash, received_at, indexed_at
            FROM events
            WHERE is_valid = true
              AND (
                    data -> 'body' ->> 'pubkey' = $1
                 OR data -> 'body' -> 'message' -> 'accountKeys' @> to_jsonb($1::text)
              )
            ORDER BY slot ASC, seq ASC
            LIMIT $2
            "#,
        )
        .bind(account)
        .bind(self.page_size(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(rows.into_iter().map(EventRecord::from_row).collect())
    }

    /// Events in an assigned-sequence range.
    pub async fn events_by_sequence_range(
        &self,
        from: SequenceNumber,
        to: SequenceNumber,
        limit: Option<u32>,
    ) -> Result<Vec<EventRecord>> {
        self.list_events(&EventQuery {
            from_seq: Some(from.0),
            to_seq: Some(to.0),
            limit,
            ..Default::default()
        })
        .await
    }

    /// A slot's chain record.
    pub async fn slot(&self, slot: u64) -> Result<Option<SlotRecord>> {
        let row = sqlx::query(
            r#"
            SELECT slot, parent_slot, block_hash, status, is_canonical,
                   transaction_count, received_at
            FROM slots WHERE slot = $1
            "#,
        )
        .bind(slot as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(row.map(|r| SlotRecord {
            slot: r.get::<i64, _>("slot") as u64,
            parent_slot: r.get::<i64, _>("parent_slot") as u64,
            block_hash: r.get("block_hash"),
            status: r.get("status"),
            is_canonical: r.get("is_canonical"),
            transaction_count: r.get("transaction_count"),
            received_at: r.get("received_at"),
        }))
    }

    /// The canonical chain head, as persisted.
    pub async fn chain_head(&self) -> Result<Option<u64>> {
        let row = sqlx::query("SELECT MAX(slot) AS head FROM slots WHERE is_canonical = true")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(row.get::<Option<i64>, _>("head").map(|s| s as u64))
    }

    /// Counts of valid and invalidated rows, for the status endpoint.
    pub async fn counts(&self) -> Result<EventCounts> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE is_valid = true)  AS valid,
                COUNT(*) FILTER (WHERE is_valid = false) AS invalidated,
                COALESCE(MAX(seq)  FILTER (WHERE is_valid = true), 0) AS max_seq,
                COALESCE(MAX(slot) FILTER (WHERE is_valid = true), 0) AS max_slot
            FROM events
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(EventCounts {
            valid: row.get::<i64, _>("valid") as u64,
            invalidated: row.get::<i64, _>("invalidated") as u64,
            max_seq: row.get::<i64, _>("max_seq") as u64,
            max_slot: row.get::<i64, _>("max_slot") as u64,
        })
    }

    /// Whether the database is reachable.
    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| Error::DatabaseConnection(e.to_string()))?;
        Ok(())
    }

    /// The pool, for callers that need it.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Row counts for the status endpoint.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct EventCounts {
    pub valid: u64,
    pub invalidated: u64,
    pub max_seq: u64,
    pub max_slot: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> EventStore {
        // Page-size clamping is pure arithmetic and worth testing without a
        // database; the query paths are covered by the integration tests.
        EventStore {
            pool: PgPool::connect_lazy("postgres://invalid/invalid").expect("lazy pool"),
            max_page_size: 1_000,
            default_page_size: 100,
        }
    }

    #[test]
    fn page_size_defaults_and_clamps() {
        let store = store();
        assert_eq!(store.page_size(None), 100);
        assert_eq!(store.page_size(Some(50)), 50);
        assert_eq!(store.page_size(Some(0)), 1, "zero would return nothing");
        assert_eq!(
            store.page_size(Some(1_000_000)),
            1_000,
            "an unbounded limit is how one caller takes down the database"
        );
    }
}
