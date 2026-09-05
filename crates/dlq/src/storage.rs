//! Storage backends for the DLQ.

use crate::entry::{DlqEntry, DlqStatus};
use async_trait::async_trait;
use chrono::Utc;
use slot_stream_common::Result;
use std::collections::HashMap;
use uuid::Uuid;

/// Trait for DLQ storage backends.
#[async_trait]
pub trait DlqStorage: Send + Sync {
    /// Insert a new entry.
    async fn insert(&self, entry: DlqEntry) -> Result<()>;

    /// Get an entry by ID.
    async fn get(&self, id: Uuid) -> Result<Option<DlqEntry>>;

    /// Update an existing entry.
    async fn update(&self, entry: DlqEntry) -> Result<()>;

    /// List unresolved entries.
    async fn list_unresolved(&self, limit: usize) -> Result<Vec<DlqEntry>>;

    /// List entries by error category.
    async fn list_by_category(&self, category: &str, limit: usize) -> Result<Vec<DlqEntry>>;

    /// Mark an entry as resolved.
    async fn resolve(&self, id: Uuid) -> Result<()>;

    /// Delete old resolved entries.
    async fn purge_resolved(&self, older_than_days: u32) -> Result<u64>;
}

/// In-memory storage for testing.
pub struct MemoryStorage {
    entries: parking_lot::RwLock<HashMap<Uuid, DlqEntry>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            entries: parking_lot::RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DlqStorage for MemoryStorage {
    async fn insert(&self, entry: DlqEntry) -> Result<()> {
        self.entries.write().insert(entry.id, entry);
        Ok(())
    }

    async fn get(&self, id: Uuid) -> Result<Option<DlqEntry>> {
        Ok(self.entries.read().get(&id).cloned())
    }

    async fn update(&self, entry: DlqEntry) -> Result<()> {
        self.entries.write().insert(entry.id, entry);
        Ok(())
    }

    async fn list_unresolved(&self, limit: usize) -> Result<Vec<DlqEntry>> {
        let entries = self.entries.read();
        Ok(entries
            .values()
            .filter(|e| e.status != DlqStatus::Resolved)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_by_category(&self, category: &str, limit: usize) -> Result<Vec<DlqEntry>> {
        let entries = self.entries.read();
        Ok(entries
            .values()
            .filter(|e| e.error_category == category && e.status != DlqStatus::Resolved)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn resolve(&self, id: Uuid) -> Result<()> {
        if let Some(entry) = self.entries.write().get_mut(&id) {
            entry.status = DlqStatus::Resolved;
            entry.resolved_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn purge_resolved(&self, _older_than_days: u32) -> Result<u64> {
        let mut entries = self.entries.write();
        let before = entries.len();
        entries.retain(|_, e| e.status != DlqStatus::Resolved);
        Ok((before - entries.len()) as u64)
    }
}

/// Postgres storage backend.
pub struct PostgresStorage {
    pool: sqlx::PgPool,
}

impl PostgresStorage {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DlqStorage for PostgresStorage {
    async fn insert(&self, entry: DlqEntry) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO dead_letter_queue (
                id, raw_payload, sequence, slot, kind,
                error_message, error_category, retry_count, max_retries,
                failed_at, received_at, is_resolved
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, false)
            "#,
        )
        .bind(entry.id)
        .bind(&entry.raw_payload)
        .bind(entry.sequence.map(|s| s.0 as i64))
        .bind(entry.slot.map(|s| s as i64))
        .bind(entry.kind.map(|k| k.as_str().to_string()))
        .bind(&entry.error_message)
        .bind(&entry.error_category)
        .bind(entry.retry_count as i32)
        .bind(entry.max_retries as i32)
        .bind(entry.failed_at)
        .bind(entry.received_at)
        .execute(&self.pool)
        .await
        .map_err(|e| slot_stream_common::Error::DlqWriteFailed(e.to_string()))?;

        Ok(())
    }

    async fn get(&self, id: Uuid) -> Result<Option<DlqEntry>> {
        let row = sqlx::query(
            r#"
            SELECT * FROM dead_letter_queue WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| slot_stream_common::Error::DatabaseQuery(e.to_string()))?;

        Ok(row.map(Self::row_to_entry))
    }

    async fn update(&self, entry: DlqEntry) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE dead_letter_queue
            SET retry_count = $2, last_retry_at = $3, is_resolved = $4, resolved_at = $5
            WHERE id = $1
            "#,
        )
        .bind(entry.id)
        .bind(entry.retry_count as i32)
        .bind(entry.last_retry_at)
        .bind(entry.status == DlqStatus::Resolved)
        .bind(entry.resolved_at)
        .execute(&self.pool)
        .await
        .map_err(|e| slot_stream_common::Error::DlqWriteFailed(e.to_string()))?;

        Ok(())
    }

    async fn list_unresolved(&self, limit: usize) -> Result<Vec<DlqEntry>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM dead_letter_queue
            WHERE is_resolved = false
            ORDER BY failed_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| slot_stream_common::Error::DatabaseQuery(e.to_string()))?;

        Ok(rows.into_iter().map(Self::row_to_entry).collect())
    }

    async fn list_by_category(&self, category: &str, limit: usize) -> Result<Vec<DlqEntry>> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM dead_letter_queue
            WHERE error_category = $1 AND is_resolved = false
            ORDER BY failed_at DESC
            LIMIT $2
            "#,
        )
        .bind(category)
        .bind(limit as i32)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| slot_stream_common::Error::DatabaseQuery(e.to_string()))?;

        Ok(rows.into_iter().map(Self::row_to_entry).collect())
    }

    async fn resolve(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE dead_letter_queue
            SET is_resolved = true, resolved_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| slot_stream_common::Error::DlqWriteFailed(e.to_string()))?;

        Ok(())
    }

    async fn purge_resolved(&self, older_than_days: u32) -> Result<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM dead_letter_queue
            WHERE is_resolved = true
            AND resolved_at < NOW() - INTERVAL '1 day' * $1
            "#,
        )
        .bind(older_than_days as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| slot_stream_common::Error::DlqWriteFailed(e.to_string()))?;

        Ok(result.rows_affected())
    }
}

impl PostgresStorage {
    fn row_to_entry(row: sqlx::postgres::PgRow) -> DlqEntry {
        use sqlx::Row;
        use slot_stream_common::SequenceNumber;

        DlqEntry {
            id: row.get("id"),
            raw_payload: row.get("raw_payload"),
            sequence: row
                .get::<Option<i64>, _>("sequence")
                .map(|s| SequenceNumber(s as u64)),
            slot: row.get::<Option<i64>, _>("slot").map(|s| s as u64),
            kind: row
                .get::<Option<String>, _>("kind")
                .as_deref()
                .and_then(slot_stream_common::EventKind::from_str_name),
            kind_name: row.get::<Option<String>, _>("kind"),
            error_message: row.get("error_message"),
            error_category: row.get("error_category"),
            retry_count: row.get::<i32, _>("retry_count") as u32,
            max_retries: row.get::<i32, _>("max_retries") as u32,
            failed_at: row.get("failed_at"),
            received_at: row.get("received_at"),
            last_retry_at: row.get("last_retry_at"),
            resolved_at: row.get("resolved_at"),
            status: if row.get::<bool, _>("is_resolved") {
                DlqStatus::Resolved
            } else {
                DlqStatus::Pending
            },
        }
    }
}
