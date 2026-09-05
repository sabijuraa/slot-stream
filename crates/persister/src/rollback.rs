//! Rollback execution for chain reorgs.

use slot_stream_common::{Error, Result, RollbackPlan};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tracing::{info, instrument, warn};

/// Applies rollback plans to persisted state.
///
/// Rollback is a soft delete: rows keep their data and gain `is_valid = false`.
/// Readers filter on `is_valid`, so the visible state matches the canonical chain
/// while the orphaned rows remain available for audit. The inverse operation
/// lives in the writer's upsert, which resets the flag when the canonical chain
/// re-delivers an event.
pub struct RollbackExecutor {
    pool: PgPool,
}

/// What a rollback did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RollbackOutcome {
    /// Event rows invalidated.
    pub events_invalidated: u64,
    /// Slot rows marked non-canonical.
    pub slots_orphaned: u64,
    /// Event rows brought back because their slot rejoined the canonical chain.
    pub events_restored: u64,
}

impl RollbackExecutor {
    /// Create a new rollback executor.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Execute a plan.
    ///
    /// Invalidating the events, de-canonicalising the slots, and recording the
    /// reorg all happen in one transaction. A partial rollback is the one state
    /// we must never leave behind: it would show a chain that never existed.
    #[instrument(skip(self, plan), fields(fork_slot = plan.fork_slot, depth = plan.depth()))]
    pub async fn execute(&self, plan: &RollbackPlan) -> Result<RollbackOutcome> {
        let mut tx: Transaction<'_, Postgres> = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::DatabaseConnection(e.to_string()))?;

        let outcome = Self::apply(&mut tx, plan).await?;

        tx.commit()
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        if plan.divergence_is_bound {
            warn!(
                fork_slot = plan.fork_slot,
                divergence_point = plan.divergence_point,
                "rolled back to a bounded divergence point; the branch reached \
                 past what we retain, so a backfill of the gap is warranted"
            );
        }

        info!(
            fork_slot = plan.fork_slot,
            divergence_point = plan.divergence_point,
            slots = plan.depth(),
            events_invalidated = outcome.events_invalidated,
            events_restored = outcome.events_restored,
            restored_slots = plan.slots_to_restore.len(),
            "rollback committed"
        );

        Ok(outcome)
    }

    async fn apply(
        tx: &mut Transaction<'_, Postgres>,
        plan: &RollbackPlan,
    ) -> Result<RollbackOutcome> {
        let mut outcome = RollbackOutcome::default();

        let slots: Vec<i64> = plan.slots_to_rollback.iter().map(|&s| s as i64).collect();

        if !slots.is_empty() {
            let result = sqlx::query(
                r#"
                UPDATE events
                SET is_valid = false, invalidated_at = NOW(), updated_at = NOW()
                WHERE slot = ANY($1) AND is_valid = true
                "#,
            )
            .bind(&slots)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
            outcome.events_invalidated = result.rows_affected();

            let result = sqlx::query(
                r#"
                UPDATE slots
                SET is_canonical = false, status = 'Orphaned', updated_at = NOW()
                WHERE slot = ANY($1)
                "#,
            )
            .bind(&slots)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
            outcome.slots_orphaned = result.rows_affected();
        }

        // Restoring the adopted branch is the mirror of the invalidation above.
        // A chain that forks away from a slot and later forks back to it will
        // never re-deliver that slot's events — they are already stored — so the
        // rows have to be revived here or they stay invisible for good.
        let restore: Vec<i64> = plan.slots_to_restore.iter().map(|&s| s as i64).collect();

        if !restore.is_empty() {
            let result = sqlx::query(
                r#"
                UPDATE events
                SET is_valid = true, invalidated_at = NULL, updated_at = NOW()
                WHERE slot = ANY($1) AND is_valid = false
                "#,
            )
            .bind(&restore)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
            outcome.events_restored = result.rows_affected();

            sqlx::query(
                r#"
                UPDATE slots
                SET is_canonical = true, status = 'Confirmed', updated_at = NOW()
                WHERE slot = ANY($1)
                "#,
            )
            .bind(&restore)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;
        }

        sqlx::query(
            r#"
            INSERT INTO reorgs (
                id, fork_slot, divergence_point, expected_parent, actual_parent,
                rollback_depth, events_invalidated, slots_rolled_back,
                divergence_is_bound, detected_at, completed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(plan.id)
        .bind(plan.fork_slot as i64)
        .bind(plan.divergence_point as i64)
        .bind(plan.expected_parent as i64)
        .bind(plan.actual_parent as i64)
        .bind(plan.depth() as i32)
        .bind(outcome.events_invalidated as i64)
        .bind(&slots)
        .bind(plan.divergence_is_bound)
        .bind(plan.created_at)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(outcome)
    }

    /// Count the events a plan would invalidate, without applying it.
    pub async fn count_affected(&self, slots: &[u64]) -> Result<u64> {
        if slots.is_empty() {
            return Ok(0);
        }
        let slot_list: Vec<i64> = slots.iter().map(|&s| s as i64).collect();

        let row = sqlx::query(
            "SELECT COUNT(*) AS count FROM events WHERE slot = ANY($1) AND is_valid = true",
        )
        .bind(&slot_list)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(row.get::<i64, _>("count") as u64)
    }

    /// Permanently remove invalidated events below a slot, reclaiming space.
    ///
    /// Only safe below the rooted watermark: above it the cluster could still
    /// switch back to a branch we invalidated, and we would want the rows.
    #[instrument(skip(self))]
    pub async fn purge_invalid_below(&self, slot: u64) -> Result<u64> {
        let result = sqlx::query("DELETE FROM events WHERE is_valid = false AND slot < $1")
            .bind(slot as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        let deleted = result.rows_affected();
        info!(
            below_slot = slot,
            events_purged = deleted,
            "purged invalid events"
        );
        Ok(deleted)
    }

    /// Reorg history, most recent first. Backs the admin/API view.
    pub async fn history(&self, limit: i64) -> Result<Vec<ReorgRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, fork_slot, divergence_point, rollback_depth,
                   events_invalidated, divergence_is_bound, detected_at
            FROM reorgs
            ORDER BY detected_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::DatabaseQuery(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| ReorgRecord {
                id: r.get("id"),
                fork_slot: r.get::<i64, _>("fork_slot") as u64,
                divergence_point: r.get::<i64, _>("divergence_point") as u64,
                rollback_depth: r.get::<i32, _>("rollback_depth") as u32,
                events_invalidated: r.get::<i64, _>("events_invalidated") as u64,
                divergence_is_bound: r.get("divergence_is_bound"),
                detected_at: r.get("detected_at"),
            })
            .collect())
    }
}

/// A recorded reorg.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReorgRecord {
    pub id: uuid::Uuid,
    pub fork_slot: u64,
    pub divergence_point: u64,
    pub rollback_depth: u32,
    pub events_invalidated: u64,
    pub divergence_is_bound: bool,
    pub detected_at: chrono::DateTime<chrono::Utc>,
}
