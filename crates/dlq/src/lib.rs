//! # slot-stream-dlq
//!
//! Dead Letter Queue for handling failed events.
//!
//! ## Design Philosophy
//!
//! Events should never be silently dropped. When processing fails:
//! 1. Event goes to DLQ with error context
//! 2. Retry can be attempted (with limits)
//! 3. Manual inspection and resolution
//! 4. Metrics and alerting
//!
//! ## When Events Go to DLQ
//!
//! - Parse errors (malformed data)
//! - Validation failures (schema mismatch)
//! - Persistence errors (after retries exhausted)
//! - Unknown errors
//!
//! ## Replay Mechanism
//!
//! Events can be replayed:
//! 1. By ID (specific event)
//! 2. By category (all parse errors)
//! 3. By slot range
//! 4. All unresolved

pub mod entry;
pub mod replay;
pub mod storage;

use chrono::Utc;
use slot_stream_common::{Error, FailedEvent, IndexedEvent, RawEvent, Result};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;

pub use entry::{DlqEntry, DlqStatus};
pub use replay::{ReplayConfig, ReplayResult, ReplaySelector};
pub use storage::{DlqStorage, MemoryStorage, PostgresStorage};

/// Configuration for the dead letter queue.
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Maximum retry attempts before permanent failure.
    pub max_retries: u32,

    /// Whether to enable automatic retry.
    pub auto_retry: bool,

    /// Channel buffer size for async operations.
    pub channel_size: usize,

    /// Maximum age for unresolved items before alerting.
    pub alert_threshold_hours: u32,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            auto_retry: false,
            channel_size: 1000,
            alert_threshold_hours: 24,
        }
    }
}

/// Statistics about the DLQ.
#[derive(Debug, Clone, Default)]
pub struct DlqStats {
    pub total_entries: u64,
    pub unresolved: u64,
    pub resolved: u64,
    pub retries_exhausted: u64,
    pub by_category: std::collections::HashMap<String, u64>,
}

/// The dead letter queue.
pub struct DeadLetterQueue {
    storage: Arc<dyn DlqStorage>,
    config: DlqConfig,
    stats: Arc<parking_lot::Mutex<DlqStats>>,
}

impl DeadLetterQueue {
    /// Create a new DLQ with the given storage backend.
    pub fn new(storage: Arc<dyn DlqStorage>, config: DlqConfig) -> Self {
        Self {
            storage,
            config,
            stats: Arc::new(parking_lot::Mutex::new(DlqStats::default())),
        }
    }

    /// Create an in-memory DLQ for testing.
    pub fn in_memory() -> Self {
        Self::new(Arc::new(MemoryStorage::new()), DlqConfig::default())
    }

    /// Enqueue a failed raw event.
    pub async fn enqueue(&self, event: RawEvent, error: &Error) -> Result<Uuid> {
        let failed = FailedEvent::from_raw(event, error);
        self.enqueue_failed(failed).await
    }

    /// Enqueue a failed indexed event.
    pub async fn enqueue_indexed(&self, event: IndexedEvent, error: &Error) -> Result<Uuid> {
        let entry = DlqEntry {
            id: Uuid::new_v4(),
            raw_payload: serde_json::to_vec(&event.data).unwrap_or_default(),
            sequence: Some(event.source_seq),
            slot: Some(event.slot),
            parent_slot: event.parent_slot,
            kind: Some(event.kind),
            kind_name: Some(event.kind.as_str().to_string()),
            error_message: error.to_string(),
            error_category: slot_stream_common::ErrorCategory::of(error)
                .as_str()
                .to_string(),
            retry_count: 0,
            max_retries: self.config.max_retries,
            failed_at: Utc::now(),
            received_at: Some(event.received_at),
            last_retry_at: None,
            resolved_at: None,
            status: DlqStatus::Pending,
        };

        self.storage.insert(entry.clone()).await?;
        self.update_stats_on_insert(&entry);

        warn!(
            id = %entry.id,
            slot = ?entry.slot,
            category = ?entry.error_category,
            "Event sent to DLQ"
        );

        Ok(entry.id)
    }

    /// Enqueue a failed event.
    pub async fn enqueue_failed(&self, failed: FailedEvent) -> Result<Uuid> {
        let entry = DlqEntry::from_failed(failed, self.config.max_retries);
        let id = entry.id;

        self.storage.insert(entry.clone()).await?;
        self.update_stats_on_insert(&entry);

        warn!(
            id = %id,
            slot = ?entry.slot,
            category = ?entry.error_category,
            "Event sent to DLQ"
        );

        Ok(id)
    }

    /// Get an entry by ID.
    pub async fn get(&self, id: Uuid) -> Result<Option<DlqEntry>> {
        self.storage.get(id).await
    }

    /// List unresolved entries.
    pub async fn list_unresolved(&self, limit: usize) -> Result<Vec<DlqEntry>> {
        self.storage.list_unresolved(limit).await
    }

    /// List entries by category.
    pub async fn list_by_category(&self, category: &str, limit: usize) -> Result<Vec<DlqEntry>> {
        self.storage.list_by_category(category, limit).await
    }

    /// Mark an entry as resolved.
    pub async fn resolve(&self, id: Uuid) -> Result<()> {
        self.storage.resolve(id).await?;

        let mut stats = self.stats.lock();
        stats.unresolved = stats.unresolved.saturating_sub(1);
        stats.resolved += 1;

        info!(id = %id, "DLQ entry resolved");

        Ok(())
    }

    /// Increment retry count and return whether retries are exhausted.
    pub async fn retry(&self, id: Uuid) -> Result<bool> {
        let entry = self.storage.get(id).await?;

        match entry {
            Some(mut e) => {
                e.retry_count += 1;
                e.last_retry_at = Some(Utc::now());

                let exhausted = e.retry_count >= e.max_retries;
                if exhausted {
                    e.status = DlqStatus::Exhausted;

                    let mut stats = self.stats.lock();
                    stats.retries_exhausted += 1;
                }

                self.storage.update(e).await?;

                Ok(exhausted)
            }
            None => Err(Error::DlqWriteFailed(format!("Entry {id} not found"))),
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> DlqStats {
        self.stats.lock().clone()
    }

    /// Replay entries back into the pipeline.
    ///
    /// Each entry is reconstructed into the stream event it came from and pushed
    /// to `sink`, which is the same channel the ingester feeds. Replayed events
    /// therefore travel the ordinary path — parsing, fork detection, idempotent
    /// write — rather than a special one that could diverge from it.
    ///
    /// An entry is resolved only once it has been accepted by the sink. If the
    /// pipeline rejects it again it lands back in the DLQ with its retry count
    /// incremented, which is why this is safe to run repeatedly.
    pub async fn replay(
        &self,
        selector: ReplaySelector,
        sink: &tokio::sync::mpsc::Sender<RawEvent>,
        config: &ReplayConfig,
    ) -> Result<ReplayResult> {
        let entries = self.select(&selector, config.batch_size).await?;
        let mut result = ReplayResult::new();

        info!(
            selected = entries.len(),
            selector = ?selector,
            "starting DLQ replay"
        );

        for entry in entries {
            if !entry.can_retry() {
                result.failed.push(replay::ReplayFailure {
                    id: entry.id,
                    error: "retries exhausted".into(),
                    retry_count: entry.retry_count,
                });
                if config.fail_fast {
                    break;
                }
                continue;
            }

            let Some(raw) = replay::to_raw_event(&entry) else {
                result.failed.push(replay::ReplayFailure {
                    id: entry.id,
                    error: "entry lacks the slot/sequence/kind needed to rebuild the event".into(),
                    retry_count: entry.retry_count,
                });
                if config.fail_fast {
                    break;
                }
                continue;
            };

            match sink.send(raw).await {
                Ok(()) => {
                    self.retry(entry.id).await?;
                    self.resolve(entry.id).await?;
                    result.succeeded.push(entry.id);
                }
                Err(_) => {
                    result.failed.push(replay::ReplayFailure {
                        id: entry.id,
                        error: "pipeline sink closed".into(),
                        retry_count: entry.retry_count,
                    });
                    break;
                }
            }

            if config.delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(config.delay_ms)).await;
            }
        }

        let result = result.complete();
        info!(
            succeeded = result.succeeded.len(),
            failed = result.failed.len(),
            "DLQ replay finished"
        );
        Ok(result)
    }

    /// Resolve a selector into the entries it names.
    async fn select(&self, selector: &ReplaySelector, limit: usize) -> Result<Vec<DlqEntry>> {
        match selector {
            ReplaySelector::AllUnresolved => self.list_unresolved(limit).await,
            ReplaySelector::ByCategory(category) => self.list_by_category(category, limit).await,
            ReplaySelector::ByIds(ids) => {
                let mut entries = Vec::new();
                for &id in ids.iter().take(limit) {
                    if let Some(entry) = self.get(id).await? {
                        entries.push(entry);
                    }
                }
                Ok(entries)
            }
            ReplaySelector::BySlots(slots) => {
                let all = self.list_unresolved(limit.max(1_000)).await?;
                Ok(all
                    .into_iter()
                    .filter(|e| e.slot.is_some_and(|s| slots.contains(&s)))
                    .take(limit)
                    .collect())
            }
        }
    }

    /// Update stats on insert.
    fn update_stats_on_insert(&self, entry: &DlqEntry) {
        let mut stats = self.stats.lock();
        stats.total_entries += 1;
        stats.unresolved += 1;
        *stats
            .by_category
            .entry(entry.error_category.clone())
            .or_insert(0) += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slot_stream_common::{EventKind, SequenceNumber};

    #[tokio::test]
    async fn test_dlq_enqueue() {
        let dlq = DeadLetterQueue::in_memory();

        let event = RawEvent::new(
            SequenceNumber(1),
            EventKind::Transaction,
            100,
            bytes::Bytes::from_static(b"test"),
        );

        let error = Error::EventParse("test error".into());
        let id = dlq.enqueue(event, &error).await.unwrap();

        let entry = dlq.get(id).await.unwrap().unwrap();
        assert_eq!(entry.error_category, "malformed");
        assert_eq!(entry.retry_count, 0);
    }

    #[tokio::test]
    async fn test_dlq_resolve() {
        let dlq = DeadLetterQueue::in_memory();

        let event = RawEvent::new(
            SequenceNumber(1),
            EventKind::Transaction,
            100,
            bytes::Bytes::from_static(b"test"),
        );

        let error = Error::EventParse("test error".into());
        let id = dlq.enqueue(event, &error).await.unwrap();

        dlq.resolve(id).await.unwrap();

        let entry = dlq.get(id).await.unwrap().unwrap();
        assert_eq!(entry.status, DlqStatus::Resolved);
    }
}
