//! Replay functionality for DLQ entries.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Configuration for replay operations.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Maximum entries to replay at once.
    pub batch_size: usize,

    /// Delay between replays.
    pub delay_ms: u64,

    /// Whether to stop on first failure.
    pub fail_fast: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            delay_ms: 100,
            fail_fast: false,
        }
    }
}

/// Result of a replay operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    /// Entries successfully replayed.
    pub succeeded: Vec<Uuid>,

    /// Entries that failed replay.
    pub failed: Vec<ReplayFailure>,

    /// When the replay started.
    pub started_at: DateTime<Utc>,

    /// When the replay completed.
    pub completed_at: DateTime<Utc>,
}

/// Information about a failed replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayFailure {
    pub id: Uuid,
    pub error: String,
    pub retry_count: u32,
}

impl ReplayResult {
    /// Create a new replay result.
    pub fn new() -> Self {
        Self {
            succeeded: Vec::new(),
            failed: Vec::new(),
            started_at: Utc::now(),
            completed_at: Utc::now(),
        }
    }

    /// Mark completion.
    pub fn complete(mut self) -> Self {
        self.completed_at = Utc::now();
        self
    }

    /// Get total entries processed.
    pub fn total(&self) -> usize {
        self.succeeded.len() + self.failed.len()
    }

    /// Get success rate.
    pub fn success_rate(&self) -> f64 {
        if self.total() == 0 {
            1.0
        } else {
            self.succeeded.len() as f64 / self.total() as f64
        }
    }
}

impl Default for ReplayResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Selector for which entries to replay.
#[derive(Debug, Clone)]
pub enum ReplaySelector {
    /// Replay specific entries by ID.
    ByIds(Vec<Uuid>),

    /// Replay all entries in a category.
    ByCategory(String),

    /// Replay entries for specific slots.
    BySlots(Vec<u64>),

    /// Replay all unresolved entries.
    AllUnresolved,
}

/// Rebuild the stream event an entry came from, so it can be fed back in.
///
/// Only possible when the entry retained enough context: a slot, a sequence, and
/// a kind. Entries that failed before any of that was known cannot be replayed
/// automatically and need an operator.
pub fn to_raw_event(entry: &crate::DlqEntry) -> Option<slot_stream_common::RawEvent> {
    use slot_stream_common::{EventKind, EventOrigin, RawEvent};

    let slot = entry.slot?;
    let sequence = entry.sequence?;
    let kind = entry.kind.or_else(|| {
        entry
            .kind_name
            .as_deref()
            .and_then(EventKind::from_str_name)
    })?;

    let mut event = RawEvent::new(
        sequence,
        kind,
        slot,
        bytes::Bytes::from(entry.raw_payload.clone()),
    )
    .with_origin(EventOrigin::Replay);

    if let Some(parent) = entry.parent_slot {
        event = event.with_parent(parent);
    }

    Some(event)
}
