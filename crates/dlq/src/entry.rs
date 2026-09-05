//! DLQ entry types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use slot_stream_common::{EventKind, FailedEvent, SequenceNumber};
use uuid::Uuid;

/// Status of a DLQ entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DlqStatus {
    /// Waiting to be processed.
    Pending,
    /// Currently being retried.
    Retrying,
    /// Retries exhausted.
    Exhausted,
    /// Manually resolved.
    Resolved,
}

/// An entry in the dead letter queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// Unique identifier.
    pub id: Uuid,

    /// Original raw payload.
    pub raw_payload: Vec<u8>,

    /// Sequence number if available.
    pub sequence: Option<SequenceNumber>,

    /// Slot if known.
    pub slot: Option<u64>,

    /// Parent slot if the stream reported one, so a replay rebuilds the event
    /// the chain tracker originally saw rather than a parentless copy of it.
    pub parent_slot: Option<u64>,

    /// Event kind if known.
    pub kind: Option<EventKind>,

    /// The kind as stored in the database, for rows read back where the enum
    /// could not be resolved.
    pub kind_name: Option<String>,

    /// Error message.
    pub error_message: String,

    /// Error category for filtering.
    pub error_category: String,

    /// Number of retry attempts.
    pub retry_count: u32,

    /// Maximum retries allowed.
    pub max_retries: u32,

    /// When the failure occurred.
    pub failed_at: DateTime<Utc>,

    /// When the event was originally received.
    pub received_at: Option<DateTime<Utc>>,

    /// When the last retry was attempted.
    pub last_retry_at: Option<DateTime<Utc>>,

    /// When the entry was resolved.
    pub resolved_at: Option<DateTime<Utc>>,

    /// Current status.
    pub status: DlqStatus,
}

impl DlqEntry {
    /// Create from a FailedEvent.
    pub fn from_failed(failed: FailedEvent, max_retries: u32) -> Self {
        Self {
            id: Uuid::new_v4(),
            raw_payload: failed.raw_payload,
            sequence: failed.sequence,
            slot: failed.slot,
            parent_slot: failed.parent_slot,
            kind: failed.kind,
            kind_name: failed.kind.map(|k| k.as_str().to_string()),
            error_message: failed.error_message,
            error_category: failed.error_category.as_str().to_string(),
            retry_count: failed.retry_count,
            max_retries,
            failed_at: failed.failed_at,
            received_at: failed.received_at,
            last_retry_at: None,
            resolved_at: None,
            status: DlqStatus::Pending,
        }
    }

    /// Check if retries are exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.retry_count >= self.max_retries
    }

    /// Check if this entry can be retried.
    pub fn can_retry(&self) -> bool {
        !self.is_exhausted() && self.status == DlqStatus::Pending
    }

    /// Get age of this entry.
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.failed_at
    }
}
