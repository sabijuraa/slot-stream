//! Event types for the indexing pipeline.
//!
//! Events flow through the pipeline in stages:
//! 1. RawEvent - Raw bytes from gRPC stream
//! 2. IndexedEvent - Parsed and validated, ready for processing
//! 3. PersistedEvent - Written to database with sequence tracking

use crate::{SequenceNumber, SlotInfo};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The kind of event in the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum EventKind {
    /// Slot metadata update.
    SlotUpdate,
    /// Account data change.
    AccountUpdate,
    /// Transaction execution.
    Transaction,
    /// Block metadata.
    BlockMeta,
    /// Entry (within a slot).
    Entry,
}

impl EventKind {
    /// Returns the priority for processing.
    /// Lower values = higher priority.
    /// Stable name used in the database `kind` column and the read API.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::SlotUpdate => "SlotUpdate",
            EventKind::AccountUpdate => "AccountUpdate",
            EventKind::Transaction => "Transaction",
            EventKind::BlockMeta => "BlockMeta",
            EventKind::Entry => "Entry",
        }
    }

    /// Parse the name written by `as_str`.
    pub fn from_str_name(name: &str) -> Option<Self> {
        match name {
            "SlotUpdate" => Some(EventKind::SlotUpdate),
            "AccountUpdate" => Some(EventKind::AccountUpdate),
            "Transaction" => Some(EventKind::Transaction),
            "BlockMeta" => Some(EventKind::BlockMeta),
            "Entry" => Some(EventKind::Entry),
            _ => None,
        }
    }

    pub fn priority(&self) -> u8 {
        match self {
            EventKind::SlotUpdate => 0,    // Process first for chain tracking
            EventKind::BlockMeta => 1,     // Then block metadata
            EventKind::Entry => 2,         // Then entries
            EventKind::Transaction => 3,   // Then transactions
            EventKind::AccountUpdate => 4, // Finally account updates
        }
    }
}

/// Where an event entered the pipeline.
///
/// The distinction is not cosmetic: it decides whether the event is evidence
/// about the shape of the chain. A live event reports what the cluster is doing
/// now, so its parent link drives fork detection. A backfilled event is a
/// historical repair fetched from RPC — it describes a slot the cluster already
/// confirmed, arriving long after the head has moved past it. Running one
/// through fork detection would read "slot 6 builds on slot 5" as a branch that
/// orphans everything above slot 6, which is the opposite of filling a gap.
///
/// It also decides whether the stream's sequence number means anything. The
/// ingester tracks sequences to spot gaps and drop retransmits, and that is only
/// coherent for one monotonic stream. A backfilled or replayed event carries a
/// sequence from somewhere else entirely; letting the tracker judge it either
/// discards the event as a rewind or reports a gap of millions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventOrigin {
    /// From the live stream. Drives sequence tracking and fork detection.
    #[default]
    Live,
    /// Fetched from RPC to fill a gap. Written, but not treated as chain news.
    Backfill,
    /// Re-injected from the dead-letter queue after an earlier failure.
    ///
    /// Unlike a backfill this really is a live-stream event, just late, so its
    /// parent link is still evidence about the chain and fork detection applies.
    Replay,
}

impl EventOrigin {
    /// Stable name, for logs and metric labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventOrigin::Live => "live",
            EventOrigin::Backfill => "backfill",
            EventOrigin::Replay => "replay",
        }
    }

    /// Whether the stream's own sequence number is meaningful for this event.
    ///
    /// Only the live stream produces one monotonic sequence; everything else
    /// arrives out of band and must not be judged against it.
    pub fn is_sequenced(&self) -> bool {
        matches!(self, EventOrigin::Live)
    }
}

/// A raw event as received from the gRPC stream.
///
/// Contains unparsed bytes and minimal metadata for routing.
#[derive(Debug, Clone)]
pub struct RawEvent {
    /// Sequence number from the stream.
    pub sequence: SequenceNumber,

    /// Where the event came from.
    pub origin: EventOrigin,

    /// Event kind for routing.
    pub kind: EventKind,

    /// Slot this event belongs to.
    pub slot: u64,

    /// Parent slot, when the stream reports it. Fork detection needs this.
    pub parent_slot: Option<u64>,

    /// Raw payload bytes.
    pub payload: bytes::Bytes,

    /// When the event was received.
    pub received_at: DateTime<Utc>,
}

impl RawEvent {
    /// Create a new raw event.
    pub fn new(
        sequence: SequenceNumber,
        kind: EventKind,
        slot: u64,
        payload: bytes::Bytes,
    ) -> Self {
        Self {
            sequence,
            origin: EventOrigin::Live,
            kind,
            slot,
            parent_slot: None,
            payload,
            received_at: Utc::now(),
        }
    }

    /// Attach the parent slot reported by the stream.
    pub fn with_parent(mut self, parent_slot: u64) -> Self {
        self.parent_slot = Some(parent_slot);
        self
    }

    /// Mark where this event came from.
    pub fn with_origin(mut self, origin: EventOrigin) -> Self {
        self.origin = origin;
        self
    }

    /// Get the size of the payload in bytes.
    pub fn payload_size(&self) -> usize {
        self.payload.len()
    }
}

/// A fully parsed and validated event ready for processing.
///
/// Carries two sequence numbers, and the distinction matters:
///
/// - `source_seq` is the stream's own number. It is stable for a given event, so
///   it is what row identity and idempotent writes key on.
/// - `seq` is assigned by the processor as it commits. It defines the order
///   readers see, and a replayed event gets a fresh one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedEvent {
    /// Unique ID for this event.
    pub id: Uuid,

    /// Ordering sequence assigned by the processor.
    pub seq: SequenceNumber,

    /// The stream's sequence number. Stable identity across replays.
    pub source_seq: SequenceNumber,

    /// Event kind.
    pub kind: EventKind,

    /// Slot this event belongs to.
    pub slot: u64,

    /// Parent slot (for chain tracking).
    pub parent_slot: Option<u64>,

    /// Parsed event data (JSON).
    pub data: serde_json::Value,

    /// When the event was received.
    pub received_at: DateTime<Utc>,

    /// When the event was indexed.
    pub indexed_at: DateTime<Utc>,

    /// Content hash, for deduplication and for comparing two records of the
    /// same logical event.
    pub event_hash: String,
}

impl IndexedEvent {
    /// Build an indexed event from a raw one, stamping the assigned sequence.
    pub fn from_raw(raw: RawEvent, data: serde_json::Value, seq: SequenceNumber) -> Self {
        let event_hash = Self::compute_hash(&raw);

        Self {
            id: Uuid::new_v4(),
            seq,
            source_seq: raw.sequence,
            kind: raw.kind,
            slot: raw.slot,
            parent_slot: raw.parent_slot,
            data,
            received_at: raw.received_at,
            indexed_at: Utc::now(),
            event_hash,
        }
    }

    /// Create with parent slot info.
    pub fn with_slot_info(mut self, slot_info: &SlotInfo) -> Self {
        self.parent_slot = Some(slot_info.parent_slot);
        self
    }

    /// Content hash of the event.
    ///
    /// SHA-256 rather than `DefaultHasher`: this value is written to the database
    /// and compared across processes and releases, and `DefaultHasher` is
    /// explicitly not stable across either.
    fn compute_hash(raw: &RawEvent) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(raw.slot.to_le_bytes());
        hasher.update(raw.sequence.0.to_le_bytes());
        hasher.update([raw.kind as u8]);
        hasher.update(&raw.payload);

        let digest = hasher.finalize();
        // 32 hex chars is ample for dedup and keeps the column narrow.
        digest[..16].iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Check if this event can be merged with another (same content).
    pub fn is_duplicate_of(&self, other: &Self) -> bool {
        self.event_hash == other.event_hash && self.slot == other.slot
    }

    /// Identity used for idempotent writes.
    pub fn identity(&self) -> (u64, u64) {
        (self.slot, self.source_seq.0)
    }
}

/// An event that failed processing and goes to the DLQ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedEvent {
    /// Original raw event data.
    pub raw_payload: Vec<u8>,

    /// Sequence number if available.
    pub sequence: Option<SequenceNumber>,

    /// Slot if known.
    pub slot: Option<u64>,

    /// Parent slot if the stream reported one.
    ///
    /// Retained so a replay is a faithful reconstruction: without it the
    /// replayed event carries no parent, the chain tracker has nothing to check,
    /// and the slot silently stops being part of the chain the indexer knows.
    pub parent_slot: Option<u64>,

    /// Event kind if known.
    pub kind: Option<EventKind>,

    /// Error message.
    pub error_message: String,

    /// Error category for filtering.
    pub error_category: ErrorCategory,

    /// Number of retry attempts.
    pub retry_count: u32,

    /// When the failure occurred.
    pub failed_at: DateTime<Utc>,

    /// When the event was first received.
    pub received_at: Option<DateTime<Utc>>,
}

/// Category of error for DLQ filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// Malformed data that cannot be parsed.
    Malformed,
    /// Schema validation failure.
    ValidationFailed,
    /// Processing logic error.
    ProcessingError,
    /// Database write error.
    PersistenceError,
    /// Unknown/unexpected error.
    Unknown,
}

impl ErrorCategory {
    /// The canonical string stored in the database and accepted by filters.
    ///
    /// Everything that writes or queries a category goes through here. Two call
    /// sites formatting the enum independently is how you end up with
    /// "Malformed" in some rows and "malformed" in others, and a category filter
    /// that quietly returns half the entries.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCategory::Malformed => "malformed",
            ErrorCategory::ValidationFailed => "validation",
            ErrorCategory::ProcessingError => "processing",
            ErrorCategory::PersistenceError => "persistence",
            ErrorCategory::Unknown => "unknown",
        }
    }

    /// Parse the string written by `as_str`.
    pub fn from_str_name(name: &str) -> Option<Self> {
        match name {
            "malformed" => Some(ErrorCategory::Malformed),
            "validation" => Some(ErrorCategory::ValidationFailed),
            "processing" => Some(ErrorCategory::ProcessingError),
            "persistence" => Some(ErrorCategory::PersistenceError),
            "unknown" => Some(ErrorCategory::Unknown),
            _ => None,
        }
    }

    /// Classify an error.
    pub fn of(error: &crate::Error) -> Self {
        match error {
            crate::Error::InvalidMessage(_) | crate::Error::EventParse(_) => {
                ErrorCategory::Malformed
            }
            crate::Error::EventValidation(_) => ErrorCategory::ValidationFailed,
            crate::Error::DatabaseQuery(_) | crate::Error::DatabaseConnection(_) => {
                ErrorCategory::PersistenceError
            }
            crate::Error::Serialization(_) => ErrorCategory::Unknown,
            _ => ErrorCategory::ProcessingError,
        }
    }
}

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FailedEvent {
    /// Create a failed event from a raw event.
    pub fn from_raw(raw: RawEvent, error: &crate::Error) -> Self {
        Self {
            raw_payload: raw.payload.to_vec(),
            sequence: Some(raw.sequence),
            slot: Some(raw.slot),
            parent_slot: raw.parent_slot,
            kind: Some(raw.kind),
            error_message: error.to_string(),
            error_category: ErrorCategory::of(error),
            retry_count: 0,
            failed_at: Utc::now(),
            received_at: Some(raw.received_at),
        }
    }

    /// Increment retry count and return whether retries are exhausted.
    pub fn increment_retry(&mut self, max_retries: u32) -> bool {
        self.retry_count += 1;
        self.retry_count > max_retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_kind_priority() {
        assert!(EventKind::SlotUpdate.priority() < EventKind::Transaction.priority());
        assert!(EventKind::BlockMeta.priority() < EventKind::AccountUpdate.priority());
    }

    #[test]
    fn test_raw_event_creation() {
        let raw = RawEvent::new(
            SequenceNumber(1),
            EventKind::Transaction,
            100,
            bytes::Bytes::from_static(b"test"),
        );

        assert_eq!(raw.sequence.0, 1);
        assert_eq!(raw.slot, 100);
        assert_eq!(raw.payload_size(), 4);
    }

    #[test]
    fn test_indexed_event_hash() {
        let raw1 = RawEvent::new(
            SequenceNumber(1),
            EventKind::Transaction,
            100,
            bytes::Bytes::from_static(b"test"),
        );
        let raw2 = RawEvent::new(
            SequenceNumber(1),
            EventKind::Transaction,
            100,
            bytes::Bytes::from_static(b"test"),
        );

        let event1 = IndexedEvent::from_raw(raw1, serde_json::json!({}), SequenceNumber(1));
        let event2 = IndexedEvent::from_raw(raw2, serde_json::json!({}), SequenceNumber(99));

        // Different assigned sequences, same content: the hash tracks content.
        assert_eq!(event1.event_hash, event2.event_hash);
        // ...and identity tracks the source sequence, not the assigned one.
        assert_eq!(event1.identity(), event2.identity());
    }

    #[test]
    fn event_hash_is_stable_across_runs() {
        // Pinned so a change to the hashing scheme is a deliberate, visible break
        // rather than a silent one that orphans every previously written row.
        let raw = RawEvent::new(
            SequenceNumber(7),
            EventKind::Transaction,
            4_242,
            bytes::Bytes::from_static(b"stable"),
        );
        let event = IndexedEvent::from_raw(raw, serde_json::json!({}), SequenceNumber(1));
        assert_eq!(event.event_hash.len(), 32);
        assert_eq!(event.event_hash, "f506a53f9c0e815d65a15b28e11fc222");
    }

    #[test]
    fn error_categories_round_trip_through_their_stored_form() {
        for category in [
            ErrorCategory::Malformed,
            ErrorCategory::ValidationFailed,
            ErrorCategory::ProcessingError,
            ErrorCategory::PersistenceError,
            ErrorCategory::Unknown,
        ] {
            assert_eq!(
                ErrorCategory::from_str_name(category.as_str()),
                Some(category)
            );
        }
    }

    #[test]
    fn classification_is_the_same_whichever_path_produced_it() {
        let error = crate::Error::EventParse("bad".into());
        let direct = ErrorCategory::of(&error);
        let raw = RawEvent::new(
            SequenceNumber(1),
            EventKind::Transaction,
            1,
            bytes::Bytes::from_static(b"x"),
        );
        let via_failed = FailedEvent::from_raw(raw, &error).error_category;
        assert_eq!(direct, via_failed);
        assert_eq!(direct.as_str(), "malformed");
    }

    #[test]
    fn event_kind_names_round_trip() {
        for kind in [
            EventKind::SlotUpdate,
            EventKind::AccountUpdate,
            EventKind::Transaction,
            EventKind::BlockMeta,
            EventKind::Entry,
        ] {
            assert_eq!(EventKind::from_str_name(kind.as_str()), Some(kind));
        }
    }
}
