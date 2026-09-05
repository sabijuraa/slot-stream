//! Processor state and statistics.

use chrono::{DateTime, Utc};
use slot_stream_common::SequenceNumber;

/// Running state of the processor.
#[derive(Debug, Clone)]
pub struct ProcessorState {
    /// Events emitted downstream.
    pub events_processed: u64,

    /// Events routed to the DLQ.
    pub events_dlq: u64,

    /// Events that failed for a reason other than a DLQ-able parse error.
    pub events_failed: u64,

    /// Reorgs detected.
    pub reorgs_detected: u64,

    /// Total slots rolled back across all reorgs.
    pub slots_rolled_back: u64,

    /// Deepest single rollback seen.
    pub max_rollback_depth: usize,

    /// Slot of the most recent reorg.
    pub last_reorg_slot: Option<u64>,

    /// Most recent slot processed.
    pub last_slot: Option<u64>,

    /// Most recent assigned sequence.
    pub last_seq: Option<SequenceNumber>,

    /// When processing started.
    pub started_at: DateTime<Utc>,
}

impl ProcessorState {
    /// Create a new processor state.
    pub fn new() -> Self {
        Self {
            events_processed: 0,
            events_dlq: 0,
            events_failed: 0,
            reorgs_detected: 0,
            slots_rolled_back: 0,
            max_rollback_depth: 0,
            last_reorg_slot: None,
            last_slot: None,
            last_seq: None,
            started_at: Utc::now(),
        }
    }

    /// Statistics snapshot.
    pub fn stats(&self) -> ProcessorStats {
        ProcessorStats {
            events_processed: self.events_processed,
            events_dlq: self.events_dlq,
            events_failed: self.events_failed,
            reorgs_detected: self.reorgs_detected,
            slots_rolled_back: self.slots_rolled_back,
            max_rollback_depth: self.max_rollback_depth,
            last_reorg_slot: self.last_reorg_slot,
            last_slot: self.last_slot,
            last_seq: self.last_seq,
            uptime_secs: (Utc::now() - self.started_at).num_seconds().max(0) as u64,
            events_per_sec: self.events_per_second(),
        }
    }

    fn events_per_second(&self) -> f64 {
        let elapsed = (Utc::now() - self.started_at).num_milliseconds() as f64 / 1000.0;
        if elapsed > 0.0 {
            self.events_processed as f64 / elapsed
        } else {
            0.0
        }
    }
}

impl Default for ProcessorState {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics snapshot, safe to serialise for the admin endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProcessorStats {
    pub events_processed: u64,
    pub events_dlq: u64,
    pub events_failed: u64,
    pub reorgs_detected: u64,
    pub slots_rolled_back: u64,
    pub max_rollback_depth: usize,
    pub last_reorg_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub last_seq: Option<SequenceNumber>,
    pub uptime_secs: u64,
    pub events_per_sec: f64,
}
