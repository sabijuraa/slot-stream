//! Sequence number tracking for exactly-once semantics.
//!
//! Sequence numbers provide:
//! - Ordering guarantees (monotonically increasing)
//! - Gap detection (missed events)
//! - Duplicate detection (idempotent processing)
//! - Reorg detection (sequence regression)
//!
//! ## Invariants
//!
//! 1. Sequence numbers are globally unique per stream
//! 2. Gaps indicate missed events that need backfill
//! 3. Regression indicates a reorg or reconnection to earlier point

use serde::{Deserialize, Serialize};
use std::fmt;

/// A monotonically increasing sequence number assigned to each event.
///
/// Sequence numbers are assigned by the Geyser plugin and represent
/// the order of events as observed by the validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SequenceNumber(pub u64);

impl SequenceNumber {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    /// Create a new sequence number.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the inner value.
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Get the next sequence number.
    pub fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Get the previous sequence number, if any.
    pub fn prev(&self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }

    /// Calculate the gap between this and another sequence number.
    /// Returns None if other is less than self.
    pub fn gap_to(&self, other: Self) -> Option<u64> {
        other.0.checked_sub(self.0)
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "seq:{}", self.0)
    }
}

impl From<u64> for SequenceNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// A range of sequence numbers, inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceRange {
    pub start: SequenceNumber,
    pub end: SequenceNumber,
}

impl SequenceRange {
    /// Create a new sequence range.
    pub fn new(start: SequenceNumber, end: SequenceNumber) -> Self {
        debug_assert!(start <= end, "start must be <= end");
        Self { start, end }
    }

    /// Create a single-element range.
    pub fn single(seq: SequenceNumber) -> Self {
        Self {
            start: seq,
            end: seq,
        }
    }

    /// Check if a sequence number is within this range.
    pub fn contains(&self, seq: SequenceNumber) -> bool {
        seq >= self.start && seq <= self.end
    }

    /// Get the number of sequence numbers in this range.
    pub fn len(&self) -> u64 {
        self.end.0 - self.start.0 + 1
    }

    /// Check if the range is empty (should never happen with valid construction).
    pub fn is_empty(&self) -> bool {
        false // Ranges are always non-empty by construction
    }

    /// Iterate over all sequence numbers in this range.
    pub fn iter(&self) -> impl Iterator<Item = SequenceNumber> {
        (self.start.0..=self.end.0).map(SequenceNumber)
    }
}

/// Assigns the monotonic sequence numbers that define downstream ordering.
///
/// The stream carries its own sequence, but that number describes the order the
/// *validator* observed events in, and it restarts, gaps, and repeats across
/// reconnects and forks. The ordering the database needs is the order this
/// pipeline committed things in, so the processor stamps its own.
///
/// On restart the assigner resumes from the highest value already persisted, so
/// numbers never collide with committed rows and never go backwards. Note that a
/// replayed source event gets a *new* assigned number, which is why row identity
/// is `(slot, source_seq)` rather than this value — see the persister.
#[derive(Debug)]
pub struct SequenceAssigner {
    next: std::sync::atomic::AtomicU64,
}

impl SequenceAssigner {
    /// Start from one, for a fresh database.
    pub fn new() -> Self {
        Self::resuming_from(SequenceNumber::ZERO)
    }

    /// Resume after `last`, which should be the highest sequence in the database.
    pub fn resuming_from(last: SequenceNumber) -> Self {
        Self {
            next: std::sync::atomic::AtomicU64::new(last.0.saturating_add(1)),
        }
    }

    /// Take the next sequence number.
    pub fn next(&self) -> SequenceNumber {
        SequenceNumber(self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    /// The value the next call to `next` will return, without consuming it.
    pub fn peek(&self) -> SequenceNumber {
        SequenceNumber(self.next.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// The highest number handed out so far, if any.
    pub fn last_assigned(&self) -> Option<SequenceNumber> {
        self.peek().prev()
    }
}

impl Default for SequenceAssigner {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks sequence numbers to detect gaps, duplicates, and regressions.
///
/// The tracker maintains:
/// - The last seen sequence number
/// - Detected gaps that need backfill
/// - Statistics for monitoring
#[derive(Debug)]
pub struct SequenceTracker {
    /// Last successfully processed sequence number
    last_seen: Option<SequenceNumber>,

    /// Detected gaps that need backfill
    gaps: Vec<SequenceRange>,

    /// Total events processed
    events_processed: u64,

    /// Total gaps detected
    gaps_detected: u64,

    /// Total duplicates skipped
    duplicates_skipped: u64,
}

impl SequenceTracker {
    /// Create a new sequence tracker.
    pub fn new() -> Self {
        Self {
            last_seen: None,
            gaps: Vec::new(),
            events_processed: 0,
            gaps_detected: 0,
            duplicates_skipped: 0,
        }
    }

    /// Create a tracker starting from a known sequence number.
    ///
    /// Use this when resuming from a checkpoint.
    pub fn from_checkpoint(last_seen: SequenceNumber) -> Self {
        Self {
            last_seen: Some(last_seen),
            gaps: Vec::new(),
            events_processed: 0,
            gaps_detected: 0,
            duplicates_skipped: 0,
        }
    }

    /// Get the last seen sequence number.
    pub fn last_seen(&self) -> Option<SequenceNumber> {
        self.last_seen
    }

    /// Get pending gaps that need backfill.
    pub fn pending_gaps(&self) -> &[SequenceRange] {
        &self.gaps
    }

    /// Check if there are any pending gaps.
    pub fn has_gaps(&self) -> bool {
        !self.gaps.is_empty()
    }

    /// Process a new sequence number.
    ///
    /// Returns:
    /// - `Ok(SequenceResult::Processed)` if this is the expected next sequence
    /// - `Ok(SequenceResult::Duplicate)` if this was already seen
    /// - `Ok(SequenceResult::Gap { .. })` if there's a gap (event still processed)
    /// - `Err(...)` if sequence went backwards (possible reorg)
    pub fn process(&mut self, seq: SequenceNumber) -> crate::Result<SequenceResult> {
        match self.last_seen {
            None => {
                // First event
                self.last_seen = Some(seq);
                self.events_processed += 1;
                Ok(SequenceResult::Processed)
            }
            Some(last) => {
                if seq == last {
                    // Exact duplicate
                    self.duplicates_skipped += 1;
                    Ok(SequenceResult::Duplicate)
                } else if seq < last {
                    // Regression - could be a reorg or reconnect
                    Err(crate::Error::SequenceRegression {
                        current: last.0,
                        received: seq.0,
                    })
                } else if seq == last.next() {
                    // Perfect - next in sequence
                    self.last_seen = Some(seq);
                    self.events_processed += 1;
                    Ok(SequenceResult::Processed)
                } else {
                    // Gap detected
                    let gap = SequenceRange::new(last.next(), SequenceNumber(seq.0 - 1));
                    self.gaps.push(gap);
                    self.gaps_detected += 1;
                    self.last_seen = Some(seq);
                    self.events_processed += 1;
                    Ok(SequenceResult::Gap { missing: gap })
                }
            }
        }
    }

    /// Mark a gap range as filled (after backfill).
    pub fn fill_gap(&mut self, range: SequenceRange) {
        self.gaps.retain(|g| {
            // Remove gaps that are fully contained in the filled range
            !(range.contains(g.start) && range.contains(g.end))
        });
    }

    /// Get statistics about sequence tracking.
    pub fn stats(&self) -> SequenceStats {
        SequenceStats {
            last_seen: self.last_seen,
            events_processed: self.events_processed,
            gaps_detected: self.gaps_detected,
            gaps_pending: self.gaps.len() as u64,
            duplicates_skipped: self.duplicates_skipped,
        }
    }
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of processing a sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceResult {
    /// Event was processed normally.
    Processed,
    /// Event was a duplicate and skipped.
    Duplicate,
    /// Gap detected; event was still processed, but backfill needed.
    Gap { missing: SequenceRange },
}

/// Statistics about sequence tracking.
#[derive(Debug, Clone)]
pub struct SequenceStats {
    pub last_seen: Option<SequenceNumber>,
    pub events_processed: u64,
    pub gaps_detected: u64,
    pub gaps_pending: u64,
    pub duplicates_skipped: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_number_ordering() {
        let a = SequenceNumber::new(1);
        let b = SequenceNumber::new(2);
        assert!(a < b);
        assert_eq!(a.next(), b);
        assert_eq!(b.prev(), Some(a));
        assert_eq!(SequenceNumber::ZERO.prev(), None);
    }

    #[test]
    fn test_sequence_range() {
        let range = SequenceRange::new(SequenceNumber(5), SequenceNumber(10));
        assert_eq!(range.len(), 6);
        assert!(range.contains(SequenceNumber(5)));
        assert!(range.contains(SequenceNumber(7)));
        assert!(range.contains(SequenceNumber(10)));
        assert!(!range.contains(SequenceNumber(4)));
        assert!(!range.contains(SequenceNumber(11)));
    }

    #[test]
    fn test_tracker_sequential() {
        let mut tracker = SequenceTracker::new();

        assert!(matches!(
            tracker.process(SequenceNumber(1)).unwrap(),
            SequenceResult::Processed
        ));
        assert!(matches!(
            tracker.process(SequenceNumber(2)).unwrap(),
            SequenceResult::Processed
        ));
        assert!(matches!(
            tracker.process(SequenceNumber(3)).unwrap(),
            SequenceResult::Processed
        ));

        assert!(!tracker.has_gaps());
        assert_eq!(tracker.stats().events_processed, 3);
    }

    #[test]
    fn test_tracker_gap_detection() {
        let mut tracker = SequenceTracker::new();

        tracker.process(SequenceNumber(1)).unwrap();
        // Skip 2, 3
        let result = tracker.process(SequenceNumber(4)).unwrap();

        match result {
            SequenceResult::Gap { missing } => {
                assert_eq!(missing.start.0, 2);
                assert_eq!(missing.end.0, 3);
                assert_eq!(missing.len(), 2);
            }
            _ => panic!("Expected gap"),
        }

        assert!(tracker.has_gaps());
    }

    #[test]
    fn test_tracker_duplicate() {
        let mut tracker = SequenceTracker::new();

        tracker.process(SequenceNumber(1)).unwrap();
        let result = tracker.process(SequenceNumber(1)).unwrap();

        assert!(matches!(result, SequenceResult::Duplicate));
        assert_eq!(tracker.stats().duplicates_skipped, 1);
    }

    #[test]
    fn assigner_is_monotonic_and_starts_at_one() {
        let assigner = SequenceAssigner::new();
        assert_eq!(assigner.next(), SequenceNumber(1));
        assert_eq!(assigner.next(), SequenceNumber(2));
        assert_eq!(assigner.next(), SequenceNumber(3));
        assert_eq!(assigner.last_assigned(), Some(SequenceNumber(3)));
    }

    #[test]
    fn assigner_resumes_past_the_persisted_maximum() {
        let assigner = SequenceAssigner::resuming_from(SequenceNumber(4_096));
        assert_eq!(assigner.next(), SequenceNumber(4_097));
    }

    #[test]
    fn assigner_hands_out_each_number_exactly_once_under_contention() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let assigner = Arc::new(SequenceAssigner::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let assigner = Arc::clone(&assigner);
            handles.push(std::thread::spawn(move || {
                (0..1_000).map(|_| assigner.next()).collect::<Vec<_>>()
            }));
        }

        let mut seen = HashSet::new();
        for handle in handles {
            for seq in handle.join().expect("thread panicked") {
                assert!(seen.insert(seq), "duplicate sequence {seq}");
            }
        }
        assert_eq!(seen.len(), 8_000);
    }

    #[test]
    fn test_tracker_regression() {
        let mut tracker = SequenceTracker::new();

        tracker.process(SequenceNumber(5)).unwrap();
        let result = tracker.process(SequenceNumber(3));

        assert!(result.is_err());
    }
}
