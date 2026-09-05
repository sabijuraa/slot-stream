//! Error types for the slot-stream pipeline.
//!
//! All errors are designed to be actionable - they indicate whether
//! the operation can be retried, should go to DLQ, or is fatal.

use thiserror::Error;

/// Result type alias using our Error type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in the slot-stream pipeline.
#[derive(Error, Debug)]
pub enum Error {
    // === Ingestion Errors ===
    /// gRPC connection failed - retryable with backoff
    #[error("gRPC connection error: {0}")]
    GrpcConnection(String),

    /// Stream ended unexpectedly - reconnect required
    #[error("gRPC stream ended: {0}")]
    StreamEnded(String),

    /// Invalid message format from gRPC - send to DLQ
    #[error("Invalid gRPC message: {0}")]
    InvalidMessage(String),

    // === Sequence Errors ===
    /// Gap detected in sequence numbers - may indicate missed events
    #[error("Sequence gap detected: expected {expected}, got {actual}")]
    SequenceGap { expected: u64, actual: u64 },

    /// Duplicate sequence number - idempotent, can skip
    #[error("Duplicate sequence number: {0}")]
    DuplicateSequence(u64),

    /// Sequence went backwards - possible reorg or bug
    #[error("Sequence regression: current {current}, received {received}")]
    SequenceRegression { current: u64, received: u64 },

    // === Reorg Errors ===
    /// Fork detected - requires rollback
    #[error("Fork detected at slot {slot}: expected parent {expected}, got {actual}")]
    ForkDetected {
        slot: u64,
        expected: u64,
        actual: u64,
    },

    /// Rollback failed - critical error
    #[error("Rollback failed at slot {slot}: {reason}")]
    RollbackFailed { slot: u64, reason: String },

    // === Processing Errors ===
    /// Event parsing failed - send to DLQ
    #[error("Event parse error: {0}")]
    EventParse(String),

    /// Event validation failed - send to DLQ
    #[error("Event validation error: {0}")]
    EventValidation(String),

    /// Processing timeout - may retry
    #[error("Processing timeout after {0:?}")]
    ProcessingTimeout(std::time::Duration),

    // === Persistence Errors ===
    /// Database connection error - retryable
    #[error("Database connection error: {0}")]
    DatabaseConnection(String),

    /// Database query error - may be retryable
    #[error("Database query error: {0}")]
    DatabaseQuery(String),

    /// Constraint violation - idempotent write, can skip
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    /// Serialization error - send to DLQ
    #[error("Serialization error: {0}")]
    Serialization(String),

    // === DLQ Errors ===
    /// Failed to write to DLQ - critical
    #[error("DLQ write failed: {0}")]
    DlqWriteFailed(String),

    // === Backfill Errors ===
    /// Backfill cursor error
    #[error("Backfill cursor error: {0}")]
    BackfillCursor(String),

    /// Backfill range error
    #[error("Invalid backfill range: {start} to {end}")]
    BackfillRange { start: u64, end: u64 },

    // === Configuration Errors ===
    /// Invalid configuration
    #[error("Configuration error: {0}")]
    Configuration(String),

    // === Channel Errors ===
    /// Channel closed - shutdown in progress
    #[error("Channel closed")]
    ChannelClosed,

    /// Channel full - backpressure triggered
    #[error("Channel full, backpressure active")]
    ChannelFull,
}

impl Error {
    /// Returns true if this error indicates the operation should be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::GrpcConnection(_)
                | Error::StreamEnded(_)
                | Error::DatabaseConnection(_)
                | Error::ProcessingTimeout(_)
                | Error::ChannelFull
        )
    }

    /// Returns true if this error should result in the event going to DLQ.
    pub fn should_dlq(&self) -> bool {
        matches!(
            self,
            Error::InvalidMessage(_)
                | Error::EventParse(_)
                | Error::EventValidation(_)
                | Error::Serialization(_)
        )
    }

    /// Returns true if this error is fatal and requires operator intervention.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Error::RollbackFailed { .. }
                | Error::DlqWriteFailed(_)
                | Error::Configuration(_)
        )
    }

    /// Returns true if this is a duplicate that can be safely skipped.
    pub fn is_duplicate(&self) -> bool {
        matches!(
            self,
            Error::DuplicateSequence(_) | Error::DuplicateKey(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification() {
        let retryable = Error::GrpcConnection("timeout".into());
        assert!(retryable.is_retryable());
        assert!(!retryable.should_dlq());
        assert!(!retryable.is_fatal());

        let dlq = Error::EventParse("invalid json".into());
        assert!(!dlq.is_retryable());
        assert!(dlq.should_dlq());
        assert!(!dlq.is_fatal());

        let fatal = Error::RollbackFailed {
            slot: 100,
            reason: "db error".into(),
        };
        assert!(!fatal.is_retryable());
        assert!(!fatal.should_dlq());
        assert!(fatal.is_fatal());
    }
}
