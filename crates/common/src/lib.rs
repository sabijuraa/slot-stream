//! # slot-stream-common
//!
//! Shared types, sequence numbers, slot metadata, and error definitions
//! for the slot-stream indexing pipeline.
//!
//! ## Core Concepts
//!
//! - **Slot**: A Solana time unit (~400ms), contains transactions
//! - **Sequence Number**: Monotonic counter for ordering and gap detection
//! - **SlotInfo**: Metadata about a slot including parent relationship for fork detection
//! - **IndexedEvent**: A processed event ready for persistence

pub mod config;
pub mod error;
pub mod event;
pub mod metrics;
pub mod rollback;
pub mod sequence;
pub mod slot;
pub mod types;

pub use config::{ApiSettings, ConfigError, PipelineConfig};
pub use error::{Error, Result};
pub use event::{ErrorCategory, EventKind, EventOrigin, FailedEvent, IndexedEvent, RawEvent};
pub use rollback::RollbackPlan;
pub use sequence::{
    SequenceAssigner, SequenceNumber, SequenceRange, SequenceResult, SequenceTracker,
};
pub use slot::{
    ChainStats, ChainUpdate, ForkInfo, IgnoreReason, SlotChainTracker, SlotInfo, SlotStatus,
};
pub use types::{AccountKey, Signature, TransactionMeta};
