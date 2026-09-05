//! The stream source abstraction.
//!
//! Everything upstream of the pipeline arrives through [`EventSource`]. There are
//! two implementations: a gRPC client for a real Geyser-side source, and a
//! scripted source that emits a chain shape you describe.
//!
//! The scripted source is not a test double. It implements the same trait, is
//! driven by the same ingester, and its events travel the same parse, fork
//! detection, and write path as anything from the wire. That is deliberate: a
//! reorg proof that bypassed the production pipeline would prove nothing about
//! the production pipeline.

use async_trait::async_trait;
use slot_stream_common::{RawEvent, Result};

/// A source of stream events.
#[async_trait]
pub trait EventSource: Send {
    /// The next event, or `None` when the source is exhausted.
    ///
    /// Returning `Err` means the stream broke and may be resumable; the ingester
    /// decides whether to reconnect.
    async fn next_event(&mut self) -> Option<Result<RawEvent>>;

    /// A label for logs and metrics.
    fn describe(&self) -> String;

    /// Reconnect after an error, resuming after `from_sequence`.
    ///
    /// The default is "not resumable", which makes the ingester surface the error
    /// rather than silently spin.
    async fn reconnect(&mut self, _from_sequence: u64) -> Result<()> {
        Err(slot_stream_common::Error::StreamEnded(
            "source does not support reconnection".into(),
        ))
    }

    /// Whether `reconnect` is worth calling.
    fn is_resumable(&self) -> bool {
        false
    }
}
