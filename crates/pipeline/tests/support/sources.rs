//! Sources the tests drive the pipeline with, beyond the scripted chain.
//!
//! Both implement the same `EventSource` the binary uses, so events from them
//! travel the production path: the ingester's buffer, the processor's parse and
//! fork detection, and the persister's idempotent write.

use async_trait::async_trait;
use slot_stream_common::{EventKind, EventOrigin, RawEvent, Result, SequenceNumber};
use slot_stream_ingester::EventSource;
use tokio::sync::mpsc;

/// A source that emits a fixed list of raw events, malformed payloads included.
///
/// The scripted source builds its payloads from `serde_json::Value`, so it can
/// only ever emit valid JSON. Exercising the DLQ needs bytes that are not.
pub struct RawSource {
    label: String,
    events: std::vec::IntoIter<RawEvent>,
}

impl RawSource {
    pub fn new(label: impl Into<String>, events: Vec<RawEvent>) -> Self {
        Self {
            label: label.into(),
            events: events.into_iter(),
        }
    }
}

#[async_trait]
impl EventSource for RawSource {
    async fn next_event(&mut self) -> Option<Result<RawEvent>> {
        self.events.next().map(Ok)
    }

    fn describe(&self) -> String {
        self.label.clone()
    }
}

/// A source fed by a channel.
///
/// Used where something else produces the events — a DLQ replay, or the
/// backfiller — and they still have to enter through the ordinary path rather
/// than being written directly.
pub struct ChannelSource {
    label: String,
    rx: mpsc::Receiver<RawEvent>,
}

impl ChannelSource {
    pub fn new(label: impl Into<String>, rx: mpsc::Receiver<RawEvent>) -> Self {
        Self {
            label: label.into(),
            rx,
        }
    }
}

#[async_trait]
impl EventSource for ChannelSource {
    async fn next_event(&mut self) -> Option<Result<RawEvent>> {
        self.rx.recv().await.map(Ok)
    }

    fn describe(&self) -> String {
        self.label.clone()
    }
}

/// A well-formed transaction event.
pub fn event(seq: u64, slot: u64, parent: u64, label: &str) -> RawEvent {
    let body = serde_json::json!({ "label": label });
    RawEvent::new(
        SequenceNumber(seq),
        EventKind::Transaction,
        slot,
        bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
    )
    .with_parent(parent)
}

/// An event whose payload is not JSON, so the processor must reject it.
pub fn malformed(seq: u64, slot: u64, parent: u64) -> RawEvent {
    RawEvent::new(
        SequenceNumber(seq),
        EventKind::Transaction,
        slot,
        bytes::Bytes::from_static(b"{ this is not json"),
    )
    .with_parent(parent)
}

/// The same event, marked as a backfill repair rather than live chain news.
pub fn backfilled(seq: u64, slot: u64, parent: u64, label: &str) -> RawEvent {
    event(seq, slot, parent, label).with_origin(EventOrigin::Backfill)
}
