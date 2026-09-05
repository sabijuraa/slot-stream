//! gRPC client and server for the slot-stream wire protocol.

use crate::scripted::ChainScript;
use crate::source::EventSource;
use async_trait::async_trait;
use slot_stream_common::{Error, EventKind, RawEvent, Result, SequenceNumber};
use std::pin::Pin;
use std::time::Duration;
use tokio_stream::Stream;
use tonic::transport::{Channel, Endpoint};
use tracing::{info, warn};

/// Generated protobuf types.
pub mod pb {
    tonic::include_proto!("slot_stream.v1");
}

use pb::slot_stream_client::SlotStreamClient;
use pb::slot_stream_server::{SlotStream, SlotStreamServer};
use pb::{StatusRequest, StatusResponse, StreamEvent, SubscribeRequest};

/// Converts a wire event into the pipeline's raw event.
fn from_wire(event: StreamEvent) -> Result<RawEvent> {
    let kind = EventKind::from_str_name(&event.kind)
        .ok_or_else(|| Error::InvalidMessage(format!("unknown event kind {:?}", event.kind)))?;

    let mut raw = RawEvent::new(
        SequenceNumber(event.sequence),
        kind,
        event.slot,
        bytes::Bytes::from(event.payload),
    );
    if let Some(parent) = event.parent_slot {
        raw = raw.with_parent(parent);
    }

    if let Some(received) = chrono::DateTime::from_timestamp_millis(event.received_at_ms) {
        raw.received_at = received;
    }

    Ok(raw)
}

fn to_wire(event: &RawEvent) -> StreamEvent {
    StreamEvent {
        sequence: event.sequence.0,
        kind: event.kind.as_str().to_string(),
        slot: event.slot,
        parent_slot: event.parent_slot,
        payload: event.payload.to_vec(),
        received_at_ms: event.received_at.timestamp_millis(),
    }
}

/// A source backed by a gRPC subscription.
pub struct GrpcEventSource {
    endpoint: String,
    kinds: Vec<String>,
    stream: tonic::Streaming<StreamEvent>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl GrpcEventSource {
    /// Connect and subscribe from the beginning of what the server holds.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        Self::connect_from(endpoint, 0, Vec::new()).await
    }

    /// Connect and subscribe, resuming after `from_sequence`.
    pub async fn connect_from(
        endpoint: &str,
        from_sequence: u64,
        kinds: Vec<String>,
    ) -> Result<Self> {
        let connect_timeout = Duration::from_secs(10);
        let request_timeout = Duration::from_secs(60);

        let channel = Self::channel(endpoint, connect_timeout, request_timeout).await?;
        let stream = Self::subscribe(channel, from_sequence, kinds.clone()).await?;

        info!(endpoint, from_sequence, "subscribed to slot stream");

        Ok(Self {
            endpoint: endpoint.to_string(),
            kinds,
            stream,
            connect_timeout,
            request_timeout,
        })
    }

    async fn channel(
        endpoint: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Channel> {
        Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| Error::GrpcConnection(e.to_string()))?
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .http2_keep_alive_interval(Duration::from_secs(15))
            .keep_alive_while_idle(true)
            .connect()
            .await
            .map_err(|e| Error::GrpcConnection(e.to_string()))
    }

    async fn subscribe(
        channel: Channel,
        from_sequence: u64,
        kinds: Vec<String>,
    ) -> Result<tonic::Streaming<StreamEvent>> {
        let mut client = SlotStreamClient::new(channel);
        let response = client
            .subscribe(SubscribeRequest {
                from_sequence,
                kinds,
            })
            .await
            .map_err(|e| Error::GrpcConnection(e.to_string()))?;
        Ok(response.into_inner())
    }

    /// The endpoint this source is attached to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl EventSource for GrpcEventSource {
    async fn next_event(&mut self) -> Option<Result<RawEvent>> {
        match self.stream.message().await {
            Ok(Some(event)) => Some(from_wire(event)),
            Ok(None) => None,
            Err(status) => Some(Err(Error::StreamEnded(status.to_string()))),
        }
    }

    fn describe(&self) -> String {
        format!("grpc({})", self.endpoint)
    }

    async fn reconnect(&mut self, from_sequence: u64) -> Result<()> {
        warn!(
            endpoint = %self.endpoint,
            from_sequence,
            "reconnecting to slot stream"
        );

        let channel =
            Self::channel(&self.endpoint, self.connect_timeout, self.request_timeout).await?;
        self.stream = Self::subscribe(channel, from_sequence, self.kinds.clone()).await?;

        info!(endpoint = %self.endpoint, from_sequence, "reconnected");
        Ok(())
    }

    fn is_resumable(&self) -> bool {
        true
    }
}

/// Serves a [`ChainScript`] over the wire protocol.
///
/// This is what the compose stack and the end-to-end tests point the indexer at.
/// It is a real gRPC server speaking the real protocol; only the chain it emits
/// is chosen rather than observed.
pub struct ChainSourceServer {
    script: ChainScript,
    pace: Option<Duration>,
}

impl ChainSourceServer {
    /// Serve `script`.
    pub fn new(script: ChainScript) -> Self {
        Self { script, pace: None }
    }

    /// Emit at a fixed interval instead of as fast as the client can read.
    pub fn paced(mut self, interval: Duration) -> Self {
        self.pace = Some(interval);
        self
    }

    /// Wrap in a tonic service.
    pub fn into_service(self) -> SlotStreamServer<Self> {
        SlotStreamServer::new(self)
    }

    /// Every event the script produces, with sequence numbers assigned.
    fn events(&self) -> Vec<RawEvent> {
        let mut out = Vec::with_capacity(self.script.event_count());
        let mut sequence = 1u64;

        for slot in &self.script.slots {
            for event in &slot.events {
                let payload = serde_json::to_vec(&event.body).unwrap_or_default();
                out.push(
                    RawEvent::new(
                        SequenceNumber(sequence),
                        event.kind,
                        slot.slot,
                        bytes::Bytes::from(payload),
                    )
                    .with_parent(slot.parent),
                );
                sequence += 1;
            }
        }

        out
    }
}

type EventStreamResponse =
    Pin<Box<dyn Stream<Item = std::result::Result<StreamEvent, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl SlotStream for ChainSourceServer {
    type SubscribeStream = EventStreamResponse;

    async fn subscribe(
        &self,
        request: tonic::Request<SubscribeRequest>,
    ) -> std::result::Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let request = request.into_inner();

        let kinds: Vec<String> = request.kinds;
        let events: Vec<StreamEvent> = self
            .events()
            .iter()
            // Resume is exclusive: the client already has everything at or below.
            .filter(|e| e.sequence.0 > request.from_sequence)
            .filter(|e| kinds.is_empty() || kinds.iter().any(|k| k == e.kind.as_str()))
            .map(to_wire)
            .collect();

        info!(
            from_sequence = request.from_sequence,
            emitting = events.len(),
            "subscribe accepted"
        );

        let pace = self.pace;
        let stream = async_stream::stream! {
            for event in events {
                if let Some(pace) = pace {
                    tokio::time::sleep(pace).await;
                }
                yield Ok(event);
            }
        };

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn get_status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> std::result::Result<tonic::Response<StatusResponse>, tonic::Status> {
        let events = self.events();
        Ok(tonic::Response::new(StatusResponse {
            highest_sequence: events.last().map(|e| e.sequence.0).unwrap_or(0),
            highest_slot: events.iter().map(|e| e.slot).max().unwrap_or(0),
            active: true,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_round_trip_preserves_everything_fork_detection_needs() {
        let original = RawEvent::new(
            SequenceNumber(42),
            EventKind::Transaction,
            1_000,
            bytes::Bytes::from_static(b"{}"),
        )
        .with_parent(999);

        let restored = from_wire(to_wire(&original)).unwrap();

        assert_eq!(restored.sequence, original.sequence);
        assert_eq!(restored.kind, original.kind);
        assert_eq!(restored.slot, original.slot);
        assert_eq!(restored.parent_slot, Some(999));
        assert_eq!(restored.payload, original.payload);
    }

    #[test]
    fn an_unknown_kind_is_rejected_rather_than_guessed() {
        let event = StreamEvent {
            sequence: 1,
            kind: "Nonsense".into(),
            slot: 1,
            parent_slot: Some(0),
            payload: vec![],
            received_at_ms: 0,
        };
        assert!(from_wire(event).is_err());
    }
}
