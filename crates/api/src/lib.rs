//! # slot-stream-api
//!
//! The read API over indexed data, plus health and metrics endpoints.
//!
//! Every data query filters on `is_valid = true`, so a caller only ever sees the
//! canonical chain. Rows orphaned by a reorg remain in the table for audit but
//! are invisible here, which is what lets a client treat this API as a
//! consistent view without knowing anything about forks.

pub mod query;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use slot_stream_common::SequenceNumber;
use std::sync::Arc;
use tracing::error;

pub use query::{EventCounts, EventQuery, EventRecord, EventStore, SlotRecord};

/// Shared state for the API handlers.
#[derive(Clone)]
pub struct ApiState {
    store: EventStore,
    /// Renders the Prometheus exposition format, when metrics are enabled.
    metrics: Option<Arc<metrics_exporter_prometheus::PrometheusHandle>>,
    started_at: chrono::DateTime<chrono::Utc>,
}

impl ApiState {
    /// Create API state over a store.
    pub fn new(store: EventStore) -> Self {
        Self {
            store,
            metrics: None,
            started_at: chrono::Utc::now(),
        }
    }

    /// Attach a Prometheus handle so `/metrics` serves real numbers.
    pub fn with_metrics(
        mut self,
        handle: metrics_exporter_prometheus::PrometheusHandle,
    ) -> Self {
        self.metrics = Some(Arc::new(handle));
        self
    }
}

/// Build the router.
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/metrics", get(metrics))
        .route("/v1/status", get(status))
        .route("/v1/events", get(list_events))
        .route("/v1/events/slot/:slot", get(events_by_slot))
        .route("/v1/events/signature/:signature", get(events_by_signature))
        .route("/v1/events/account/:account", get(events_by_account))
        .route("/v1/slots/:slot", get(slot))
        .route("/v1/chain/head", get(chain_head))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state)
}

/// An error surfaced to a client.
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn internal(err: slot_stream_common::Error) -> Self {
        // The client gets a stable message; the detail goes to the log, because
        // database error text can carry schema and connection information.
        error!(error = %err, "request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal error".into(),
        }
    }

    fn not_found(what: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: what.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

/// A list response with the count, so callers can spot truncation.
#[derive(Debug, Serialize)]
pub struct ListResponse<T> {
    pub count: usize,
    pub items: Vec<T>,
}

impl<T> ListResponse<T> {
    fn new(items: Vec<T>) -> Self {
        Self {
            count: items.len(),
            items,
        }
    }
}

async fn health_live() -> impl IntoResponse {
    // Liveness only asks whether the process is running.
    (StatusCode::OK, Json(serde_json::json!({ "status": "alive" })))
}

async fn health_ready(State(state): State<ApiState>) -> Response {
    // Readiness asks whether we can actually serve, which means the database.
    match state.store.ping().await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "ready" })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "status": "not ready", "reason": e.to_string() })),
        )
            .into_response(),
    }
}

async fn metrics(State(state): State<ApiState>) -> Response {
    match state.metrics {
        Some(handle) => (StatusCode::OK, handle.render()).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "metrics are not enabled\n",
        )
            .into_response(),
    }
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    uptime_secs: i64,
    chain_head: Option<u64>,
    events: EventCounts,
}

async fn status(State(state): State<ApiState>) -> ApiResult<Json<StatusResponse>> {
    let events = state.store.counts().await.map_err(ApiError::internal)?;
    let chain_head = state.store.chain_head().await.map_err(ApiError::internal)?;

    Ok(Json(StatusResponse {
        uptime_secs: (chrono::Utc::now() - state.started_at).num_seconds(),
        chain_head,
        events,
    }))
}

async fn list_events(
    State(state): State<ApiState>,
    Query(query): Query<EventQuery>,
) -> ApiResult<Json<ListResponse<EventRecord>>> {
    let events = state
        .store
        .list_events(&query)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(ListResponse::new(events)))
}

#[derive(Debug, serde::Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

async fn events_by_slot(
    State(state): State<ApiState>,
    Path(slot): Path<u64>,
    Query(limit): Query<LimitQuery>,
) -> ApiResult<Json<ListResponse<EventRecord>>> {
    let events = state
        .store
        .events_by_slot(slot, limit.limit)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(ListResponse::new(events)))
}

async fn events_by_signature(
    State(state): State<ApiState>,
    Path(signature): Path<String>,
) -> ApiResult<Json<ListResponse<EventRecord>>> {
    let events = state
        .store
        .events_by_signature(&signature)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(ListResponse::new(events)))
}

async fn events_by_account(
    State(state): State<ApiState>,
    Path(account): Path<String>,
    Query(limit): Query<LimitQuery>,
) -> ApiResult<Json<ListResponse<EventRecord>>> {
    let events = state
        .store
        .events_by_account(&account, limit.limit)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(ListResponse::new(events)))
}

async fn slot(
    State(state): State<ApiState>,
    Path(slot): Path<u64>,
) -> ApiResult<Json<SlotRecord>> {
    state
        .store
        .slot(slot)
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("slot {slot} is not indexed")))
}

async fn chain_head(State(state): State<ApiState>) -> ApiResult<Json<serde_json::Value>> {
    let head = state.store.chain_head().await.map_err(ApiError::internal)?;
    Ok(Json(serde_json::json!({ "head": head })))
}

/// Convenience for callers that want a sequence-range query.
pub async fn events_in_sequence_range(
    store: &EventStore,
    from: u64,
    to: u64,
    limit: Option<u32>,
) -> slot_stream_common::Result<Vec<EventRecord>> {
    store
        .events_by_sequence_range(SequenceNumber(from), SequenceNumber(to), limit)
        .await
}
