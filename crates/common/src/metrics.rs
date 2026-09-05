//! Prometheus metrics for the slot-stream pipeline.
//!
//! All components record metrics for observability. This module provides
//! the metric definitions and helper functions.

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics for the ingester component.
pub struct IngesterMetrics;

impl IngesterMetrics {
    /// Record events received from the stream.
    pub fn events_received(count: u64) {
        metrics::counter!("ingester.events_received").increment(count);
    }

    /// Record events emitted to the buffer.
    pub fn events_emitted(count: u64) {
        metrics::counter!("ingester.events_emitted").increment(count);
    }

    /// Record events dropped due to backpressure.
    pub fn events_dropped(count: u64) {
        metrics::counter!("ingester.events_dropped").increment(count);
    }

    /// Record bytes received.
    pub fn bytes_received(bytes: u64) {
        metrics::counter!("ingester.bytes_received").increment(bytes);
    }

    /// Record a reconnection attempt.
    pub fn reconnect() {
        metrics::counter!("ingester.reconnects").increment(1);
    }

    /// Record sequence gap detected.
    pub fn sequence_gap(size: u64) {
        metrics::counter!("ingester.sequence_gaps").increment(1);
        metrics::histogram!("ingester.sequence_gap_size").record(size as f64);
    }

    /// Record buffer utilization.
    pub fn buffer_utilization(utilization: f64) {
        metrics::gauge!("ingester.buffer_utilization").set(utilization);
    }

    /// Record parse latency.
    pub fn parse_latency(duration_ms: f64) {
        metrics::histogram!("ingester.parse_latency_ms").record(duration_ms);
    }
}

/// Metrics for the processor component.
pub struct ProcessorMetrics;

impl ProcessorMetrics {
    /// Record events processed.
    pub fn events_processed(count: u64, kind: &str) {
        metrics::counter!("processor.events_processed", "kind" => kind.to_string())
            .increment(count);
    }

    /// Record events sent to DLQ.
    pub fn events_dlq(count: u64, category: &str) {
        metrics::counter!("processor.events_dlq", "category" => category.to_string())
            .increment(count);
    }

    /// Record reorg detected.
    pub fn reorg_detected(depth: usize) {
        metrics::counter!("processor.reorgs_detected").increment(1);
        metrics::histogram!("processor.reorg_depth").record(depth as f64);
    }

    /// Record rollback executed.
    pub fn rollback_executed(slots: usize, events: u64) {
        metrics::counter!("processor.rollbacks_executed").increment(1);
        metrics::histogram!("processor.rollback_slots").record(slots as f64);
        metrics::histogram!("processor.rollback_events").record(events as f64);
    }

    /// Record processing latency.
    pub fn processing_latency(duration_ms: f64) {
        metrics::histogram!("processor.processing_latency_ms").record(duration_ms);
    }

    /// Record processing lag (slots behind head).
    pub fn processing_lag(slots: u64) {
        metrics::gauge!("processor.processing_lag").set(slots as f64);
    }

    /// Record handler execution time.
    pub fn handler_latency(handler: &str, duration_ms: f64) {
        metrics::histogram!("processor.handler_latency_ms", "handler" => handler.to_string())
            .record(duration_ms);
    }
}

/// Metrics for the persister component.
pub struct PersisterMetrics;

impl PersisterMetrics {
    /// Record events written.
    pub fn events_written(count: u64) {
        metrics::counter!("persister.events_written").increment(count);
    }

    /// Record batches written.
    pub fn batches_written(count: u64) {
        metrics::counter!("persister.batches_written").increment(count);
    }

    /// Record duplicate events skipped.
    pub fn duplicates_skipped(count: u64) {
        metrics::counter!("persister.duplicates_skipped").increment(count);
    }

    /// Record write errors.
    pub fn write_errors(count: u64) {
        metrics::counter!("persister.write_errors").increment(count);
    }

    /// Record write latency.
    pub fn write_latency(duration_ms: f64) {
        metrics::histogram!("persister.write_latency_ms").record(duration_ms);
    }

    /// Record batch size.
    pub fn batch_size(size: usize) {
        metrics::histogram!("persister.batch_size").record(size as f64);
    }

    /// Record events invalidated during rollback.
    pub fn events_invalidated(count: u64) {
        metrics::counter!("persister.events_invalidated").increment(count);
    }

    /// Record database connection pool stats.
    pub fn pool_stats(active: u32, idle: u32, max: u32) {
        metrics::gauge!("persister.pool_active").set(active as f64);
        metrics::gauge!("persister.pool_idle").set(idle as f64);
        metrics::gauge!("persister.pool_max").set(max as f64);
    }
}

/// Metrics for the DLQ component.
pub struct DlqMetrics;

impl DlqMetrics {
    /// Record entry enqueued.
    pub fn enqueued(category: &str) {
        metrics::counter!("dlq.enqueued", "category" => category.to_string()).increment(1);
    }

    /// Record entry resolved.
    pub fn resolved() {
        metrics::counter!("dlq.resolved").increment(1);
    }

    /// Record retry attempted.
    pub fn retry_attempted() {
        metrics::counter!("dlq.retries").increment(1);
    }

    /// Record retries exhausted.
    pub fn retries_exhausted() {
        metrics::counter!("dlq.retries_exhausted").increment(1);
    }

    /// Record current unresolved count.
    pub fn unresolved_count(count: u64) {
        metrics::gauge!("dlq.unresolved_count").set(count as f64);
    }

    /// Record oldest unresolved age in hours.
    pub fn oldest_unresolved_age(hours: f64) {
        metrics::gauge!("dlq.oldest_unresolved_hours").set(hours);
    }
}

/// Metrics for the backfill component.
pub struct BackfillMetrics;

impl BackfillMetrics {
    /// Record slots processed.
    pub fn slots_processed(count: u64) {
        metrics::counter!("backfill.slots_processed").increment(count);
    }

    /// Record events fetched.
    pub fn events_fetched(count: u64) {
        metrics::counter!("backfill.events_fetched").increment(count);
    }

    /// Record events merged.
    pub fn events_merged(count: u64) {
        metrics::counter!("backfill.events_merged").increment(count);
    }

    /// Record duplicates skipped during merge.
    pub fn duplicates_skipped(count: u64) {
        metrics::counter!("backfill.duplicates_skipped").increment(count);
    }

    /// Record gaps filled.
    pub fn gaps_filled(count: u64) {
        metrics::counter!("backfill.gaps_filled").increment(count);
    }

    /// Record pending gaps.
    pub fn pending_gaps(count: u64) {
        metrics::gauge!("backfill.pending_gaps").set(count as f64);
    }

    /// Record pending slots.
    pub fn pending_slots(count: u64) {
        metrics::gauge!("backfill.pending_slots").set(count as f64);
    }

    /// Record RPC request latency.
    pub fn rpc_latency(duration_ms: f64) {
        metrics::histogram!("backfill.rpc_latency_ms").record(duration_ms);
    }

    /// Record RPC errors.
    pub fn rpc_errors(count: u64) {
        metrics::counter!("backfill.rpc_errors").increment(count);
    }

    /// Record backfill progress.
    pub fn progress(current_slot: u64, target_slot: u64) {
        metrics::gauge!("backfill.current_slot").set(current_slot as f64);
        metrics::gauge!("backfill.target_slot").set(target_slot as f64);
        if target_slot > 0 {
            let progress = current_slot as f64 / target_slot as f64 * 100.0;
            metrics::gauge!("backfill.progress_percent").set(progress);
        }
    }
}

/// Metrics for chain tracking.
pub struct ChainMetrics;

impl ChainMetrics {
    /// Record highest slot seen.
    pub fn highest_slot(slot: u64) {
        metrics::gauge!("chain.highest_slot").set(slot as f64);
    }

    /// Record highest rooted slot.
    pub fn highest_rooted(slot: u64) {
        metrics::gauge!("chain.highest_rooted").set(slot as f64);
    }

    /// Record slots tracked in memory.
    pub fn slots_tracked(count: usize) {
        metrics::gauge!("chain.slots_tracked").set(count as f64);
    }

    /// Record fork detected.
    pub fn fork_detected(depth: usize) {
        metrics::counter!("chain.forks_detected").increment(1);
        metrics::histogram!("chain.fork_depth").record(depth as f64);
    }
}

/// Initialize the metrics exporter.
///
/// Call this once at startup to set up the Prometheus metrics endpoint.
pub fn init_metrics(port: u16) -> Result<(), MetricsError> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .map_err(|e| MetricsError::InitFailed(e.to_string()))?;

    tracing::info!(port = port, "Metrics server started");
    Ok(())
}

/// Metrics initialization error.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("Failed to initialize metrics: {0}")]
    InitFailed(String),
}

/// A simple counter that can be atomically updated.
pub struct AtomicCounter {
    value: AtomicU64,
}

impl AtomicCounter {
    /// Create a new counter.
    pub const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    /// Increment the counter.
    pub fn increment(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value.
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset the counter.
    pub fn reset(&self) -> u64 {
        self.value.swap(0, Ordering::Relaxed)
    }
}

impl Default for AtomicCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_counter() {
        let counter = AtomicCounter::new();
        assert_eq!(counter.get(), 0);

        counter.increment(5);
        assert_eq!(counter.get(), 5);

        counter.increment(3);
        assert_eq!(counter.get(), 8);

        let old = counter.reset();
        assert_eq!(old, 8);
        assert_eq!(counter.get(), 0);
    }
}
