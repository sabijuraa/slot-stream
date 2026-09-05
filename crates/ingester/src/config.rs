//! Configuration for the ingester.

use crate::buffer::OverflowPolicy;
use std::time::Duration;

/// Configuration for the ingester.
#[derive(Debug, Clone)]
pub struct IngesterConfig {
    /// Capacity of the internal event buffer.
    pub buffer_capacity: usize,

    /// Channel capacity for downstream consumers.
    pub channel_capacity: usize,

    /// What to do when buffer is full.
    pub overflow_policy: OverflowPolicy,

    /// Connection timeout for gRPC.
    pub connect_timeout: Duration,

    /// Request timeout for gRPC.
    pub request_timeout: Duration,

    /// Initial backoff for reconnection.
    pub initial_backoff: Duration,

    /// Maximum backoff for reconnection.
    pub max_backoff: Duration,

    /// Maximum reconnection attempts before giving up.
    pub max_reconnect_attempts: u32,

    /// Whether to enable TLS.
    pub tls_enabled: bool,

    /// Path to TLS certificate (if TLS enabled).
    pub tls_cert_path: Option<String>,

    /// Batch size for parsing.
    pub parse_batch_size: usize,
}

impl Default for IngesterConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 100_000,
            channel_capacity: 10_000,
            overflow_policy: OverflowPolicy::Block,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            max_reconnect_attempts: 10,
            tls_enabled: false,
            tls_cert_path: None,
            parse_batch_size: 100,
        }
    }
}

impl IngesterConfig {
    /// Create a config optimized for high throughput.
    pub fn high_throughput() -> Self {
        Self {
            buffer_capacity: 500_000,
            channel_capacity: 50_000,
            overflow_policy: OverflowPolicy::DropOldest,
            parse_batch_size: 500,
            ..Default::default()
        }
    }

    /// Create a config optimized for reliability (no drops).
    pub fn reliable() -> Self {
        Self {
            buffer_capacity: 1_000_000,
            channel_capacity: 100_000,
            overflow_policy: OverflowPolicy::Block,
            max_reconnect_attempts: u32::MAX,
            ..Default::default()
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_capacity == 0 {
            return Err("buffer_capacity must be > 0".into());
        }
        if self.channel_capacity == 0 {
            return Err("channel_capacity must be > 0".into());
        }
        if self.tls_enabled && self.tls_cert_path.is_none() {
            return Err("tls_cert_path required when tls_enabled".into());
        }
        Ok(())
    }
}
