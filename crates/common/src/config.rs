//! Configuration types for the slot-stream pipeline.
//!
//! All configuration can be loaded from TOML files or environment variables.

use serde::{Deserialize, Serialize};

/// Root configuration for the entire pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Ingester configuration.
    pub ingester: IngesterSettings,

    /// Processor configuration.
    pub processor: ProcessorSettings,

    /// Persister configuration.
    pub persister: PersisterSettings,

    /// Backfill configuration.
    pub backfill: BackfillSettings,

    /// DLQ configuration.
    pub dlq: DlqSettings,

    /// Database configuration.
    pub database: DatabaseSettings,

    /// gRPC configuration.
    pub grpc: GrpcSettings,

    /// RPC configuration.
    pub rpc: RpcSettings,

    /// Observability configuration.
    pub observability: ObservabilitySettings,

    /// Read API configuration.
    #[serde(default)]
    pub api: ApiSettings,
}

/// Settings for the read API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSettings {
    /// Whether to serve the read API.
    pub enabled: bool,

    /// Address to bind.
    pub bind_address: String,

    /// Port to listen on.
    pub port: u16,

    /// Maximum rows a single query may return.
    pub max_page_size: u32,

    /// Default rows returned when the caller does not specify a limit.
    pub default_page_size: u32,
}

impl Default for ApiSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_address: "0.0.0.0".into(),
            port: 8080,
            max_page_size: 1000,
            default_page_size: 100,
        }
    }
}

/// Ingester settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngesterSettings {
    /// Buffer capacity for events.
    pub buffer_capacity: usize,

    /// Channel capacity for downstream.
    pub channel_capacity: usize,

    /// Overflow policy: "drop_oldest", "drop_newest", "block".
    pub overflow_policy: String,

    /// Parse batch size.
    pub parse_batch_size: usize,

    /// Maximum payload size in bytes.
    pub max_payload_size: usize,
}

impl Default for IngesterSettings {
    fn default() -> Self {
        Self {
            buffer_capacity: 100_000,
            channel_capacity: 10_000,
            overflow_policy: "drop_oldest".into(),
            parse_batch_size: 100,
            max_payload_size: 10 * 1024 * 1024,
        }
    }
}

/// Processor settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorSettings {
    /// Number of worker tasks.
    pub worker_count: usize,

    /// Batch size for processing.
    pub batch_size: usize,

    /// Maximum batch wait time in milliseconds.
    pub batch_timeout_ms: u64,

    /// Enable parallel handler execution.
    pub parallel_handlers: bool,

    /// Maximum processing latency before alerting (ms).
    pub max_latency_ms: u64,

    /// Maximum slots to keep in chain tracker.
    pub chain_tracker_max_slots: usize,
}

impl Default for ProcessorSettings {
    fn default() -> Self {
        Self {
            worker_count: 4,
            batch_size: 100,
            batch_timeout_ms: 100,
            parallel_handlers: true,
            max_latency_ms: 5000,
            chain_tracker_max_slots: 10_000,
        }
    }
}

/// Persister settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersisterSettings {
    /// Batch size for writes.
    pub batch_size: usize,

    /// Maximum batch wait time in milliseconds.
    pub batch_timeout_ms: u64,

    /// Maximum write retries.
    pub max_retries: u32,

    /// Retry backoff in milliseconds.
    pub retry_backoff_ms: u64,

    /// Whether to enable batch writes.
    pub batch_writes: bool,
}

impl Default for PersisterSettings {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout_ms: 100,
            max_retries: 3,
            retry_backoff_ms: 100,
            batch_writes: true,
        }
    }
}

/// Backfill settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillSettings {
    /// Enabled backfill on startup.
    pub enabled: bool,

    /// Batch size for RPC requests.
    pub batch_size: usize,

    /// Delay between batches in milliseconds.
    pub batch_delay_ms: u64,

    /// Maximum concurrent RPC requests.
    pub max_concurrent: usize,

    /// Rate limit (requests per second).
    pub rate_limit_rps: u32,

    /// Maximum retries per request.
    pub max_retries: u32,
}

impl Default for BackfillSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 100,
            batch_delay_ms: 100,
            max_concurrent: 4,
            rate_limit_rps: 100,
            max_retries: 3,
        }
    }
}

/// DLQ settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqSettings {
    /// Maximum retry attempts.
    pub max_retries: u32,

    /// Enable automatic retry.
    pub auto_retry: bool,

    /// Alert threshold in hours.
    pub alert_threshold_hours: u32,

    /// Purge resolved entries after days.
    pub purge_after_days: u32,
}

impl Default for DlqSettings {
    fn default() -> Self {
        Self {
            max_retries: 3,
            auto_retry: false,
            alert_threshold_hours: 24,
            purge_after_days: 30,
        }
    }
}

/// Database settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSettings {
    /// Connection URL.
    pub url: String,

    /// Maximum connections.
    pub max_connections: u32,

    /// Minimum connections.
    pub min_connections: u32,

    /// Connection acquire timeout in seconds.
    pub acquire_timeout_secs: u64,

    /// Idle timeout in seconds.
    pub idle_timeout_secs: u64,

    /// Maximum connection lifetime in seconds.
    pub max_lifetime_secs: u64,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            url: "postgres://localhost/slot_stream".into(),
            max_connections: 20,
            min_connections: 5,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1800,
        }
    }
}

/// gRPC settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcSettings {
    /// Geyser endpoint URL.
    pub endpoint: String,

    /// Connection timeout in seconds.
    pub connect_timeout_secs: u64,

    /// Request timeout in seconds.
    pub request_timeout_secs: u64,

    /// Initial reconnect backoff in milliseconds.
    pub initial_backoff_ms: u64,

    /// Maximum reconnect backoff in milliseconds.
    pub max_backoff_ms: u64,

    /// Maximum reconnect attempts.
    pub max_reconnect_attempts: u32,

    /// Enable TLS.
    pub tls_enabled: bool,

    /// TLS certificate path.
    pub tls_cert_path: Option<String>,
}

impl Default for GrpcSettings {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:10000".into(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
            max_reconnect_attempts: 10,
            tls_enabled: false,
            tls_cert_path: None,
        }
    }
}

/// RPC settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcSettings {
    /// RPC endpoint URL.
    pub endpoint: String,

    /// Request timeout in seconds.
    pub timeout_secs: u64,

    /// Rate limit (requests per second).
    pub rate_limit_rps: u32,

    /// Maximum retries.
    pub max_retries: u32,
}

impl Default for RpcSettings {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8899".into(),
            timeout_secs: 30,
            rate_limit_rps: 100,
            max_retries: 3,
        }
    }
}

/// Observability settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilitySettings {
    /// Enable metrics.
    pub metrics_enabled: bool,

    /// Metrics port.
    pub metrics_port: u16,

    /// Log level.
    pub log_level: String,

    /// Log format: "json" or "pretty".
    pub log_format: String,

    /// Enable tracing.
    pub tracing_enabled: bool,
}

impl Default for ObservabilitySettings {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            metrics_port: 9090,
            log_level: "info".into(),
            log_format: "json".into(),
            tracing_enabled: true,
        }
    }
}

impl PipelineConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::IoError(e.to_string()))?;
        let config: Self = toml::from_str(&contents)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from environment variables.
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config = Self::default();

        // Override with environment variables
        if let Ok(url) = std::env::var("DATABASE_URL") {
            config.database.url = url;
        }
        if let Ok(endpoint) = std::env::var("GRPC_ENDPOINT") {
            config.grpc.endpoint = endpoint;
        }
        if let Ok(endpoint) = std::env::var("RPC_ENDPOINT") {
            config.rpc.endpoint = endpoint;
        }
        if let Ok(level) = std::env::var("LOG_LEVEL") {
            config.observability.log_level = level;
        }
        if let Ok(port) = std::env::var("METRICS_PORT") {
            config.observability.metrics_port = port
                .parse()
                .map_err(|_| ConfigError::ValidationError(format!("METRICS_PORT: {port}")))?;
        }
        if let Ok(port) = std::env::var("API_PORT") {
            config.api.port = port
                .parse()
                .map_err(|_| ConfigError::ValidationError(format!("API_PORT: {port}")))?;
        }

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.ingester.buffer_capacity == 0 {
            return Err(ConfigError::ValidationError(
                "buffer_capacity must be > 0".into(),
            ));
        }
        if self.processor.worker_count == 0 {
            return Err(ConfigError::ValidationError(
                "worker_count must be > 0".into(),
            ));
        }
        if self.database.max_connections == 0 {
            return Err(ConfigError::ValidationError(
                "max_connections must be > 0".into(),
            ));
        }
        if self.api.enabled && self.api.port == 0 {
            return Err(ConfigError::ValidationError(
                "api.port must be > 0 when the api is enabled".into(),
            ));
        }
        if self.api.default_page_size > self.api.max_page_size {
            return Err(ConfigError::ValidationError(
                "api.default_page_size must not exceed api.max_page_size".into(),
            ));
        }
        Ok(())
    }
}

/// Configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    IoError(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PipelineConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        let mut config = PipelineConfig::default();
        config.ingester.buffer_capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_roundtrips_through_toml() {
        let config = PipelineConfig::default();
        let text = toml::to_string(&config).expect("serialize");
        let parsed: PipelineConfig = toml::from_str(&text).expect("parse");
        assert_eq!(parsed.database.max_connections, config.database.max_connections);
        assert_eq!(parsed.ingester.buffer_capacity, config.ingester.buffer_capacity);
    }
}
