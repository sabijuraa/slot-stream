//! Database connection pool configuration.

use slot_stream_common::{Error, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::time::Duration;

/// Configuration for the database connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Database connection URL.
    pub database_url: String,

    /// Maximum number of connections in the pool.
    pub max_connections: u32,

    /// Minimum number of connections to maintain.
    pub min_connections: u32,

    /// Connection acquisition timeout.
    pub acquire_timeout: Duration,

    /// Idle connection timeout.
    pub idle_timeout: Duration,

    /// Maximum connection lifetime.
    pub max_lifetime: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/slot_stream".into(),
            max_connections: 20,
            min_connections: 5,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(1800),
        }
    }
}

impl PoolConfig {
    /// Create a config optimized for high write throughput.
    pub fn high_throughput() -> Self {
        Self {
            max_connections: 50,
            min_connections: 20,
            acquire_timeout: Duration::from_secs(10),
            ..Default::default()
        }
    }

    /// Create a pool from this configuration.
    pub async fn create_pool(&self) -> Result<PgPool> {
        let options: PgConnectOptions = self
            .database_url
            .parse()
            .map_err(|e: sqlx::Error| Error::Configuration(e.to_string()))?;

        let pool = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .acquire_timeout(self.acquire_timeout)
            .idle_timeout(self.idle_timeout)
            .max_lifetime(self.max_lifetime)
            .connect_with(options)
            .await
            .map_err(|e| Error::DatabaseConnection(e.to_string()))?;

        Ok(pool)
    }
}
