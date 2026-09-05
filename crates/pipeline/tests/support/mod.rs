//! Shared support for the integration tests.
//!
//! Every test runs against a real PostgreSQL database, in its own freshly
//! created schema, and drives the same composition root the binary uses.

#![allow(dead_code)]

pub mod sources;

use anyhow::{Context, Result};
use slot_stream_common::PipelineConfig;
use slot_stream_ingester::{ChainScript, ScriptedSource};
use slot_stream_pipeline::Pipeline;
use sqlx::{PgPool, Row};
use std::time::Duration;

/// Base connection string. Points at a database the tests may create others from.
pub fn base_url() -> String {
    std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://slotstream:slotstream@127.0.0.1:5432/slot_stream_test".to_string()
    })
}

/// A throwaway database, dropped when the guard falls out of scope.
pub struct TestDb {
    pub name: String,
    pub url: String,
    admin_url: String,
}

impl TestDb {
    /// Create a uniquely named database.
    pub async fn create() -> Result<Self> {
        let admin_url = base_url();
        let name = format!("ss_test_{}", uuid_like(),);

        let admin = PgPool::connect(&admin_url)
            .await
            .with_context(|| format!("connecting to {admin_url} to create a test database"))?;

        sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
            .execute(&admin)
            .await
            .context("creating the test database")?;
        admin.close().await;

        let url = swap_database(&admin_url, &name);
        Ok(Self {
            name,
            url,
            admin_url,
        })
    }

    /// A pool against this database.
    pub async fn pool(&self) -> Result<PgPool> {
        PgPool::connect(&self.url).await.context("connecting")
    }

    /// Drop the database.
    pub async fn cleanup(&self) {
        if let Ok(admin) = PgPool::connect(&self.admin_url).await {
            let _ = sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.name
            ))
            .execute(&admin)
            .await;
            admin.close().await;
        }
    }
}

fn swap_database(url: &str, name: &str) -> String {
    match url.rfind('/') {
        Some(idx) => format!("{}/{}", &url[..idx], name),
        None => format!("{url}/{name}"),
    }
}

/// A short unique-enough suffix without pulling in a uuid dependency here.
fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let counter = std::sync::atomic::AtomicU64::new(0);
    let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{nanos:x}_{n}_{:x}", std::process::id())
}

/// A pipeline configuration pointed at `url`.
pub fn test_config(url: &str) -> PipelineConfig {
    let mut config = PipelineConfig::default();
    config.database.url = url.to_string();
    config.database.max_connections = 8;
    config.database.min_connections = 1;
    // Small batches and a short timeout so tests observe commits promptly.
    config.persister.batch_size = 64;
    config.persister.batch_timeout_ms = 25;
    config.ingester.channel_capacity = 512;
    config.ingester.overflow_policy = "block".into();
    config.processor.chain_tracker_max_slots = 8_192;
    config.api.enabled = false;
    config.observability.metrics_enabled = false;
    config
}

/// Run a script end to end through the production pipeline.
///
/// This is the point of the whole harness: the events go through the ingester,
/// the processor's fork detection, and the persister's writes, exactly as they
/// would from a real source.
pub async fn run_script(url: &str, script: ChainScript) -> Result<()> {
    let config = test_config(url);
    let (pipeline, _done) = Pipeline::build(&config).await?;
    pipeline.ingest(ScriptedSource::new(script)).await?;
    pipeline.shutdown().await?;
    Ok(())
}

/// Run a script, pacing emission so backpressure has a chance to engage.
pub async fn run_script_paced(url: &str, script: ChainScript, pace: Duration) -> Result<()> {
    let config = test_config(url);
    let (pipeline, _done) = Pipeline::build(&config).await?;
    pipeline
        .ingest(ScriptedSource::new(script).paced(pace))
        .await?;
    pipeline.shutdown().await?;
    Ok(())
}

/// The valid events currently in the database, as `(slot, label)` in order.
///
/// This is what a reader sees, and what must equal a from-scratch replay of the
/// canonical chain.
pub async fn persisted_events(pool: &PgPool) -> Result<Vec<(u64, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT slot, data -> 'body' ->> 'label' AS label
        FROM events
        WHERE is_valid = true
        ORDER BY slot ASC, seq ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.get::<i64, _>("slot") as u64,
                r.get::<Option<String>, _>("label").unwrap_or_default(),
            )
        })
        .collect())
}

/// Distinct slots holding valid events, ascending.
pub async fn persisted_slots(pool: &PgPool) -> Result<Vec<u64>> {
    let rows =
        sqlx::query("SELECT DISTINCT slot FROM events WHERE is_valid = true ORDER BY slot ASC")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<i64, _>("slot") as u64)
        .collect())
}

/// Counts of valid and invalidated rows.
pub async fn row_counts(pool: &PgPool) -> Result<(i64, i64)> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*) FILTER (WHERE is_valid = true)  AS valid,
               COUNT(*) FILTER (WHERE is_valid = false) AS invalid
        FROM events
        "#,
    )
    .fetch_one(pool)
    .await?;
    Ok((row.get::<i64, _>("valid"), row.get::<i64, _>("invalid")))
}

/// Recorded reorgs, as `(fork_slot, divergence_point, depth, events_invalidated)`.
pub async fn recorded_reorgs(pool: &PgPool) -> Result<Vec<(u64, u64, i32, i64)>> {
    let rows = sqlx::query(
        r#"
        SELECT fork_slot, divergence_point, rollback_depth, events_invalidated
        FROM reorgs ORDER BY detected_at ASC, fork_slot ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.get::<i64, _>("fork_slot") as u64,
                r.get::<i64, _>("divergence_point") as u64,
                r.get::<i32, _>("rollback_depth"),
                r.get::<i64, _>("events_invalidated"),
            )
        })
        .collect())
}

/// Assert that no `(slot, source_seq)` pair appears twice.
pub async fn assert_no_duplicate_rows(pool: &PgPool) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT slot, source_seq, COUNT(*) AS n
        FROM events
        GROUP BY slot, source_seq
        HAVING COUNT(*) > 1
        "#,
    )
    .fetch_all(pool)
    .await?;

    assert!(
        rows.is_empty(),
        "found {} duplicated (slot, source_seq) rows",
        rows.len()
    );
    Ok(())
}

/// The committed cursor, as (slot, seq, source_seq).
pub async fn cursor_position(pool: &PgPool) -> Result<Option<(u64, u64, u64)>> {
    let row = sqlx::query("SELECT slot, seq, source_seq FROM cursors WHERE name = 'live'")
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| {
        (
            r.get::<i64, _>("slot") as u64,
            r.get::<i64, _>("seq") as u64,
            r.get::<i64, _>("source_seq") as u64,
        )
    }))
}

/// Print the two sides of a comparison, for evidence in the test log.
pub fn report(label: &str, expected: &[(u64, String)], actual: &[(u64, String)]) {
    println!("--- {label} ---");
    println!("expected (canonical replay): {} events", expected.len());
    println!("actual   (persisted state) : {} events", actual.len());
    let expected_slots: Vec<u64> = dedup_slots(expected);
    let actual_slots: Vec<u64> = dedup_slots(actual);
    println!("expected slots: {expected_slots:?}");
    println!("actual   slots: {actual_slots:?}");
}

fn dedup_slots(events: &[(u64, String)]) -> Vec<u64> {
    let mut slots: Vec<u64> = events.iter().map(|(s, _)| *s).collect();
    slots.dedup();
    slots
}
