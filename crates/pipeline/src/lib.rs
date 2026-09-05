//! # slot-stream-pipeline
//!
//! Assembling the pipeline.
//!
//! This is the composition root: the one place where the ingester, processor,
//! and persister become a running system rather than a set of crates that each
//! compile. It lives in a library rather than inside the binary specifically so
//! the integration tests can drive the same wiring the binary runs. A test that
//! assembled its own pipeline would be testing an arrangement nothing ships.

use anyhow::{Context, Result};
use slot_stream_common::PipelineConfig;
use slot_stream_dlq::{DeadLetterQueue, DlqConfig, PostgresStorage};
use slot_stream_ingester::{EventSource, Ingester, IngesterConfig, OverflowPolicy};
use slot_stream_persister::{
    migrate, PersistCommand, Persister, PersisterConfig, PoolConfig, ResumeState,
};
use slot_stream_processor::{MetricsHandler, Processor, ProcessorConfig};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// A wired, running pipeline.
pub struct Pipeline {
    pub pool: PgPool,
    pub ingester: Arc<Ingester>,
    pub processor: Arc<Processor>,
    pub persister: Arc<Persister>,
    pub resume: ResumeState,
    processor_task: JoinHandle<()>,
    persister_task: JoinHandle<()>,
}

impl Pipeline {
    /// Connect, migrate, and start the processor and persister.
    ///
    /// The ingester is returned unstarted so the caller chooses the source.
    pub async fn build(config: &PipelineConfig) -> Result<(Self, mpsc::Receiver<()>)> {
        let pool = connect(config).await?;
        migrate(&pool).await.context("running migrations")?;

        let persister_config = PersisterConfig {
            batch_size: config.persister.batch_size,
            batch_timeout: Duration::from_millis(config.persister.batch_timeout_ms),
            ..Default::default()
        };
        let persister = Arc::new(Persister::new(pool.clone(), persister_config));

        // Everything about resuming safely comes from here: where the stream left
        // off, what sequence to carry on from, and the chain shape so a reorg
        // spanning the restart is still detected.
        let resume = persister
            .resume_state()
            .await
            .context("reading resume state")?;

        let dlq = Arc::new(DeadLetterQueue::new(
            Arc::new(PostgresStorage::new(pool.clone())),
            DlqConfig {
                max_retries: config.dlq.max_retries,
                ..Default::default()
            },
        ));

        // Both channels are bounded, so memory is capped end to end. A slow
        // persister fills the persist channel, which stalls the processor, which
        // fills the raw channel, which stalls the ingester, which stops reading
        // the source. That chain is the backpressure design.
        let (persist_tx, persist_rx) = mpsc::channel::<PersistCommand>(config.persister.batch_size * 4);
        let (raw_tx, raw_rx) = mpsc::channel(config.ingester.channel_capacity);

        let mut processor = Processor::resuming(
            dlq,
            persist_tx,
            ProcessorConfig {
                chain_window: config.processor.chain_tracker_max_slots,
                ..Default::default()
            },
            resume.max_seq,
            &resume.canonical_slots,
        );
        processor.register_handler(MetricsHandler);
        let processor = Arc::new(processor);

        let ingester_config = IngesterConfig {
            channel_capacity: config.ingester.channel_capacity,
            overflow_policy: parse_overflow_policy(&config.ingester.overflow_policy),
            initial_backoff: Duration::from_millis(config.grpc.initial_backoff_ms),
            max_backoff: Duration::from_millis(config.grpc.max_backoff_ms),
            max_reconnect_attempts: config.grpc.max_reconnect_attempts,
            ..Default::default()
        };
        // The ingester is constructed around a channel we already hold, so its
        // own is replaced by the one the processor reads.
        let ingester = Arc::new(Ingester::with_sender(
            ingester_config,
            raw_tx,
            resume.resume_source_seq(),
        ));

        let (done_tx, done_rx) = mpsc::channel(2);

        let persister_task = {
            let persister = Arc::clone(&persister);
            let done = done_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = persister.run(persist_rx).await {
                    warn!(error = %e, "persister exited with an error");
                }
                let _ = done.send(()).await;
            })
        };

        let processor_task = {
            let processor = Arc::clone(&processor);
            tokio::spawn(async move {
                if let Err(e) = processor.run(raw_rx).await {
                    warn!(error = %e, "processor exited with an error");
                }
                let _ = done_tx.send(()).await;
            })
        };

        info!(
            resume_source_seq = resume.resume_source_seq().0,
            max_seq = resume.max_seq.0,
            "pipeline built"
        );

        Ok((
            Self {
                pool,
                ingester,
                processor,
                persister,
                resume,
                processor_task,
                persister_task,
            },
            done_rx,
        ))
    }

    /// Run a source to completion through this pipeline.
    pub async fn ingest<S: EventSource>(&self, source: S) -> Result<()> {
        self.ingester.run(source).await.context("ingesting")
    }

    /// Stop cleanly, draining what is in flight.
    ///
    /// The order is the pipeline order, and each step is what unblocks the next:
    ///
    /// 1. Ask the ingester to stop, then drop it. Dropping it is the part that
    ///    matters — it owns the only sender for the raw channel, and the
    ///    processor's loop runs until that channel closes.
    /// 2. Wait for the processor. When it returns, its task releases the last
    ///    reference to the `Processor`, which owns the only sender for the
    ///    persist channel.
    /// 3. Wait for the persister, which flushes its final batch and commits the
    ///    cursor before returning.
    ///
    /// Skipping a drop here does not fail loudly; it hangs. So the drops are
    /// explicit and named rather than left to a `..` pattern.
    pub async fn shutdown(self) -> Result<()> {
        info!("shutting down pipeline");

        let Pipeline {
            pool,
            ingester,
            processor,
            persister,
            resume: _,
            processor_task,
            persister_task,
        } = self;

        ingester.shutdown();
        drop(ingester);

        // Our own handle first; the task still holds one until it returns.
        drop(processor);
        let _ = processor_task.await;
        let _ = persister_task.await;

        let stats = persister.stats();
        drop(persister);
        pool.close().await;

        info!(?stats, "pipeline stopped");
        Ok(())
    }
}

/// Build a connection pool from config.
pub async fn connect(config: &PipelineConfig) -> Result<PgPool> {
    let pool_config = PoolConfig {
        database_url: config.database.url.clone(),
        max_connections: config.database.max_connections,
        min_connections: config.database.min_connections,
        acquire_timeout: Duration::from_secs(config.database.acquire_timeout_secs),
        idle_timeout: Duration::from_secs(config.database.idle_timeout_secs),
        max_lifetime: Duration::from_secs(config.database.max_lifetime_secs),
    };

    pool_config
        .create_pool()
        .await
        .with_context(|| format!("connecting to {}", redact(&config.database.url)))
}

/// Hide credentials before a connection string reaches a log or an error.
pub fn redact(url: &str) -> String {
    match (url.find("://"), url.find('@')) {
        (Some(scheme_end), Some(at)) if at > scheme_end + 3 => {
            format!("{}://***{}", &url[..scheme_end], &url[at..])
        }
        _ => url.to_string(),
    }
}

fn parse_overflow_policy(name: &str) -> OverflowPolicy {
    match name {
        "drop_oldest" => OverflowPolicy::DropOldest,
        "drop_newest" => OverflowPolicy::DropNewest,
        // Blocking is the right default for an indexer: a delay is recoverable,
        // a hole in the data is not. An unrecognised value gets it too.
        _ => OverflowPolicy::Block,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_never_reach_a_log_line() {
        assert_eq!(
            redact("postgres://user:hunter2@db:5432/slot_stream"),
            "postgres://***@db:5432/slot_stream"
        );
        assert_eq!(
            redact("postgres://localhost/slot_stream"),
            "postgres://localhost/slot_stream"
        );
    }

    #[test]
    fn an_unknown_overflow_policy_falls_back_to_blocking() {
        assert_eq!(parse_overflow_policy("block"), OverflowPolicy::Block);
        assert_eq!(parse_overflow_policy("nonsense"), OverflowPolicy::Block);
        assert_eq!(
            parse_overflow_policy("drop_newest"),
            OverflowPolicy::DropNewest
        );
    }
}
