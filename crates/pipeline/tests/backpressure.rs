//! Backpressure and bounded resources.
//!
//! The producer here is far faster than the consumer: the persister is
//! configured to commit one transaction per event through a single connection,
//! while the scripted source emits as fast as it can be polled. That gap is the
//! condition under which an unbounded pipeline grows until it dies.
//!
//! What must hold is that the queue depth stays inside its capacity, nothing is
//! dropped under the blocking policy, and every event still arrives.

mod support;

use anyhow::Result;
use slot_stream_ingester::ScriptedSource;
use slot_stream_pipeline::Pipeline;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::*;

/// Resident set size in kilobytes, for supporting evidence.
fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| {
            let pages: u64 = s.split_whitespace().nth(1)?.parse().ok()?;
            Some(pages * 4)
        })
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_persister_throttles_ingestion_without_dropping_or_growing() -> Result<()> {
    let db = TestDb::create().await?;

    let mut config = test_config(&db.url);
    // Make the consumer genuinely slow: one transaction per event, one
    // connection, so commits serialise against the disk.
    config.persister.batch_size = 1;
    config.persister.batch_timeout_ms = 1;
    config.database.max_connections = 2;
    config.database.min_connections = 1;
    // A small buffer, so the producer meets the limit almost immediately.
    config.ingester.channel_capacity = 32;
    config.ingester.overflow_policy = "block".into();

    let script = slot_stream_ingester::ChainScript::new().extend_from(1, 200, 5);
    let total = script.event_count();

    let (pipeline, _done) = Pipeline::build(&config).await?;

    // A second handle on purpose. Watching the queue while the pipeline runs is
    // what an operator's metrics endpoint does, and shutdown has to survive it.
    let ingester = Arc::clone(&pipeline.ingester);

    let capacity = ingester.buffer_capacity();
    let max_depth = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(AtomicUsize::new(0));

    let sampler = {
        let ingester = Arc::clone(&ingester);
        let max_depth = Arc::clone(&max_depth);
        let samples = Arc::clone(&samples);
        tokio::spawn(async move {
            loop {
                let depth = ingester.buffer_depth();
                max_depth.fetch_max(depth, Ordering::Relaxed);
                samples.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };

    let rss_before = rss_kb();
    let started = Instant::now();
    pipeline.ingest(ScriptedSource::new(script)).await?;
    let ingest_elapsed = started.elapsed();

    let shutdown_started = Instant::now();
    pipeline.shutdown().await?;
    let shutdown_elapsed = shutdown_started.elapsed();
    let rss_after = rss_kb();

    sampler.abort();

    let stats = ingester.stats();
    let observed_max = max_depth.load(Ordering::Relaxed);

    let pool = db.pool().await?;
    let (valid, _) = row_counts(&pool).await?;

    println!("--- backpressure under a slow persister ---");
    println!("events in script        : {total}");
    println!("events persisted        : {valid}");
    println!("events dropped          : {}", stats.events_dropped);
    println!("pushes that had to wait : {}", ingester.blocked_waits());
    println!("buffer capacity         : {capacity}");
    println!(
        "max observed depth      : {observed_max} (over {} samples)",
        samples.load(Ordering::Relaxed)
    );
    println!("ingest wall time        : {ingest_elapsed:?}");
    println!("shutdown wall time      : {shutdown_elapsed:?}");
    println!("RSS before / after      : {rss_before} kB / {rss_after} kB");

    assert_eq!(
        stats.events_dropped, 0,
        "the blocking policy must never drop an event"
    );
    assert_eq!(
        valid as usize, total,
        "every event must reach the database despite the slow consumer"
    );
    assert!(
        observed_max <= capacity,
        "queue depth {observed_max} exceeded its capacity {capacity}"
    );
    assert!(
        ingester.blocked_waits() > 0,
        "the producer should have been throttled; if it never waited, the \
         consumer was not actually slower and this proves nothing"
    );
    assert!(
        observed_max > 0,
        "the sampler never saw a queued event, so it measured nothing"
    );

    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_shedding_policy_drops_rather_than_growing() -> Result<()> {
    // The contrast case. Configured to shed, the pipeline must actually shed —
    // which is what demonstrates the bound is enforced by the channel rather
    // than by the producer happening to be slow.
    let db = TestDb::create().await?;

    let mut config = test_config(&db.url);
    config.persister.batch_size = 1;
    config.persister.batch_timeout_ms = 1;
    config.database.max_connections = 2;
    config.database.min_connections = 1;
    config.ingester.channel_capacity = 8;
    config.ingester.overflow_policy = "drop_newest".into();

    let script = slot_stream_ingester::ChainScript::new().extend_from(1, 200, 5);
    let total = script.event_count();

    let (pipeline, _done) = Pipeline::build(&config).await?;
    let ingester = Arc::clone(&pipeline.ingester);

    pipeline.ingest(ScriptedSource::new(script)).await?;
    pipeline.shutdown().await?;

    let stats = ingester.stats();
    let pool = db.pool().await?;
    let (valid, _) = row_counts(&pool).await?;

    println!("--- shedding policy ---");
    println!("events in script : {total}");
    println!("events dropped   : {}", stats.events_dropped);
    println!("events persisted : {valid}");

    assert!(
        stats.events_dropped > 0,
        "a shedding policy under overload must drop something"
    );
    assert_eq!(
        stats.events_emitted as i64, valid,
        "everything that was not dropped must have been persisted"
    );
    assert!(
        (valid as usize) < total,
        "shedding means fewer rows than events, by definition"
    );

    pool.close().await;
    db.cleanup().await;
    Ok(())
}
