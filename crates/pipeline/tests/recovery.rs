//! Crash recovery.
//!
//! A crash is simulated by tearing the runtime down underneath a running
//! pipeline, which aborts the processor and persister wherever they happen to be
//! — mid-batch, mid-transaction, with events still queued. Nothing is flushed and
//! no shutdown runs, which is the point.
//!
//! The pipeline is then restarted against the same database and fed the same
//! stream from its committed resume point. The requirement is that the end state
//! is indistinguishable from a run that never crashed: no gaps, no duplicates.

mod support;

use anyhow::Result;
use slot_stream_ingester::{ChainScript, ScriptedSource};
use slot_stream_pipeline::Pipeline;
use std::time::Duration;
use support::*;

/// Run a pipeline on its own runtime and drop the runtime mid-stream.
///
/// Returns nothing: whatever was committed before the drop is whatever it is.
/// That uncertainty is exactly what recovery has to cope with.
fn crash_midway(url: String, script: ChainScript, run_for: Duration) {
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let config = test_config(&url);
            let (pipeline, _done) = Pipeline::build(&config).await.expect("build");
            // Paced so the crash lands in the middle of the stream rather than
            // after it has all been consumed.
            let source = ScriptedSource::new(script).paced(Duration::from_micros(200));
            let _ = tokio::time::timeout(run_for, pipeline.ingest(source)).await;
            // Deliberately no shutdown: leak the pipeline so nothing flushes.
            std::mem::forget(pipeline);
        });

        // Dropping the runtime aborts the tasks where they stand.
        rt.shutdown_timeout(Duration::from_millis(1));
    });

    handle.join().expect("crash thread");
}

#[tokio::test]
async fn restart_after_a_crash_leaves_no_gaps_and_no_duplicates() -> Result<()> {
    let db = TestDb::create().await?;
    let script = ChainScript::new().extend_from(500, 60, 5);
    let expected = script.canonical_events();

    // First run, killed part-way through.
    crash_midway(db.url.clone(), script.clone(), Duration::from_millis(300));

    let pool = db.pool().await?;
    let after_crash = persisted_events(&pool).await?;
    let cursor_before = cursor_position(&pool).await?;
    println!("--- after crash ---");
    println!("events committed before the crash: {}", after_crash.len());
    println!("cursor: {cursor_before:?}");

    assert!(
        after_crash.len() < expected.len(),
        "the crash should have interrupted the run; got {} of {} events",
        after_crash.len(),
        expected.len()
    );

    // Everything committed must be a genuine prefix of the canonical order.
    assert_eq!(
        after_crash.as_slice(),
        &expected[..after_crash.len()],
        "committed data must be a prefix of the canonical stream, not a jumble"
    );

    // Restart. The source replays from the beginning, as a real source would on
    // reconnect; the overlap must be absorbed rather than duplicated.
    run_script(&db.url, script).await?;

    let final_events = persisted_events(&pool).await?;
    let (valid, invalid) = row_counts(&pool).await?;
    let cursor_after = cursor_position(&pool).await?;

    println!("--- after restart ---");
    println!("events: {valid} valid, {invalid} invalidated");
    println!("cursor: {cursor_after:?}");

    assert_no_duplicate_rows(&pool).await?;
    assert_eq!(
        final_events, expected,
        "after recovery the state must equal an uninterrupted run"
    );
    assert_eq!(valid as usize, expected.len());
    assert_eq!(invalid, 0, "nothing was orphaned; this run had no fork");

    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn a_crash_during_a_reorg_still_recovers_to_the_canonical_chain() -> Result<()> {
    // The harder case: the interruption can land between the rollback and the
    // events of the branch that replaces it.
    let db = TestDb::create().await?;
    let script = ChainScript::new()
        .extend_from(500, 40, 4)
        .fork_run(540, 520, 12, 4);
    let expected = script.canonical_events();

    crash_midway(db.url.clone(), script.clone(), Duration::from_millis(400));

    let pool = db.pool().await?;
    let mid = persisted_events(&pool).await?;
    println!("--- after crash during a reorg run ---");
    println!("events committed: {}", mid.len());
    println!("reorgs recorded: {:?}", recorded_reorgs(&pool).await?);

    run_script(&db.url, script).await?;

    let final_events = persisted_events(&pool).await?;
    let (valid, invalid) = row_counts(&pool).await?;
    println!("--- after restart ---");
    println!("events: {valid} valid, {invalid} invalidated");
    println!("reorgs recorded: {:?}", recorded_reorgs(&pool).await?);

    assert_no_duplicate_rows(&pool).await?;
    assert_eq!(
        final_events, expected,
        "recovery across a reorg must still equal the canonical replay"
    );

    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn replaying_a_completed_run_changes_nothing() -> Result<()> {
    // Idempotency in its plainest form: feeding the identical stream twice must
    // leave the database exactly as it was after the first pass.
    let db = TestDb::create().await?;
    let script = ChainScript::new()
        .extend_from(700, 20, 3)
        .fork_run(720, 712, 6, 3);

    run_script(&db.url, script.clone()).await?;
    let pool = db.pool().await?;
    let first = persisted_events(&pool).await?;
    let (valid_first, invalid_first) = row_counts(&pool).await?;

    run_script(&db.url, script.clone()).await?;
    let second = persisted_events(&pool).await?;
    let (valid_second, invalid_second) = row_counts(&pool).await?;

    println!("--- replay idempotency ---");
    println!("first pass:  {valid_first} valid, {invalid_first} invalidated");
    println!("second pass: {valid_second} valid, {invalid_second} invalidated");

    assert_no_duplicate_rows(&pool).await?;
    assert_eq!(first, second, "a replay must not change visible state");
    assert_eq!(valid_first, valid_second, "a replay must not add rows");
    assert_eq!(first, script.canonical_events());

    pool.close().await;
    db.cleanup().await;
    Ok(())
}
