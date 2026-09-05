//! The two recovery paths that are not about forks: the dead-letter queue and
//! backfill.
//!
//! Both are proven the same way as everything else here — through the wired
//! pipeline against a real database, with the events entering by the same door
//! the live stream uses.

mod support;

use anyhow::{Context, Result};
use axum::{routing::post, Json, Router};
use slot_stream_backfill::{BackfillConfig, BackfillRange, Backfiller};
use slot_stream_dlq::{DeadLetterQueue, DlqConfig, PostgresStorage, ReplayConfig, ReplaySelector};
use slot_stream_pipeline::Pipeline;
use sqlx::{PgPool, Row};
use std::net::SocketAddr;
use std::sync::Arc;
use support::sources::*;
use support::*;
use tokio::sync::mpsc;

/// Rows in the dead-letter queue, as `(slot, category, resolved, retries)`.
async fn dlq_rows(pool: &PgPool) -> Result<Vec<(Option<i64>, String, bool, i32)>> {
    let rows = sqlx::query(
        "SELECT slot, error_category, is_resolved, retry_count
         FROM dead_letter_queue ORDER BY created_at ASC, slot ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.get::<Option<i64>, _>("slot"),
                r.get::<String, _>("error_category"),
                r.get::<bool, _>("is_resolved"),
                r.get::<i32, _>("retry_count"),
            )
        })
        .collect())
}

/// Replay the DLQ back through a live pipeline.
///
/// The sink is a channel feeding a real `Pipeline`, which is the point: a
/// replayed entry is parsed, fork-checked and written by the same code as a live
/// event, so it cannot succeed by taking a shortcut the stream does not have.
async fn replay_through_the_pipeline(
    dlq: &DeadLetterQueue,
    config: &slot_stream_common::PipelineConfig,
    selector: ReplaySelector,
) -> Result<slot_stream_dlq::ReplayResult> {
    let (pipeline, _done) = Pipeline::build(config).await?;
    let (tx, rx) = mpsc::channel(64);

    let replay = {
        let selector = selector.clone();
        async move {
            let result = dlq.replay(selector, &tx, &ReplayConfig::default()).await;
            drop(tx);
            result
        }
    };

    let (result, ingest) = tokio::join!(
        replay,
        pipeline.ingest(ChannelSource::new("dlq replay", rx))
    );
    ingest?;
    pipeline.shutdown().await?;
    Ok(result?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_events_are_quarantined_and_stay_quarantined_on_replay() -> Result<()> {
    let db = TestDb::create().await?;
    let config = test_config(&db.url);

    // Slots 1..=4, with the two events of slot 3 carrying payloads that are not
    // JSON. Nothing about them is a pipeline failure; they must be quarantined
    // rather than dropped, and the rest of the stream must keep flowing.
    let events = vec![
        event(1, 1, 0, "s1-a"),
        event(2, 1, 0, "s1-b"),
        event(3, 2, 1, "s2-a"),
        event(4, 2, 1, "s2-b"),
        malformed(5, 3, 2),
        malformed(6, 3, 2),
        event(7, 4, 3, "s4-a"),
        event(8, 4, 3, "s4-b"),
    ];

    let (pipeline, _done) = Pipeline::build(&config).await?;
    pipeline
        .ingest(RawSource::new("malformed stream", events))
        .await?;
    pipeline.shutdown().await?;

    let pool = db.pool().await?;
    let persisted = persisted_events(&pool).await?;
    let quarantined = dlq_rows(&pool).await?;

    println!("--- malformed events ---");
    println!("persisted    : {persisted:?}");
    println!("dead-lettered: {quarantined:?}");

    let expected_good = vec![
        (1, "s1-a".to_string()),
        (1, "s1-b".to_string()),
        (2, "s2-a".to_string()),
        (2, "s2-b".to_string()),
        (4, "s4-a".to_string()),
        (4, "s4-b".to_string()),
    ];
    assert_eq!(
        persisted, expected_good,
        "the good events must all land, and the bad ones must not"
    );
    assert_eq!(quarantined.len(), 2, "both bad events must be quarantined");
    assert!(
        quarantined
            .iter()
            .all(|(slot, _, resolved, _)| *slot == Some(3) && !resolved),
        "each entry keeps the slot it came from and is awaiting replay"
    );

    // Replaying them changes nothing about the data, which is the property that
    // matters: the payload is still not JSON, so it is rejected again and
    // re-quarantined rather than written or lost. Retrying is therefore safe to
    // automate.
    let dlq = Arc::new(DeadLetterQueue::new(
        Arc::new(PostgresStorage::new(pool.clone())),
        DlqConfig::default(),
    ));
    let result = replay_through_the_pipeline(&dlq, &config, ReplaySelector::AllUnresolved).await?;

    let after = persisted_events(&pool).await?;
    let entries = dlq_rows(&pool).await?;
    let pending = entries
        .iter()
        .filter(|(_, _, resolved, _)| !resolved)
        .count();

    println!(
        "replayed     : {} accepted, {} rejected outright",
        result.succeeded.len(),
        result.failed.len()
    );
    println!("persisted    : {after:?}");
    println!("dlq now      : {entries:?}");

    assert_eq!(
        after, expected_good,
        "a replay of an event that is still malformed must not write anything"
    );
    assert_eq!(
        result.succeeded.len(),
        2,
        "both entries must be handed back to the pipeline"
    );
    assert_eq!(
        pending, 2,
        "and both must be quarantined again, not lost: an unfixable event has to \
         stay visible to an operator"
    );

    assert_no_duplicate_rows(&pool).await?;
    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_that_failed_for_a_transient_reason_replays_successfully() -> Result<()> {
    // The case the DLQ exists for. The payload was always fine; the write failed
    // because the database was briefly unavailable. Once it is back, replaying
    // must index the event exactly as if it had never failed.
    let db = TestDb::create().await?;
    let config = test_config(&db.url);

    let (pipeline, _done) = Pipeline::build(&config).await?;
    pipeline
        .ingest(RawSource::new(
            "live",
            vec![event(1, 1, 0, "s1"), event(2, 2, 1, "s2")],
        ))
        .await?;
    pipeline.shutdown().await?;

    let pool = db.pool().await?;
    let dlq = Arc::new(DeadLetterQueue::new(
        Arc::new(PostgresStorage::new(pool.clone())),
        DlqConfig::default(),
    ));

    // Slot 3's event never reached the database: the write failed. It is
    // well-formed, so it is a candidate for automatic retry.
    let stranded = event(3, 3, 2, "s3");
    dlq.enqueue(
        stranded,
        &slot_stream_common::Error::DatabaseQuery("connection reset by peer".into()),
    )
    .await?;

    let before = persisted_events(&pool).await?;
    let quarantined = dlq_rows(&pool).await?;
    println!("--- transient failure ---");
    println!("persisted before replay: {before:?}");
    println!("dead-lettered          : {quarantined:?}");
    assert_eq!(before.len(), 2, "slot 3 is missing");
    assert_eq!(quarantined.len(), 1);
    assert_eq!(
        quarantined[0].1, "persistence",
        "the failure must be categorised as what it was"
    );
    assert!(!quarantined[0].2, "and it starts unresolved");

    let result = replay_through_the_pipeline(&dlq, &config, ReplaySelector::AllUnresolved).await?;

    let after = persisted_events(&pool).await?;
    let entries = dlq_rows(&pool).await?;
    println!("persisted after replay : {after:?}");
    println!("dlq now                : {entries:?}");

    assert_eq!(
        after,
        vec![
            (1, "s1".to_string()),
            (2, "s2".to_string()),
            (3, "s3".to_string()),
        ],
        "the stranded event must be indexed in its proper place"
    );
    assert_eq!(result.succeeded.len(), 1);
    assert!(
        entries.iter().all(|(_, _, resolved, _)| *resolved),
        "a successfully replayed entry must not stay pending, or it replays forever"
    );

    assert_no_duplicate_rows(&pool).await?;
    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backfilled_gap_merges_without_disturbing_the_live_chain() -> Result<()> {
    let db = TestDb::create().await?;
    let config = test_config(&db.url);

    // The live stream misses slots 6, 7 and 8 entirely — the connection dropped
    // and came back further along. Slot 9 arrives claiming parent 8, which we
    // never saw.
    let live = vec![
        event(1, 1, 0, "s1"),
        event(2, 2, 1, "s2"),
        event(3, 3, 2, "s3"),
        event(4, 4, 3, "s4"),
        event(5, 5, 4, "s5"),
        event(9, 9, 8, "s9"),
        event(10, 10, 9, "s10"),
    ];

    let (pipeline, _done) = Pipeline::build(&config).await?;
    pipeline
        .ingest(RawSource::new("live with a gap", live))
        .await?;
    pipeline.shutdown().await?;

    let pool = db.pool().await?;
    let before = persisted_slots(&pool).await?;
    println!("--- backfill ---");
    println!("live slots before backfill: {before:?}");
    assert_eq!(before, vec![1, 2, 3, 4, 5, 9, 10], "the gap is real");

    // A real Solana JSON-RPC endpoint, served over a real socket. The backfiller
    // is the shipped one, with its own reqwest client and retry logic; only the
    // chain the endpoint reports is chosen rather than observed.
    let rpc = FakeRpc::start(6..=8).await?;

    let backfiller = Backfiller::new(
        BackfillConfig {
            batch_size: 100,
            batch_delay: std::time::Duration::ZERO,
            max_retries: 1,
            skip_present_slots: true,
        },
        &rpc.url,
        pool.clone(),
    )?;
    // The whole span, not just the hole: the backfiller has to work out for
    // itself which slots it already has.
    backfiller.add_range(BackfillRange::gap_fill(1, 10));

    let (pipeline, _done) = Pipeline::build(&config).await?;
    let (tx, rx) = mpsc::channel(64);
    let fill = tokio::spawn(async move { backfiller.run(tx).await.map(|()| backfiller) });
    pipeline.ingest(ChannelSource::new("backfill", rx)).await?;
    pipeline.shutdown().await?;
    let backfiller = fill.await??;

    let after = persisted_slots(&pool).await?;
    let events = persisted_events(&pool).await?;
    let (valid, invalid) = row_counts(&pool).await?;
    let reorgs = recorded_reorgs(&pool).await?;
    let stats = backfiller.stats();

    println!("slots after backfill      : {after:?}");
    println!("rows                      : {valid} valid, {invalid} invalidated");
    println!("backfill stats            : {stats:?}");
    println!("reorgs recorded           : {reorgs:?}");

    assert_eq!(
        after,
        (1..=10).collect::<Vec<u64>>(),
        "the gap must be filled and nothing else touched"
    );
    assert_eq!(
        invalid, 0,
        "backfilling history must not orphan the live chain above it"
    );
    assert!(
        reorgs.is_empty(),
        "a backfilled slot is not a fork; recording one means it was misread as \
         a branch off an old parent, got {reorgs:?}"
    );
    assert_eq!(
        stats.slots_skipped_present, 0,
        "the RPC only offers the three missing slots, so there is nothing to skip"
    );
    assert_eq!(stats.slots_fetched, 3, "only the missing slots are fetched");

    // The backfilled rows are ordinary indexed events, readable like any other.
    // They carry the RPC block's transaction body rather than the live stream's
    // label, so they are identified by signature here.
    let filled: Vec<(i64, String)> = sqlx::query(
        "SELECT slot, data -> 'body' ->> 'signature' AS signature
         FROM events WHERE is_valid AND slot BETWEEN 6 AND 8 ORDER BY slot",
    )
    .fetch_all(&pool)
    .await?
    .into_iter()
    .map(|r| (r.get::<i64, _>("slot"), r.get::<String, _>("signature")))
    .collect();
    println!("backfilled events         : {filled:?}");
    assert_eq!(
        filled,
        vec![
            (6, "sig-6".to_string()),
            (7, "sig-7".to_string()),
            (8, "sig-8".to_string()),
        ],
        "one transaction per backfilled block, carrying the RPC payload"
    );
    let _ = &events;

    assert_no_duplicate_rows(&pool).await?;

    // Running it again must change nothing: the merge is idempotent.
    let backfiller = Backfiller::new(
        BackfillConfig {
            batch_delay: std::time::Duration::ZERO,
            ..Default::default()
        },
        &rpc.url,
        pool.clone(),
    )?;
    backfiller.add_range(BackfillRange::gap_fill(1, 10));
    let (pipeline, _done) = Pipeline::build(&config).await?;
    let (tx, rx) = mpsc::channel(64);
    let fill = tokio::spawn(async move { backfiller.run(tx).await });
    pipeline
        .ingest(ChannelSource::new("backfill again", rx))
        .await?;
    pipeline.shutdown().await?;
    fill.await??;

    let (valid_again, invalid_again) = row_counts(&pool).await?;
    println!("after a second backfill   : {valid_again} valid, {invalid_again} invalidated");
    assert_eq!(
        (valid, invalid),
        (valid_again, invalid_again),
        "a repeated backfill must be a no-op"
    );

    drop(rpc);
    pool.close().await;
    db.cleanup().await;
    Ok(())
}

/// A Solana JSON-RPC endpoint serving a fixed set of blocks.
struct FakeRpc {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeRpc {
    /// Serve `getBlocks`, `getBlock` and `getSlot` for the given slots.
    async fn start(available: std::ops::RangeInclusive<u64>) -> Result<Self> {
        let slots: Vec<u64> = available.collect();
        let head = *slots.last().unwrap_or(&0);

        let handler = move |Json(body): Json<serde_json::Value>| {
            let slots = slots.clone();
            async move {
                let method = body["method"].as_str().unwrap_or_default().to_string();
                let params = &body["params"];

                let result = match method.as_str() {
                    "getSlot" => serde_json::json!(head),
                    "getBlocks" => {
                        let start = params[0].as_u64().unwrap_or(0);
                        let end = params[1].as_u64().unwrap_or(0);
                        let visible: Vec<u64> = slots
                            .iter()
                            .copied()
                            .filter(|s| *s >= start && *s <= end)
                            .collect();
                        serde_json::json!(visible)
                    }
                    "getBlock" => {
                        let slot = params[0].as_u64().unwrap_or(0);
                        if slots.contains(&slot) {
                            serde_json::json!({
                                "parentSlot": slot - 1,
                                "blockhash": format!("hash-{slot}"),
                                "blockTime": 1_700_000_000i64 + slot as i64,
                                "transactions": [{
                                    "transaction": {
                                        "signatures": [format!("sig-{slot}")],
                                        "message": { "accountKeys": ["acct-backfill"] },
                                    },
                                    "meta": { "err": null, "fee": 5000, "computeUnitsConsumed": 900 },
                                }],
                            })
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    other => {
                        return Json(serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "error": { "code": -32601, "message": format!("no such method: {other}") },
                        }))
                    }
                };

                Json(serde_json::json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
            }
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding the RPC endpoint")?;
        let addr: SocketAddr = listener.local_addr()?;
        let router = Router::new().route("/", post(handler));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Ok(Self {
            url: format!("http://{addr}/"),
            task,
        })
    }
}
