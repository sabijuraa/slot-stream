//! The read API over indexed data.
//!
//! The pipeline runs for real, a reorg happens for real, and the API is served
//! over a real TCP socket and queried with a real HTTP client. The property
//! being proven is that the API is reorg-aware without knowing anything about
//! forks: orphaned rows are still in the table, and are invisible to every
//! endpoint.

mod support;

use anyhow::{Context, Result};
use slot_stream_api::{ApiState, EventStore};
use slot_stream_common::EventKind;
use slot_stream_ingester::{ChainScript, ScriptedEvent, ScriptedSlot, ScriptedSource};
use slot_stream_pipeline::Pipeline;
use sqlx::Row;
use std::net::SocketAddr;
use support::*;

/// A transaction body shaped like the one the API queries against.
fn transaction(slot: u64, index: usize, branch: &str) -> ScriptedEvent {
    ScriptedEvent::new(
        EventKind::Transaction,
        serde_json::json!({
            "label": format!("slot{slot}-ev{index}"),
            "signature": format!("sig-{branch}-{slot}-{index}"),
            "message": { "accountKeys": [format!("acct-{branch}"), "acct-shared"] },
        }),
    )
}

/// A slot of such transactions.
fn slot_of(slot: u64, parent: u64, count: usize, branch: &str) -> ScriptedSlot {
    ScriptedSlot {
        slot,
        parent,
        events: (0..count).map(|i| transaction(slot, i, branch)).collect(),
    }
}

/// A live API server bound to an ephemeral port.
struct ApiServer {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl ApiServer {
    async fn start(store: EventStore) -> Result<Self> {
        Self::serve(ApiState::new(store)).await
    }

    /// The same server, with a Prometheus handle attached.
    async fn start_with_metrics(
        store: EventStore,
        handle: metrics_exporter_prometheus::PrometheusHandle,
    ) -> Result<Self> {
        Self::serve(ApiState::new(store).with_metrics(handle)).await
    }

    async fn serve(state: ApiState) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding the API")?;
        let addr = listener.local_addr()?;
        let router = slot_stream_api::router(state);
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(Self { addr, task })
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

impl Drop for ApiServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// GET, returning the status and the decoded body.
async fn get(client: &reqwest::Client, url: String) -> Result<(u16, serde_json::Value)> {
    let response = client.get(&url).send().await.with_context(|| url.clone())?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    let value = serde_json::from_str(&body).unwrap_or(serde_json::Value::String(body));
    Ok((status, value))
}

/// The slots each item in a list response came from.
fn slots_in(body: &serde_json::Value) -> Vec<u64> {
    body["items"]
        .as_array()
        .map(|items| items.iter().filter_map(|i| i["slot"].as_u64()).collect())
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_api_serves_indexed_data_and_hides_what_a_reorg_orphaned() -> Result<()> {
    let db = TestDb::create().await?;

    // Branch "a" runs slots 1..=10. Branch "b" then arrives as slots 11..=14
    // building on slot 7, which is what a real competing branch looks like: new
    // slot numbers off an older parent. Slots 8, 9 and 10 are orphaned.
    let mut script = ChainScript::new();
    for slot in 1..=10u64 {
        script
            .slots
            .push(slot_of(slot, slot.saturating_sub(1), 3, "a"));
    }
    for slot in 11..=14u64 {
        let parent = if slot == 11 { 7 } else { slot - 1 };
        script.slots.push(slot_of(slot, parent, 3, "b"));
    }

    // What the API must end up serving, independent of anything the pipeline
    // computed: branch a below the divergence point, then branch b.
    let canonical: Vec<u64> = (1..=7u64).chain(11..=14u64).collect();
    let expected_rows: Vec<u64> = canonical
        .iter()
        .flat_map(|s| std::iter::repeat_n(*s, 3))
        .collect();

    let config = test_config(&db.url);
    let (pipeline, _done) = Pipeline::build(&config).await?;
    pipeline.ingest(ScriptedSource::new(script)).await?;
    pipeline.shutdown().await?;

    let pool = db.pool().await?;
    let (valid, invalidated) = row_counts(&pool).await?;

    let store = EventStore::new(pool.clone(), 1_000, 100);
    let server = ApiServer::start(store).await?;
    let client = reqwest::Client::new();

    println!("--- read API against a reorged index ---");
    println!("API bound to           : {}", server.addr);
    println!("rows valid / invalidated: {valid} / {invalidated}");

    // Liveness and readiness.
    let (status, body) = get(&client, server.url("/health/live")).await?;
    println!("GET /health/live       : {status} {body}");
    assert_eq!(status, 200);

    let (status, body) = get(&client, server.url("/health/ready")).await?;
    println!("GET /health/ready      : {status} {body}");
    assert_eq!(status, 200, "readiness must reflect a reachable database");
    assert_eq!(body["status"], "ready");

    // Status carries the counts the database actually holds.
    let (status, body) = get(&client, server.url("/v1/status")).await?;
    println!("GET /v1/status         : {status} {body}");
    assert_eq!(status, 200);
    assert_eq!(
        body["events"]["valid"].as_i64(),
        Some(valid),
        "the API's valid count must match the table"
    );
    assert_eq!(
        body["events"]["invalidated"].as_i64(),
        Some(invalidated),
        "the API's invalidated count must match the table"
    );
    assert!(
        invalidated > 0,
        "the fork must have orphaned rows, or this test proves nothing"
    );

    // The chain head is the canonical tip.
    let (status, body) = get(&client, server.url("/v1/chain/head")).await?;
    println!("GET /v1/chain/head     : {status} {body}");
    assert_eq!(status, 200);
    assert_eq!(body["head"].as_u64(), Some(14));

    // The full listing is the canonical chain, exactly once per slot.
    let (status, body) = get(&client, server.url("/v1/events?limit=1000")).await?;
    let listed = slots_in(&body);
    println!(
        "GET /v1/events         : {status} count={} slots={:?}",
        body["count"], listed
    );
    assert_eq!(status, 200);
    assert_eq!(
        body["count"].as_i64(),
        Some(valid),
        "the listing must return every valid row"
    );
    assert_eq!(
        listed, expected_rows,
        "each canonical slot appears once per event, and no orphan appears at all"
    );

    // A slot the fork orphaned serves nothing at all.
    let (status, body) = get(&client, server.url("/v1/events/slot/8")).await?;
    println!(
        "GET /v1/events/slot/8  : {status} count={} (orphaned)",
        body["count"]
    );
    assert_eq!(status, 200);
    assert_eq!(
        body["count"].as_i64(),
        Some(0),
        "an orphaned slot must serve nothing"
    );

    // The slot that won serves its own events.
    let (status, body) = get(&client, server.url("/v1/events/slot/11")).await?;
    let signatures: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["data"]["body"]["signature"].as_str().unwrap())
        .collect();
    println!(
        "GET /v1/events/slot/11 : {status} count={} sigs={signatures:?}",
        body["count"]
    );
    assert_eq!(status, 200);
    assert_eq!(body["count"].as_i64(), Some(3));
    assert!(
        signatures.iter().all(|s| s.starts_with("sig-b-")),
        "slot 11 belongs to the surviving branch, got {signatures:?}"
    );

    // The orphaned rows are still on disk. Invisible is not deleted.
    let orphans: i64 =
        sqlx::query("SELECT COUNT(*) AS n FROM events WHERE is_valid = false AND slot = 8")
            .fetch_one(&pool)
            .await?
            .get("n");
    println!("orphaned rows at slot 8: {orphans} (retained for audit)");
    assert_eq!(orphans, 3, "the losing branch is retained, just not served");

    // Lookup by signature: the winner resolves, the orphan does not.
    let (status, body) = get(&client, server.url("/v1/events/signature/sig-b-11-0")).await?;
    println!(
        "GET .../signature/sig-b-11-0 : {status} count={}",
        body["count"]
    );
    assert_eq!(status, 200);
    assert_eq!(body["count"].as_i64(), Some(1));
    assert_eq!(body["items"][0]["slot"].as_u64(), Some(11));

    let (status, body) = get(&client, server.url("/v1/events/signature/sig-a-8-0")).await?;
    println!(
        "GET .../signature/sig-a-8-0 : {status} count={}",
        body["count"]
    );
    assert_eq!(status, 200);
    assert_eq!(
        body["count"].as_i64(),
        Some(0),
        "an orphaned signature must not resolve"
    );

    // Lookup by account, which reads a nested array in the payload.
    let (status, body) = get(&client, server.url("/v1/events/account/acct-b?limit=100")).await?;
    let listed = slots_in(&body);
    println!("GET .../account/acct-b : {status} slots={listed:?}");
    assert_eq!(status, 200);
    assert_eq!(
        listed,
        (11..=14u64)
            .flat_map(|s| std::iter::repeat_n(s, 3))
            .collect::<Vec<_>>(),
        "the surviving branch's account appears on exactly its own slots"
    );

    let (status, body) = get(&client, server.url("/v1/events/account/acct-a?limit=100")).await?;
    let listed = slots_in(&body);
    println!("GET .../account/acct-a : {status} slots={listed:?}");
    assert_eq!(status, 200);
    assert_eq!(
        listed,
        (1..=7u64)
            .flat_map(|s| std::iter::repeat_n(s, 3))
            .collect::<Vec<_>>(),
        "branch a survives only below the divergence point"
    );

    // Slot metadata is still served for an orphaned slot, flagged as off-chain.
    // Hiding its events and admitting the slot existed are different questions.
    let (status, body) = get(&client, server.url("/v1/slots/8")).await?;
    println!("GET /v1/slots/8        : {status} {body}");
    assert_eq!(status, 200);
    assert_eq!(body["slot"].as_u64(), Some(8));
    assert_eq!(
        body["is_canonical"].as_bool(),
        Some(false),
        "an orphaned slot must not claim to be canonical"
    );

    let (status, body) = get(&client, server.url("/v1/slots/11")).await?;
    println!("GET /v1/slots/11       : {status} {body}");
    assert_eq!(status, 200);
    assert_eq!(
        body["parent_slot"].as_u64(),
        Some(7),
        "branch b builds on slot 7"
    );
    assert_eq!(body["is_canonical"].as_bool(), Some(true));

    // A slot that was never indexed is a 404, not an empty 200.
    let (status, body) = get(&client, server.url("/v1/slots/99999")).await?;
    println!("GET /v1/slots/99999    : {status} {body}");
    assert_eq!(status, 404);

    // Range filters compose with the validity filter.
    let (status, body) = get(
        &client,
        server.url("/v1/events?from_slot=5&to_slot=11&limit=1000"),
    )
    .await?;
    let listed = slots_in(&body);
    println!("GET /v1/events?5..11   : {status} slots={listed:?}");
    assert_eq!(status, 200);
    assert_eq!(
        listed,
        [5u64, 6, 7, 11]
            .iter()
            .flat_map(|s| std::iter::repeat_n(*s, 3))
            .collect::<Vec<_>>(),
        "the range filter skips the orphaned slots inside it"
    );

    // Paging is honoured and clamped.
    let (status, body) = get(&client, server.url("/v1/events?limit=5")).await?;
    println!("GET /v1/events?limit=5 : {status} count={}", body["count"]);
    assert_eq!(status, 200);
    assert_eq!(body["count"].as_i64(), Some(5));

    // Metrics are off in this configuration, and say so rather than lying.
    let (status, _) = get(&client, server.url("/metrics")).await?;
    println!("GET /metrics           : {status} (disabled in this config)");
    assert_eq!(status, 503);

    drop(server);
    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_render_what_the_pipeline_actually_did() -> Result<()> {
    // Observability is easy to document and easy to get wrong: a metric that is
    // declared but never emitted looks fine in a dashboard definition and is
    // empty when an incident starts. So the recorder is installed for real, a
    // chain with a fork is indexed for real, and /metrics is scraped over HTTP.
    let db = TestDb::create().await?;

    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("installing the Prometheus recorder")?;

    let script = ChainScript::new()
        .extend_from(1, 20, 3)
        .fork_run(21, 15, 5, 3);

    let config = test_config(&db.url);
    let (pipeline, _done) = Pipeline::build(&config).await?;
    pipeline.ingest(ScriptedSource::new(script)).await?;
    pipeline.shutdown().await?;

    let pool = db.pool().await?;
    let store = EventStore::new(pool.clone(), 1_000, 100);
    let server = ApiServer::start_with_metrics(store, handle).await?;
    let client = reqwest::Client::new();

    let response = client.get(server.url("/metrics")).send().await?;
    let status = response.status().as_u16();
    let body = response.text().await?;

    println!("--- /metrics after indexing a chain with a fork ---");
    println!("GET /metrics : {status}, {} bytes", body.len());

    assert_eq!(status, 200, "with a recorder installed this must serve");

    // The series an operator alerts on. Each must be present with a value, not
    // merely declared somewhere in the source.
    for metric in [
        "ingester_events_emitted",
        "processor_events",
        "processor_reorgs",
        "processor_rollback_depth",
        "persister_events_written",
        "persister_batches_written",
        "persister_rollbacks_executed",
        "persister_events_invalidated",
    ] {
        let line = body
            .lines()
            .find(|l| l.starts_with(metric) && !l.starts_with('#'))
            .unwrap_or_else(|| panic!("/metrics has no series for {metric}:\n{body}"));
        println!("  {line}");
    }

    pool.close().await;
    db.cleanup().await;
    Ok(())
}
