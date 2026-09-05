//! The write path's idempotency, tested directly.
//!
//! The reorg proofs cover this indirectly — a replay that doubled rows would
//! fail them — but the property is specific enough to be worth pinning on its
//! own, because the interesting half of it only shows up in a state the proofs
//! reach by a long route: writing an event whose row is currently invalid.

mod support;

use anyhow::Result;
use slot_stream_common::{EventKind, IndexedEvent, RawEvent, RollbackPlan, SequenceNumber};
use slot_stream_persister::{migrate, Persister, PersisterConfig, PoolConfig, WriteResult};
use sqlx::{PgPool, Row};
use support::*;

/// An indexed event with a stable identity and a chosen assigned sequence.
fn indexed(source_seq: u64, slot: u64, parent: u64, seq: u64, label: &str) -> IndexedEvent {
    let raw = RawEvent::new(
        SequenceNumber(source_seq),
        EventKind::Transaction,
        slot,
        bytes::Bytes::from(serde_json::to_vec(&serde_json::json!({ "label": label })).unwrap()),
    )
    .with_parent(parent);

    let data = serde_json::json!({
        "kind": "Transaction",
        "slot": slot,
        "parent_slot": parent,
        "source_seq": source_seq,
        "body": { "label": label },
    });

    IndexedEvent::from_raw(raw, data, SequenceNumber(seq))
}

/// One row's state, as `(seq, is_valid, label)`.
async fn row(pool: &PgPool, slot: u64, source_seq: u64) -> Result<Option<(i64, bool, String)>> {
    let row = sqlx::query(
        "SELECT seq, is_valid, data -> 'body' ->> 'label' AS label
         FROM events WHERE slot = $1 AND source_seq = $2",
    )
    .bind(slot as i64)
    .bind(source_seq as i64)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| {
        (
            r.get::<i64, _>("seq"),
            r.get::<bool, _>("is_valid"),
            r.get::<Option<String>, _>("label").unwrap_or_default(),
        )
    }))
}

async fn persister(url: &str) -> Result<(Persister, PgPool)> {
    let pool = PoolConfig {
        database_url: url.to_string(),
        max_connections: 4,
        min_connections: 1,
        ..Default::default()
    }
    .create_pool()
    .await?;
    migrate(&pool).await?;
    Ok((
        Persister::new(pool.clone(), PersisterConfig::default()),
        pool,
    ))
}

#[tokio::test]
async fn rewriting_an_event_updates_its_row_instead_of_adding_one() -> Result<()> {
    let db = TestDb::create().await?;
    let (persister, pool) = persister(&db.url).await?;

    let first = persister.write(&indexed(7, 100, 99, 1, "original")).await?;
    // Same identity, a later assigned sequence, a revised payload: this is what a
    // redelivery after a reconnect looks like. The assigned sequence offered here
    // is deliberately different, to check it does not overwrite the stored one.
    let second = persister.write(&indexed(7, 100, 99, 42, "revised")).await?;

    let stored = row(&pool, 100, 7).await?;
    let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM events")
        .fetch_one(&pool)
        .await?
        .get("n");

    println!("--- rewrite ---");
    println!("first write : {first:?}");
    println!("second write: {second:?}");
    println!("stored row  : {stored:?}");
    println!("total rows  : {count}");

    assert_eq!(first, WriteResult::Inserted);
    assert_eq!(
        second,
        WriteResult::Updated,
        "the identity is (slot, source_seq), so this is the same row"
    );
    assert_eq!(count, 1, "a redelivery must not add a row");
    assert_eq!(
        stored,
        Some((1, true, "revised".to_string())),
        "the row carries the latest payload but keeps its original read position: \
         moving it to seq 42 would slip it past a reader already paging beyond 1"
    );

    let stats = persister.stats();
    assert_eq!(stats.events_written, 1, "one insert");
    assert_eq!(stats.events_updated, 1, "one update");

    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn rewriting_an_invalidated_event_brings_it_back() -> Result<()> {
    // The case that makes ON CONFLICT DO NOTHING wrong. After a reorg has
    // soft-deleted a slot, the cluster can switch back and redeliver those exact
    // events. If the write leaves the row invalid, the event is real, the row
    // exists, and no reader will ever see it again.
    let db = TestDb::create().await?;
    let (persister, pool) = persister(&db.url).await?;

    persister.write(&indexed(7, 100, 99, 1, "original")).await?;

    let plan = RollbackPlan {
        id: uuid::Uuid::new_v4(),
        slots_to_rollback: vec![100],
        slots_to_restore: Vec::new(),
        divergence_point: 99,
        fork_slot: 101,
        expected_parent: 100,
        actual_parent: 99,
        divergence_is_bound: false,
        created_at: chrono::Utc::now(),
    };
    let outcome = persister.rollback(&plan).await?;

    let invalidated = row(&pool, 100, 7).await?;
    println!("--- invalidate, then rewrite ---");
    println!("rollback    : {outcome:?}");
    println!("after reorg : {invalidated:?}");
    assert_eq!(outcome.events_invalidated, 1);
    assert_eq!(
        invalidated,
        Some((1, false, "original".to_string())),
        "the row is retained, marked invalid"
    );

    // The cluster switches back and redelivers.
    let result = persister.write(&indexed(7, 100, 99, 9, "original")).await?;
    let restored = row(&pool, 100, 7).await?;

    let invalidated_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query("SELECT invalidated_at FROM events WHERE slot = 100 AND source_seq = 7")
            .fetch_one(&pool)
            .await?
            .get("invalidated_at");

    println!("rewrite     : {result:?}");
    println!("after rewrite: {restored:?}, invalidated_at = {invalidated_at:?}");

    assert_eq!(result, WriteResult::Updated);
    assert_eq!(
        restored,
        Some((1, true, "original".to_string())),
        "redelivering an event asserts it is real; the write has to record that"
    );
    assert!(
        invalidated_at.is_none(),
        "the invalidation timestamp must be cleared too, or the row lies about itself"
    );

    pool.close().await;
    db.cleanup().await;
    Ok(())
}
