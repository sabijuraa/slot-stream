//! The reorg correctness proof.
//!
//! Each test drives a chain — including its forks — through the production
//! pipeline into a real PostgreSQL database, then asserts that what is persisted
//! equals what a from-scratch replay of the canonical chain would have produced.
//!
//! The expected value is computed by [`ChainScript::canonical_events`], which
//! builds a parent map, walks the final head's ancestry, and collects the events
//! of those slots. It shares no code with the fork detector under test, so it is
//! an independent yardstick rather than a restatement of the implementation.

mod support;

use anyhow::Result;
use slot_stream_ingester::ChainScript;
use support::*;

/// Run a script and assert persisted state equals the canonical replay.
async fn prove(label: &str, script: ChainScript) -> Result<()> {
    let db = TestDb::create().await?;
    let result = prove_inner(label, script, &db).await;
    db.cleanup().await;
    result
}

async fn prove_inner(label: &str, script: ChainScript, db: &TestDb) -> Result<()> {
    // The yardstick assumes every parent is present in the script. Where it is
    // not, the true chain below the gap is unknowable and the indexer keeps what
    // it cannot prove is orphaned, so comparing the two would be meaningless.
    // Gap behaviour is asserted directly in its own test instead.
    assert!(
        script.is_contiguous(),
        "{label}: canonical_events is only a valid expectation for a contiguous script"
    );

    let expected = script.canonical_events();
    let expected_chain = script.canonical_chain();

    run_script(&db.url, script).await?;

    let pool = db.pool().await?;
    let actual = persisted_events(&pool).await?;
    let (valid, invalid) = row_counts(&pool).await?;
    let reorgs = recorded_reorgs(&pool).await?;

    report(label, &expected, &actual);
    println!("canonical chain: {expected_chain:?}");
    println!("rows: {valid} valid, {invalid} invalidated");
    println!("reorgs recorded: {reorgs:?}");

    assert_no_duplicate_rows(&pool).await?;

    assert_eq!(
        actual, expected,
        "persisted state does not equal a canonical replay for {label}"
    );
    assert_eq!(
        valid as usize,
        expected.len(),
        "valid row count disagrees with the canonical replay"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn no_reorg_persists_the_whole_chain() -> Result<()> {
    // The control: without a fork, every event must survive.
    prove(
        "straight chain, no fork",
        ChainScript::new().extend_from(100, 20, 3),
    )
    .await
}

#[tokio::test]
async fn reorg_depth_1() -> Result<()> {
    // 100..119, then 120 builds on 118, orphaning only slot 119.
    prove(
        "reorg depth 1",
        ChainScript::new()
            .extend_from(100, 20, 3)
            .fork_run(120, 118, 4, 3),
    )
    .await
}

#[tokio::test]
async fn reorg_depth_5() -> Result<()> {
    // 100..119, then a branch from 114, orphaning 115..119.
    prove(
        "reorg depth 5",
        ChainScript::new()
            .extend_from(100, 20, 3)
            .fork_run(120, 114, 8, 3),
    )
    .await
}

#[tokio::test]
async fn reorg_depth_32() -> Result<()> {
    // 100..151, then a branch from 119, orphaning 120..151: 32 slots.
    prove(
        "reorg depth 32",
        ChainScript::new()
            .extend_from(100, 52, 3)
            .fork_run(152, 119, 10, 3),
    )
    .await
}

#[tokio::test]
async fn competing_forks_resolve_to_the_last_branch() -> Result<()> {
    // Three branches contend off the same ancestor. Whichever arrives last is
    // the one the persisted state must reflect, with the other two rolled back.
    prove(
        "three competing branches",
        ChainScript::new()
            .extend_from(100, 10, 2) // 100..109
            .fork_run(110, 104, 4, 2) // branch A off 104
            .fork_run(120, 106, 4, 2) // branch B off 106
            .fork_run(130, 102, 5, 2), // branch C off 102 wins
    )
    .await
}

#[tokio::test]
async fn repeated_reorgs_leave_state_correct() -> Result<()> {
    // Several forks in succession. The detector must recover between each; the
    // previous implementation deadlocked after the first because nothing ever
    // moved its state machine out of Planning.
    let mut script = ChainScript::new().extend_from(1_000, 15, 2);
    let mut next = 1_015;
    for i in 0..6 {
        let parent = 1_000 + (i * 2) + 1;
        script = script.fork_run(next, parent, 4, 2);
        next += 10;
    }
    prove("six consecutive reorgs", script).await
}

/// A branch whose ancestry reaches into a gap.
///
/// The indexer cannot see whether slots 100..104 are ancestors of the new branch,
/// because the slot it descends from was never delivered. The documented rule is
/// to roll back only what is provably orphaned — canonical slots above the
/// divergence point — and to keep the rest rather than guess. This asserts that
/// rule directly rather than against the replay yardstick, which has no way to
/// model the gap.
#[tokio::test]
async fn a_branch_reaching_into_a_gap_keeps_what_it_cannot_disprove() -> Result<()> {
    let db = TestDb::create().await?;

    let script = ChainScript::new()
        .extend_from(100, 5, 2) // 100..104
        .fork_run(200, 150, 3, 2); // 200 names parent 150, which never arrives

    assert!(!script.is_contiguous(), "this script deliberately has a gap");
    run_script(&db.url, script).await?;

    let pool = db.pool().await?;
    let slots = persisted_slots(&pool).await?;
    let (valid, invalid) = row_counts(&pool).await?;
    let reorgs = recorded_reorgs(&pool).await?;

    println!("--- branch reaching into a gap ---");
    println!("valid slots: {slots:?}");
    println!("rows: {valid} valid, {invalid} invalidated");
    println!("reorgs: {reorgs:?}");

    // Nothing canonical sat above the divergence point of 150, so nothing is
    // orphaned, and the new branch is adopted alongside what came before.
    assert_eq!(invalid, 0, "nothing was provably orphaned");
    assert_eq!(slots, vec![100, 101, 102, 103, 104, 200, 201, 202]);
    assert_eq!(reorgs.len(), 1, "the divergence is still recorded");
    assert_eq!(reorgs[0].1, 150, "divergence point is the unreachable parent");
    assert_eq!(reorgs[0].2, 0, "with an empty rollback set");

    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn deep_reorg_actually_invalidates_the_orphaned_rows() -> Result<()> {
    // Beyond equality with the replay, check the mechanism: the orphaned rows
    // must still be present and marked invalid, not deleted, and the reorg must
    // be recorded with the right depth.
    let db = TestDb::create().await?;
    let script = ChainScript::new()
        .extend_from(100, 52, 3)
        .fork_run(152, 119, 6, 3);

    let expected = script.canonical_events();
    run_script(&db.url, script).await?;

    let pool = db.pool().await?;
    let actual = persisted_events(&pool).await?;
    let (valid, invalid) = row_counts(&pool).await?;
    let reorgs = recorded_reorgs(&pool).await?;

    println!("--- deep reorg mechanism ---");
    println!("valid rows: {valid}, invalidated rows: {invalid}");
    println!("reorgs: {reorgs:?}");

    assert_eq!(actual, expected, "state must match the canonical replay");
    assert_eq!(
        invalid, 96,
        "32 orphaned slots at 3 events each must be soft-deleted, not removed"
    );
    assert_eq!(reorgs.len(), 1, "exactly one reorg should be recorded");
    let (fork_slot, divergence, depth, invalidated) = reorgs[0];
    assert_eq!(fork_slot, 152);
    assert_eq!(divergence, 119);
    assert_eq!(depth, 32, "rollback depth must be the full orphaned span");
    assert_eq!(invalidated, 96);

    pool.close().await;
    db.cleanup().await;
    Ok(())
}

#[tokio::test]
async fn re_applying_an_orphaned_slot_brings_its_rows_back() -> Result<()> {
    // The idempotency bug this project shipped with: after a rollback soft-deletes
    // a slot, re-delivering the same events must restore them. Without the
    // is_valid reset in the upsert they stay invisible forever.
    let db = TestDb::create().await?;

    // Fork away from 104, then return to it: the final head descends through 104
    // again, so its events must be valid at the end.
    let script = ChainScript::new()
        .extend_from(100, 8, 2) // 100..107
        .fork_run(110, 103, 2, 2) // orphans 104..107
        .fork_run(104, 103, 4, 2); // re-delivers 104, then 105, 106, 107

    let expected = script.canonical_events();
    run_script(&db.url, script).await?;

    let pool = db.pool().await?;
    let actual = persisted_events(&pool).await?;
    let slots = persisted_slots(&pool).await?;

    println!("--- re-apply after rollback ---");
    println!("valid slots: {slots:?}");
    report("re-apply", &expected, &actual);

    assert!(
        slots.contains(&104),
        "slot 104 was orphaned then re-delivered; its rows must be valid again"
    );
    assert_eq!(actual, expected);

    pool.close().await;
    db.cleanup().await;
    Ok(())
}
