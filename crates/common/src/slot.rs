//! Slot chain tracking and fork detection.
//!
//! Solana's chain is a tree, not a list. Each slot names a parent, and the
//! canonical chain is the path from our root to the current head. A reorg is the
//! cluster choosing a different path through that tree, which orphans everything
//! we had accepted above the point where the two paths meet.
//!
//! # Detecting a reorg
//!
//! A slot `S` arriving with parent `P` is one of:
//!
//! - `P == head` — an ordinary extension. Nothing to do.
//! - no head yet — the chain starts here.
//! - already known with the same parent — a duplicate or a status update.
//! - anything else — a divergence.
//!
//! That last case is the one that matters, and it is broader than it looks. It
//! covers the textbook fork (slot 105 arrives a second time naming a different
//! parent) but also the far more common shape: the head is 105 and slot 106
//! arrives naming parent 101, orphaning 102 through 105. An implementation that
//! only compares a slot against a previous copy of *itself* misses that entirely,
//! because 106 has never been seen before.
//!
//! # Finding the divergence point
//!
//! Given the incoming parent `P`, walk back through known parent links until
//! reaching a slot that is on the canonical chain. That slot is the common
//! ancestor `A`, and every canonical slot above `A` is orphaned.
//!
//! The walk is minimal, and it is worth being explicit about why. `P` is `S`'s
//! direct parent, so every *other* ancestor of `S` has a slot number strictly
//! below `P`. A canonical slot above the common ancestor therefore cannot be an
//! ancestor of `S`, so rolling it back is always sound. And we roll back no more
//! than that, so we never discard a slot the new chain still depends on.
//!
//! If the walk runs off the end of what we know — the branch reaches back past
//! our retention window, or into a gap — we stop at the deepest slot we can
//! justify and roll back canonical slots above it. The same argument applies:
//! everything on the new branch sits at or below that slot number.
//!
//! # Rooted slots
//!
//! Solana guarantees rooted slots never revert. If a divergence would orphan one,
//! that is a violated invariant rather than a routine reorg, and we surface it as
//! an error instead of quietly destroying finalised state.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fmt;

/// Status of a slot in the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotStatus {
    /// Slot is being processed, not yet confirmed.
    Processing,
    /// Slot is confirmed by the cluster.
    Confirmed,
    /// Slot is rooted (finalized, cannot be reverted).
    Rooted,
    /// Slot was skipped (no block produced).
    Skipped,
    /// Slot was orphaned due to fork resolution.
    Orphaned,
}

impl SlotStatus {
    /// Returns true if this slot can still be reverted.
    pub fn is_revocable(&self) -> bool {
        matches!(self, SlotStatus::Processing | SlotStatus::Confirmed)
    }

    /// Returns true if this slot is finalized.
    pub fn is_final(&self) -> bool {
        matches!(self, SlotStatus::Rooted | SlotStatus::Orphaned)
    }
}

/// Information about a specific slot in the chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotInfo {
    /// The slot number.
    pub slot: u64,

    /// Parent slot number (for chain linking).
    pub parent_slot: u64,

    /// Block hash if available. Distinguishes competing blocks at one slot.
    pub block_hash: Option<String>,

    /// Current status.
    pub status: SlotStatus,

    /// Timestamp when this slot info was received.
    pub received_at: chrono::DateTime<chrono::Utc>,

    /// Block time from the chain (if available).
    pub block_time: Option<i64>,

    /// Number of transactions in this slot.
    pub transaction_count: u32,
}

impl SlotInfo {
    /// Create a new slot info.
    pub fn new(slot: u64, parent_slot: u64) -> Self {
        Self {
            slot,
            parent_slot,
            block_hash: None,
            status: SlotStatus::Processing,
            received_at: chrono::Utc::now(),
            block_time: None,
            transaction_count: 0,
        }
    }

    /// Attach a block hash.
    pub fn with_block_hash(mut self, hash: impl Into<String>) -> Self {
        self.block_hash = Some(hash.into());
        self
    }

    /// Attach a status.
    pub fn with_status(mut self, status: SlotStatus) -> Self {
        self.status = status;
        self
    }

    /// Check if this slot's parent matches the expected parent.
    pub fn parent_matches(&self, expected_parent: u64) -> bool {
        self.parent_slot == expected_parent
    }

    /// Check if this slot is the direct child of another slot.
    pub fn is_child_of(&self, other: &SlotInfo) -> bool {
        self.parent_slot == other.slot
    }

    /// Two records describe the same block if they agree on parent and hash.
    fn same_block_as(&self, other: &SlotInfo) -> bool {
        self.parent_slot == other.parent_slot && self.block_hash == other.block_hash
    }
}

impl fmt::Display for SlotInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Slot {} (parent: {}, status: {:?})",
            self.slot, self.parent_slot, self.status
        )
    }
}

/// Information about a detected fork.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkInfo {
    /// The slot whose arrival revealed the fork.
    pub fork_slot: u64,

    /// The head we were building on before this slot arrived.
    pub expected_parent: u64,

    /// The parent the incoming slot actually named.
    pub actual_parent: u64,

    /// Canonical slots that are now orphaned, descending (deepest rollback last).
    pub slots_to_rollback: Vec<u64>,

    /// The common ancestor of the old head and the new branch.
    pub divergence_point: u64,

    /// Slots of the new branch that we already know about, ascending. These sit
    /// between the divergence point and the incoming slot.
    pub new_branch: Vec<u64>,

    /// True when the walk could not reach the canonical chain and the divergence
    /// point is a lower bound rather than an observed common ancestor.
    pub divergence_is_bound: bool,

    /// When the fork was detected.
    pub detected_at: chrono::DateTime<chrono::Utc>,
}

impl ForkInfo {
    /// Create a new fork info.
    pub fn new(
        fork_slot: u64,
        expected_parent: u64,
        actual_parent: u64,
        divergence_point: u64,
    ) -> Self {
        Self {
            fork_slot,
            expected_parent,
            actual_parent,
            slots_to_rollback: Vec::new(),
            divergence_point,
            new_branch: Vec::new(),
            divergence_is_bound: false,
            detected_at: chrono::Utc::now(),
        }
    }

    /// Add slots that need to be rolled back.
    pub fn with_rollback_slots(mut self, mut slots: Vec<u64>) -> Self {
        slots.sort_unstable_by(|a, b| b.cmp(a));
        self.slots_to_rollback = slots;
        self
    }

    /// Add the known slots of the new branch.
    pub fn with_new_branch(mut self, mut slots: Vec<u64>) -> Self {
        slots.sort_unstable();
        self.new_branch = slots;
        self
    }

    /// Get the number of slots that need rollback.
    pub fn rollback_depth(&self) -> usize {
        self.slots_to_rollback.len()
    }

    /// True when the fork orphaned nothing we had persisted.
    pub fn is_empty(&self) -> bool {
        self.slots_to_rollback.is_empty()
    }
}

impl fmt::Display for ForkInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Fork at slot {}: was building on {}, incoming parent {} (diverged at {}{}, rolling back {} slots)",
            self.fork_slot,
            self.expected_parent,
            self.actual_parent,
            self.divergence_point,
            if self.divergence_is_bound { ", bound" } else { "" },
            self.slots_to_rollback.len()
        )
    }
}

/// Why a slot was ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IgnoreReason {
    /// The slot is below the retention window and cannot be reasoned about.
    BelowRetentionWindow,
    /// The slot is already rooted; the cluster cannot revert it.
    AlreadyRooted,
}

/// What processing a slot did to the chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainUpdate {
    /// First slot seen; the chain starts here.
    Initialised { slot: u64 },
    /// The slot extends the current head.
    Extended { slot: u64 },
    /// Already known with the same parent and hash; nothing changed.
    Duplicate { slot: u64 },
    /// Outside the window we can reason about; deliberately not applied.
    Ignored { slot: u64, reason: IgnoreReason },
    /// The canonical chain moved to a different branch.
    Reorg(Box<ForkInfo>),
}

impl ChainUpdate {
    /// The fork description, when this update was a reorg.
    pub fn fork(&self) -> Option<&ForkInfo> {
        match self {
            ChainUpdate::Reorg(info) => Some(info),
            _ => None,
        }
    }

    /// True when the canonical chain changed shape rather than just growing.
    pub fn is_reorg(&self) -> bool {
        matches!(self, ChainUpdate::Reorg(_))
    }
}

/// Statistics about chain tracking.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainStats {
    pub slots_tracked: usize,
    pub canonical_len: usize,
    pub reorgs: u64,
    pub slots_orphaned: u64,
    pub max_reorg_depth: usize,
    pub slots_pruned: u64,
}

/// Tracks the slot tree and identifies which branch is canonical.
///
/// Memory is bounded by `max_slots`: slots further below the head than that are
/// dropped. Pruning is driven by the head rather than by the rooted watermark,
/// so a stream that never reports roots still stays bounded.
#[derive(Debug)]
pub struct SlotChainTracker {
    /// Every slot we currently know about, on any branch.
    slots: HashMap<u64, SlotInfo>,

    /// Slot numbers on the canonical chain, ascending.
    canonical: BTreeSet<u64>,

    /// Tip of the canonical chain.
    head: Option<u64>,

    /// The highest rooted (finalized) slot.
    highest_rooted: Option<u64>,

    /// How many slots below the head to retain.
    max_slots: usize,

    /// Statistics.
    stats: ChainStats,
}

impl SlotChainTracker {
    /// Create a new slot chain tracker retaining `max_slots` below the head.
    pub fn new(max_slots: usize) -> Self {
        Self {
            slots: HashMap::with_capacity(max_slots.min(4096)),
            canonical: BTreeSet::new(),
            head: None,
            highest_rooted: None,
            max_slots: max_slots.max(1),
            stats: ChainStats::default(),
        }
    }

    /// The tip of the canonical chain.
    pub fn head(&self) -> Option<u64> {
        self.head
    }

    /// The tip of the canonical chain. Retained for call sites that read it as
    /// "the highest slot we have accepted".
    pub fn highest_slot(&self) -> Option<u64> {
        self.head
    }

    /// The highest rooted slot.
    pub fn highest_rooted(&self) -> Option<u64> {
        self.highest_rooted
    }

    /// Lowest slot still retained.
    pub fn oldest_tracked(&self) -> Option<u64> {
        self.slots.keys().min().copied()
    }

    /// True when `slot` is currently on the canonical chain.
    pub fn is_canonical(&self, slot: u64) -> bool {
        self.canonical.contains(&slot)
    }

    /// The canonical chain, ascending.
    pub fn canonical_chain(&self) -> Vec<u64> {
        self.canonical.iter().copied().collect()
    }

    /// Get slot info by slot number.
    pub fn get(&self, slot: u64) -> Option<&SlotInfo> {
        self.slots.get(&slot)
    }

    /// Statistics snapshot.
    pub fn stats(&self) -> ChainStats {
        let mut stats = self.stats.clone();
        stats.slots_tracked = self.slots.len();
        stats.canonical_len = self.canonical.len();
        stats
    }

    /// Process a slot, reporting what it did to the canonical chain.
    ///
    /// Returns an error only when applying the slot would revert a rooted slot,
    /// which the cluster promises never happens.
    pub fn process_slot(&mut self, info: SlotInfo) -> Result<ChainUpdate, crate::Error> {
        let slot = info.slot;
        let parent = info.parent_slot;

        // Below the retention window we have no parent links left, so we cannot
        // say whether this belongs to the canonical chain. Refusing is honest;
        // guessing would corrupt state.
        if let Some(oldest) = self.retention_floor() {
            if slot < oldest {
                return Ok(ChainUpdate::Ignored {
                    slot,
                    reason: IgnoreReason::BelowRetentionWindow,
                });
            }
        }

        let head = match self.head {
            None => {
                self.insert_canonical(info);
                return Ok(ChainUpdate::Initialised { slot });
            }
            Some(head) => head,
        };

        // Same block arriving again: a retransmit or a status update.
        //
        // Only when the slot is still canonical. Re-delivery of a slot we had
        // orphaned is the cluster switching back to that branch, which is a
        // reorg and has to be resolved as one — treating it as a duplicate would
        // leave the abandoned branch canonical and the returning one invisible.
        if let Some(existing) = self
            .slots
            .get(&slot)
            .filter(|_| self.canonical.contains(&slot))
        {
            if existing.same_block_as(&info) {
                let previously_rooted = existing.status == SlotStatus::Rooted;
                let entry = self.slots.get_mut(&slot).expect("checked above");
                // Never downgrade a rooted slot on a retransmit.
                if !previously_rooted {
                    entry.status = info.status;
                }
                if entry.block_hash.is_none() {
                    entry.block_hash = info.block_hash.clone();
                }
                if info.status == SlotStatus::Rooted {
                    self.mark_rooted(slot);
                }
                return Ok(ChainUpdate::Duplicate { slot });
            }
        }

        // The ordinary case: this slot builds directly on our head.
        if parent == head {
            self.insert_canonical(info);
            return Ok(ChainUpdate::Extended { slot });
        }

        // Anything else is a divergence.
        self.handle_divergence(info, head)
    }

    /// Resolve a divergence into a fork description and switch branches.
    fn handle_divergence(
        &mut self,
        info: SlotInfo,
        head: u64,
    ) -> Result<ChainUpdate, crate::Error> {
        let slot = info.slot;
        let parent = info.parent_slot;

        let (divergence_point, branch, is_bound) = self.find_divergence(parent);

        // Everything canonical above the common ancestor is orphaned.
        let orphaned: Vec<u64> = self
            .canonical
            .range((divergence_point + 1)..)
            .copied()
            .collect();

        // Rooted slots do not revert. If one is in the rollback set, our view of
        // the chain and the cluster's have genuinely disagreed.
        if let Some(&rooted) = orphaned
            .iter()
            .find(|s| self.slots.get(s).map(|i| i.status) == Some(SlotStatus::Rooted))
        {
            return Err(crate::Error::RollbackFailed {
                slot: rooted,
                reason: format!(
                    "slot {rooted} is rooted but slot {slot} (parent {parent}) would orphan it; \
                     divergence computed at {divergence_point}"
                ),
            });
        }

        for orphan in &orphaned {
            self.canonical.remove(orphan);
            if let Some(entry) = self.slots.get_mut(orphan) {
                entry.status = SlotStatus::Orphaned;
            }
        }
        self.refresh_head();

        // Adopt the new branch: the slots we walked, then the incoming slot.
        for &branch_slot in &branch {
            self.canonical.insert(branch_slot);
        }
        self.insert_canonical(info);

        self.stats.reorgs += 1;
        self.stats.slots_orphaned += orphaned.len() as u64;
        self.stats.max_reorg_depth = self.stats.max_reorg_depth.max(orphaned.len());

        let mut fork = ForkInfo::new(slot, head, parent, divergence_point)
            .with_rollback_slots(orphaned)
            .with_new_branch(branch);
        fork.divergence_is_bound = is_bound;

        Ok(ChainUpdate::Reorg(Box::new(fork)))
    }

    /// Walk back from `parent` to the canonical chain.
    ///
    /// Returns the common ancestor, the non-canonical slots walked through
    /// (ascending), and whether the ancestor is an observed slot or only a lower
    /// bound because the walk left the region we retain.
    fn find_divergence(&self, parent: u64) -> (u64, Vec<u64>, bool) {
        let mut branch = Vec::new();
        let mut cursor = parent;

        // The walk is bounded by the number of slots we hold, so a corrupt parent
        // cycle terminates instead of spinning.
        for _ in 0..=self.slots.len() {
            if self.canonical.contains(&cursor) {
                branch.reverse();
                return (cursor, branch, false);
            }
            match self.slots.get(&cursor) {
                Some(entry) => {
                    branch.push(cursor);
                    cursor = entry.parent_slot;
                }
                None => {
                    // We have run out of known links. `cursor` was named as a
                    // parent by the branch, so every slot on the new branch is at
                    // or below it, and canonical slots above it cannot be
                    // ancestors of the incoming slot.
                    branch.reverse();
                    return (cursor, branch, true);
                }
            }
        }

        // Only reachable if parent links form a cycle.
        branch.reverse();
        (cursor.min(parent), branch, true)
    }

    /// Insert a slot and put it on the canonical chain.
    fn insert_canonical(&mut self, info: SlotInfo) {
        let slot = info.slot;
        let rooted = info.status == SlotStatus::Rooted;
        self.slots.insert(slot, info);
        self.canonical.insert(slot);
        self.refresh_head();
        if rooted {
            self.mark_rooted(slot);
        }
        self.prune();
    }

    /// Recompute the head from the canonical chain.
    ///
    /// The head is the highest slot on the canonical chain, which is not the
    /// same as the highest slot ever seen. A reorg can move the head *down* —
    /// we were building on branch A at slot 111, the cluster switched to branch
    /// B whose tip is 107 — and carrying the old maximum forward would leave the
    /// tracker comparing every subsequent slot against a head that is no longer
    /// on the chain, turning ordinary extensions into phantom forks.
    ///
    /// Taking the maximum of the canonical set is valid because a slot's parent
    /// always precedes it, so the chain's tip is its highest member.
    fn refresh_head(&mut self) {
        self.head = self.canonical.iter().next_back().copied();
    }

    /// Update a slot's status.
    pub fn update_status(&mut self, slot: u64, status: SlotStatus) {
        if let Some(info) = self.slots.get_mut(&slot) {
            info.status = status;
            if status == SlotStatus::Rooted {
                self.mark_rooted(slot);
            }
        }
    }

    fn mark_rooted(&mut self, slot: u64) {
        self.highest_rooted = Some(self.highest_rooted.map_or(slot, |h| h.max(slot)));
    }

    /// Mark slots as orphaned during rollback.
    pub fn mark_orphaned(&mut self, slots: &[u64]) {
        for &slot in slots {
            self.canonical.remove(&slot);
            if let Some(info) = self.slots.get_mut(&slot) {
                info.status = SlotStatus::Orphaned;
            }
        }
        self.refresh_head();
    }

    /// Lowest slot number we are still willing to reason about.
    fn retention_floor(&self) -> Option<u64> {
        self.head
            .map(|head| head.saturating_sub(self.max_slots as u64))
    }

    /// Drop slots that have fallen out of the retention window.
    ///
    /// Bounded by distance from the head, so a stream that never reports rooted
    /// slots stays bounded just the same.
    fn prune(&mut self) {
        let Some(floor) = self.retention_floor() else {
            return;
        };
        if self.slots.len() <= self.max_slots {
            return;
        }
        let before = self.slots.len();
        self.slots.retain(|&slot, _| slot >= floor);
        self.canonical.retain(|&slot| slot >= floor);
        self.stats.slots_pruned += (before - self.slots.len()) as u64;
        self.refresh_head();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(tracker: &mut SlotChainTracker, links: &[(u64, u64)]) {
        for &(slot, parent) in links {
            tracker
                .process_slot(SlotInfo::new(slot, parent))
                .expect("no rooted slot involved");
        }
    }

    #[test]
    fn test_slot_info_parent_matching() {
        let info = SlotInfo::new(100, 99);
        assert!(info.parent_matches(99));
        assert!(!info.parent_matches(98));
    }

    #[test]
    fn test_slot_status_properties() {
        assert!(SlotStatus::Processing.is_revocable());
        assert!(SlotStatus::Confirmed.is_revocable());
        assert!(!SlotStatus::Rooted.is_revocable());
        assert!(SlotStatus::Rooted.is_final());
    }

    #[test]
    fn extending_the_head_is_not_a_reorg() {
        let mut tracker = SlotChainTracker::new(1000);
        assert!(matches!(
            tracker.process_slot(SlotInfo::new(100, 99)).unwrap(),
            ChainUpdate::Initialised { slot: 100 }
        ));
        assert!(matches!(
            tracker.process_slot(SlotInfo::new(101, 100)).unwrap(),
            ChainUpdate::Extended { slot: 101 }
        ));
        assert_eq!(tracker.head(), Some(101));
        assert_eq!(tracker.stats().reorgs, 0);
    }

    #[test]
    fn retransmitting_the_same_block_is_a_duplicate() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(&mut tracker, &[(100, 99), (101, 100)]);
        assert!(matches!(
            tracker.process_slot(SlotInfo::new(101, 100)).unwrap(),
            ChainUpdate::Duplicate { slot: 101 }
        ));
        assert_eq!(tracker.canonical_chain(), vec![100, 101]);
    }

    /// The shape the previous implementation could not see: a brand new, higher
    /// slot that names a parent other than the head.
    #[test]
    fn new_slot_naming_an_older_parent_is_a_reorg() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(
            &mut tracker,
            &[(100, 99), (101, 100), (102, 101), (103, 102)],
        );

        let update = tracker.process_slot(SlotInfo::new(104, 101)).unwrap();
        let fork = update.fork().expect("should be a reorg");

        assert_eq!(fork.fork_slot, 104);
        assert_eq!(fork.expected_parent, 103);
        assert_eq!(fork.actual_parent, 101);
        assert_eq!(fork.divergence_point, 101);
        assert_eq!(fork.slots_to_rollback, vec![103, 102]);
        assert!(!fork.divergence_is_bound);
        assert_eq!(tracker.canonical_chain(), vec![100, 101, 104]);
    }

    #[test]
    fn competing_block_at_the_same_slot_is_a_reorg() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(&mut tracker, &[(100, 99), (101, 100), (102, 101)]);

        // A different block at slot 102 that builds on 100 instead of 101.
        let update = tracker.process_slot(SlotInfo::new(102, 100)).unwrap();
        let fork = update.fork().expect("should be a reorg");

        assert_eq!(fork.divergence_point, 100);
        assert_eq!(fork.slots_to_rollback, vec![102, 101]);
        assert_eq!(tracker.canonical_chain(), vec![100, 102]);
    }

    /// A branch several slots long that rejoins well below the head. The walk has
    /// to follow the branch down, not just compare two slot numbers.
    #[test]
    fn divergence_walks_a_multi_slot_branch_back_to_the_ancestor() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(
            &mut tracker,
            &[(100, 99), (101, 100), (102, 101), (103, 102), (104, 103)],
        );

        // Build a side branch off 100 without adopting it yet: 201 <- 202 <- 203.
        // Each of these is itself a reorg as it arrives; the last one leaves the
        // canonical chain on the new branch.
        tracker.process_slot(SlotInfo::new(201, 100)).unwrap();
        tracker.process_slot(SlotInfo::new(202, 201)).unwrap();
        let update = tracker.process_slot(SlotInfo::new(203, 202)).unwrap();

        // 203 extends 202, which is already canonical by now.
        assert!(matches!(update, ChainUpdate::Extended { slot: 203 }));
        assert_eq!(tracker.canonical_chain(), vec![100, 201, 202, 203]);
        assert!(!tracker.is_canonical(101));
        assert!(!tracker.is_canonical(104));
    }

    /// When the branch arrives head-first, the walk has to reach through slots we
    /// have never seen and still roll back the right amount.
    #[test]
    fn unknown_parent_yields_a_bounded_divergence() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(
            &mut tracker,
            &[(100, 99), (101, 100), (102, 101), (103, 102)],
        );

        // Parent 150 is a slot we have never seen.
        let update = tracker.process_slot(SlotInfo::new(151, 150)).unwrap();
        let fork = update.fork().expect("should be a reorg");

        assert!(fork.divergence_is_bound);
        assert_eq!(fork.divergence_point, 150);
        // Nothing canonical sits above 150, so nothing is orphaned.
        assert!(fork.slots_to_rollback.is_empty());
    }

    #[test]
    fn unknown_parent_below_the_head_rolls_back_above_it() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(
            &mut tracker,
            &[(100, 99), (101, 100), (102, 101), (103, 102)],
        );

        // Parent 101 is known and canonical, but reached via an unknown slot.
        let update = tracker.process_slot(SlotInfo::new(120, 110)).unwrap();
        let fork = update.fork().expect("should be a reorg");

        assert!(fork.divergence_is_bound);
        assert_eq!(fork.divergence_point, 110);
        assert_eq!(fork.slots_to_rollback, Vec::<u64>::new());
    }

    #[test]
    fn deep_reorg_rolls_back_every_orphaned_slot() {
        let mut tracker = SlotChainTracker::new(1000);
        let links: Vec<(u64, u64)> = (101..=132).map(|s| (s, s - 1)).collect();
        chain(&mut tracker, &[(100, 99)]);
        chain(&mut tracker, &links);
        assert_eq!(tracker.head(), Some(132));

        let update = tracker.process_slot(SlotInfo::new(133, 100)).unwrap();
        let fork = update.fork().expect("should be a reorg");

        assert_eq!(fork.divergence_point, 100);
        assert_eq!(fork.rollback_depth(), 32);
        assert_eq!(fork.slots_to_rollback.first(), Some(&132));
        assert_eq!(fork.slots_to_rollback.last(), Some(&101));
        assert_eq!(tracker.canonical_chain(), vec![100, 133]);
    }

    #[test]
    fn rollback_set_is_ordered_deepest_last() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(&mut tracker, &[(10, 9), (11, 10), (12, 11), (13, 12)]);
        let update = tracker.process_slot(SlotInfo::new(14, 10)).unwrap();
        let fork = update.fork().unwrap();
        assert_eq!(fork.slots_to_rollback, vec![13, 12, 11]);
    }

    /// A reorg can move the head to a *lower* slot: we were on a branch that had
    /// reached 111, the cluster switched to one whose tip is 105. Keeping the
    /// old maximum as the head made every later slot look like a fork.
    #[test]
    fn a_reorg_can_move_the_head_backwards() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(
            &mut tracker,
            &[(100, 99), (101, 100), (102, 101), (103, 102)],
        );

        // Jump to a branch off 101 that reaches 111.
        tracker.process_slot(SlotInfo::new(110, 101)).unwrap();
        tracker.process_slot(SlotInfo::new(111, 110)).unwrap();
        assert_eq!(tracker.head(), Some(111));

        // Now the cluster comes back to the original branch at 102.
        let update = tracker.process_slot(SlotInfo::new(102, 101)).unwrap();
        assert!(update.is_reorg(), "returning to 102 is a branch switch");
        assert_eq!(
            tracker.head(),
            Some(102),
            "the head must follow the canonical tip downwards"
        );
        assert_eq!(tracker.canonical_chain(), vec![100, 101, 102]);

        // And the next slot must be an ordinary extension, not another fork.
        let update = tracker.process_slot(SlotInfo::new(103, 102)).unwrap();
        assert!(
            matches!(update, ChainUpdate::Extended { slot: 103 }),
            "expected an extension, got {update:?}"
        );
    }

    #[test]
    fn reorg_that_would_revert_a_rooted_slot_is_refused() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(&mut tracker, &[(100, 99), (101, 100), (102, 101)]);
        tracker.update_status(101, SlotStatus::Rooted);

        let err = tracker.process_slot(SlotInfo::new(103, 100)).unwrap_err();
        assert!(
            matches!(err, crate::Error::RollbackFailed { slot: 101, .. }),
            "unexpected error: {err}"
        );
        // The chain must be left untouched when we refuse.
        assert!(tracker.is_canonical(101));
        assert!(tracker.is_canonical(102));
    }

    #[test]
    fn rooted_status_survives_a_retransmit() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(&mut tracker, &[(100, 99), (101, 100)]);
        tracker.update_status(101, SlotStatus::Rooted);
        tracker.process_slot(SlotInfo::new(101, 100)).unwrap();
        assert_eq!(tracker.get(101).unwrap().status, SlotStatus::Rooted);
    }

    #[test]
    fn memory_stays_bounded_without_any_rooted_slots() {
        let mut tracker = SlotChainTracker::new(64);
        for slot in 1..=5_000u64 {
            tracker
                .process_slot(SlotInfo::new(slot, slot.saturating_sub(1)))
                .unwrap();
        }
        assert!(tracker.highest_rooted().is_none());
        assert!(
            tracker.stats().slots_tracked <= 128,
            "tracked {} slots",
            tracker.stats().slots_tracked
        );
        assert!(tracker.stats().slots_pruned > 4_000);
    }

    #[test]
    fn slots_below_the_retention_window_are_ignored_not_guessed() {
        let mut tracker = SlotChainTracker::new(16);
        for slot in 1..=200u64 {
            tracker
                .process_slot(SlotInfo::new(slot, slot.saturating_sub(1)))
                .unwrap();
        }
        let update = tracker.process_slot(SlotInfo::new(5, 4)).unwrap();
        assert!(matches!(
            update,
            ChainUpdate::Ignored {
                slot: 5,
                reason: IgnoreReason::BelowRetentionWindow
            }
        ));
    }

    #[test]
    fn parent_cycle_terminates() {
        let mut tracker = SlotChainTracker::new(1000);
        chain(&mut tracker, &[(100, 99), (101, 100)]);
        // Inject a cycle directly: 50 -> 51 -> 50.
        tracker.slots.insert(50, SlotInfo::new(50, 51));
        tracker.slots.insert(51, SlotInfo::new(51, 50));
        // Must return rather than spin.
        let update = tracker.process_slot(SlotInfo::new(102, 50)).unwrap();
        assert!(update.is_reorg());
    }
}
