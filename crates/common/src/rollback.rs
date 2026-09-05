//! The rollback plan produced by fork detection and executed by the persister.
//!
//! This lives in `common` rather than in the processor because both ends of the
//! pipeline need it: the processor produces one when the chain switches
//! branches, and the persister consumes it to invalidate the orphaned rows.

use crate::ForkInfo;
use serde::{Deserialize, Serialize};

/// An instruction to undo the persisted effects of a set of orphaned slots.
///
/// Ordering against surrounding writes is the whole game. A plan must be applied
/// after the events it invalidates and before the events of the branch that
/// replaces them, which is why plans and events travel down a single channel
/// rather than two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackPlan {
    /// Identifies this rollback in logs and in the `reorgs` audit table.
    pub id: uuid::Uuid,

    /// Slots to invalidate, descending.
    pub slots_to_rollback: Vec<u64>,

    /// Slots of the adopted branch that were already known, ascending.
    ///
    /// These become canonical again. If an earlier reorg had orphaned them their
    /// rows are still soft-deleted, and nothing will re-deliver those events —
    /// the branch was already stored, it is only being re-adopted. So the
    /// rollback has to restore them explicitly, or a chain that forks away and
    /// later forks back leaves a hole where those slots should be.
    pub slots_to_restore: Vec<u64>,

    /// The common ancestor the chain fell back to.
    pub divergence_point: u64,

    /// The slot whose arrival revealed the fork.
    pub fork_slot: u64,

    /// The head we had been building on.
    pub expected_parent: u64,

    /// The parent the incoming slot named.
    pub actual_parent: u64,

    /// True when the divergence point is a lower bound rather than an observed
    /// common ancestor.
    pub divergence_is_bound: bool,

    /// When the plan was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl RollbackPlan {
    /// Derive a plan from a detected fork.
    pub fn from_fork(fork: &ForkInfo) -> Self {
        let mut slots = fork.slots_to_rollback.clone();
        slots.sort_unstable_by(|a, b| b.cmp(a));

        let mut restore = fork.new_branch.clone();
        restore.sort_unstable();

        Self {
            id: uuid::Uuid::new_v4(),
            slots_to_rollback: slots,
            slots_to_restore: restore,
            divergence_point: fork.divergence_point,
            fork_slot: fork.fork_slot,
            expected_parent: fork.expected_parent,
            actual_parent: fork.actual_parent,
            divergence_is_bound: fork.divergence_is_bound,
            created_at: fork.detected_at,
        }
    }

    /// Number of slots this plan invalidates.
    pub fn depth(&self) -> usize {
        self.slots_to_rollback.len()
    }

    /// True when the plan changes nothing.
    pub fn is_empty(&self) -> bool {
        self.slots_to_rollback.is_empty() && self.slots_to_restore.is_empty()
    }

    /// Highest slot this plan touches.
    pub fn highest_slot(&self) -> Option<u64> {
        self.slots_to_rollback.first().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_from_fork_is_ordered_highest_first() {
        let fork = ForkInfo::new(105, 104, 100, 100).with_rollback_slots(vec![102, 104, 101, 103]);
        let plan = RollbackPlan::from_fork(&fork);

        assert_eq!(plan.slots_to_rollback, vec![104, 103, 102, 101]);
        assert_eq!(plan.depth(), 4);
        assert_eq!(plan.highest_slot(), Some(104));
        assert_eq!(plan.divergence_point, 100);
        assert!(!plan.is_empty());
    }

    #[test]
    fn a_fork_that_orphans_nothing_yields_an_empty_plan() {
        let fork = ForkInfo::new(151, 103, 150, 150);
        let plan = RollbackPlan::from_fork(&fork);
        assert!(plan.is_empty());
        assert_eq!(plan.highest_slot(), None);
    }

    #[test]
    fn the_adopted_branch_is_carried_as_slots_to_restore() {
        let fork = ForkInfo::new(1_025, 1_018, 1_003, 1_001)
            .with_rollback_slots(vec![1_018, 1_017, 1_016, 1_015])
            .with_new_branch(vec![1_003, 1_002]);
        let plan = RollbackPlan::from_fork(&fork);

        assert_eq!(plan.slots_to_restore, vec![1_002, 1_003]);
        assert_eq!(plan.slots_to_rollback, vec![1_018, 1_017, 1_016, 1_015]);
        assert!(!plan.is_empty());
    }
}
