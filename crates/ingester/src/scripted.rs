//! A deterministic chain source.
//!
//! Describe the exact shape of a chain — including the forks — and this emits it
//! as a stream. It exists so reorg behaviour can be exercised against the real
//! pipeline with a chain whose correct outcome is known in advance.
//!
//! The script is a flat list of slots in arrival order. A slot naming a parent
//! other than the previous slot is a fork, which is all a reorg actually is.

use crate::source::EventSource;
use async_trait::async_trait;
use slot_stream_common::{EventKind, RawEvent, Result, SequenceNumber};
use std::collections::HashMap;

/// One event within a scripted slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptedEvent {
    /// Event kind.
    pub kind: EventKind,
    /// JSON body, stored as the event payload.
    pub body: serde_json::Value,
}

impl ScriptedEvent {
    /// A transaction-shaped event carrying an identifying label.
    pub fn transaction(label: impl Into<String>) -> Self {
        Self {
            kind: EventKind::Transaction,
            body: serde_json::json!({ "label": label.into() }),
        }
    }
}

/// One slot in the script, with the events it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptedSlot {
    /// Slot number.
    pub slot: u64,
    /// The slot this one builds on.
    pub parent: u64,
    /// Events emitted for this slot, in order.
    pub events: Vec<ScriptedEvent>,
}

impl ScriptedSlot {
    /// A slot carrying `count` labelled transactions.
    pub fn with_events(slot: u64, parent: u64, count: usize) -> Self {
        Self {
            slot,
            parent,
            events: (0..count)
                .map(|i| ScriptedEvent::transaction(format!("slot{slot}-ev{i}")))
                .collect(),
        }
    }
}

/// A chain to emit, in arrival order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainScript {
    /// Slots in the order the source will emit them.
    pub slots: Vec<ScriptedSlot>,
}

impl ChainScript {
    /// An empty script.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a run of slots that each extend the previous one.
    ///
    /// Starts at `from` with parent `from - 1` and adds `count` slots.
    pub fn extend_from(mut self, from: u64, count: u64, events_per_slot: usize) -> Self {
        for offset in 0..count {
            let slot = from + offset;
            self.slots
                .push(ScriptedSlot::with_events(slot, slot - 1, events_per_slot));
        }
        self
    }

    /// Append a single slot naming an explicit parent.
    ///
    /// When that parent is not the previous slot, this is the fork.
    pub fn fork(mut self, slot: u64, parent: u64, events_per_slot: usize) -> Self {
        self.slots
            .push(ScriptedSlot::with_events(slot, parent, events_per_slot));
        self
    }

    /// Append a run building on a slot other than the current tip.
    pub fn fork_run(
        mut self,
        first_slot: u64,
        parent: u64,
        count: u64,
        events_per_slot: usize,
    ) -> Self {
        for offset in 0..count {
            let slot = first_slot + offset;
            let slot_parent = if offset == 0 { parent } else { slot - 1 };
            self.slots
                .push(ScriptedSlot::with_events(slot, slot_parent, events_per_slot));
        }
        self
    }

    /// Total events the script will emit.
    pub fn event_count(&self) -> usize {
        self.slots.iter().map(|s| s.events.len()).sum()
    }

    /// The canonical chain this script ends on, ascending.
    ///
    /// Computed independently of the pipeline: build the parent map in arrival
    /// order, take the last slot emitted as the head, and walk its ancestry. This
    /// is the yardstick the reorg proof measures persisted state against, so it
    /// deliberately shares no code with the fork detector.
    pub fn canonical_chain(&self) -> Vec<u64> {
        let mut parents: HashMap<u64, u64> = HashMap::new();
        for slot in &self.slots {
            parents.insert(slot.slot, slot.parent);
        }

        let Some(head) = self.slots.last().map(|s| s.slot) else {
            return Vec::new();
        };

        let mut chain = vec![head];
        let mut cursor = head;
        // Bounded by the number of distinct slots, so a malformed script with a
        // parent cycle terminates instead of hanging the test.
        for _ in 0..parents.len() {
            match parents.get(&cursor) {
                Some(&parent) => {
                    chain.push(parent);
                    cursor = parent;
                }
                None => break,
            }
        }

        chain.reverse();
        chain
    }

    /// The events a from-scratch replay of the canonical chain would leave
    /// behind, as `(slot, label)` pairs in slot order.
    ///
    /// This is the expected database state after the script has been consumed.
    /// Where a slot number appears more than once in the script — competing
    /// blocks — the last emission wins, since that is the one the canonical head
    /// descends from.
    pub fn canonical_events(&self) -> Vec<(u64, String)> {
        let canonical: std::collections::HashSet<u64> =
            self.canonical_chain().into_iter().collect();

        let mut latest: HashMap<u64, &ScriptedSlot> = HashMap::new();
        for slot in &self.slots {
            latest.insert(slot.slot, slot);
        }

        let mut slots: Vec<&ScriptedSlot> = latest
            .into_values()
            .filter(|s| canonical.contains(&s.slot))
            .collect();
        slots.sort_by_key(|s| s.slot);

        slots
            .iter()
            .flat_map(|s| {
                s.events.iter().map(move |e| {
                    let label = e
                        .body
                        .get("label")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    (s.slot, label)
                })
            })
            .collect()
    }
}

/// Emits a [`ChainScript`] as a stream, assigning source sequence numbers.
pub struct ScriptedSource {
    script: ChainScript,
    slot_index: usize,
    event_index: usize,
    next_sequence: u64,
    /// Delay between events, for exercising the pipeline under a paced load.
    pace: Option<std::time::Duration>,
}

impl ScriptedSource {
    /// Create a source that will emit `script`.
    pub fn new(script: ChainScript) -> Self {
        Self {
            script,
            slot_index: 0,
            event_index: 0,
            next_sequence: 1,
            pace: None,
        }
    }

    /// Emit at a fixed interval rather than as fast as possible.
    pub fn paced(mut self, interval: std::time::Duration) -> Self {
        self.pace = Some(interval);
        self
    }

    /// Start sequence numbering after `sequence`, as a resuming source would.
    pub fn resuming_from(mut self, sequence: u64) -> Self {
        self.next_sequence = sequence + 1;
        self
    }

    /// The script being emitted.
    pub fn script(&self) -> &ChainScript {
        &self.script
    }
}

#[async_trait]
impl EventSource for ScriptedSource {
    async fn next_event(&mut self) -> Option<Result<RawEvent>> {
        loop {
            let slot = self.script.slots.get(self.slot_index)?;

            match slot.events.get(self.event_index) {
                Some(event) => {
                    self.event_index += 1;

                    if let Some(pace) = self.pace {
                        tokio::time::sleep(pace).await;
                    }

                    let payload = match serde_json::to_vec(&event.body) {
                        Ok(bytes) => bytes::Bytes::from(bytes),
                        Err(e) => {
                            return Some(Err(slot_stream_common::Error::Serialization(
                                e.to_string(),
                            )))
                        }
                    };

                    let sequence = SequenceNumber(self.next_sequence);
                    self.next_sequence += 1;

                    return Some(Ok(RawEvent::new(sequence, event.kind, slot.slot, payload)
                        .with_parent(slot.parent)));
                }
                None => {
                    self.slot_index += 1;
                    self.event_index = 0;
                }
            }
        }
    }

    fn describe(&self) -> String {
        format!(
            "scripted({} slots, {} events)",
            self.script.slots.len(),
            self.script.event_count()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_straight_chain_is_entirely_canonical() {
        let script = ChainScript::new().extend_from(100, 5, 2);
        assert_eq!(script.canonical_chain(), vec![99, 100, 101, 102, 103, 104]);
        assert_eq!(script.canonical_events().len(), 10);
    }

    #[test]
    fn a_fork_drops_the_orphaned_slots_from_the_expected_state() {
        // 100..104, then 105 builds on 101, orphaning 102, 103, 104.
        let script = ChainScript::new()
            .extend_from(100, 5, 1)
            .fork(105, 101, 1);

        assert_eq!(script.canonical_chain(), vec![99, 100, 101, 105]);

        let expected = script.canonical_events();
        let slots: Vec<u64> = expected.iter().map(|(s, _)| *s).collect();
        assert_eq!(slots, vec![100, 101, 105]);
    }

    #[test]
    fn a_competing_block_at_the_same_slot_replaces_the_first() {
        let script = ChainScript::new()
            .extend_from(100, 3, 1) // 100, 101, 102
            .fork(102, 100, 1); // a different 102 building on 100

        assert_eq!(script.canonical_chain(), vec![99, 100, 102]);
        // The later emission of slot 102 is the one that survives.
        assert_eq!(script.canonical_events().len(), 2);
    }

    #[tokio::test]
    async fn the_source_emits_every_event_with_monotonic_sequences() {
        let script = ChainScript::new().extend_from(10, 3, 2);
        let mut source = ScriptedSource::new(script);

        let mut seen = Vec::new();
        while let Some(event) = source.next_event().await {
            seen.push(event.expect("scripted source does not fail"));
        }

        assert_eq!(seen.len(), 6);
        for (i, event) in seen.iter().enumerate() {
            assert_eq!(event.sequence.0, i as u64 + 1);
            assert!(event.parent_slot.is_some(), "parent must always be set");
        }
        assert_eq!(seen[0].slot, 10);
        assert_eq!(seen[5].slot, 12);
    }

    #[tokio::test]
    async fn resuming_continues_the_sequence_numbering() {
        let script = ChainScript::new().extend_from(10, 1, 1);
        let mut source = ScriptedSource::new(script).resuming_from(500);
        let event = source.next_event().await.unwrap().unwrap();
        assert_eq!(event.sequence.0, 501);
    }
}
