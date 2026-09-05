# ADR 002: Sequence Number Ordering Guarantees

## Status

Accepted

## Context

The Geyser plugin assigns monotonically increasing sequence numbers to events. These numbers are crucial for:

1. **Ordering**: Events must be processed in correct order
2. **Gap detection**: Missing events must be identified for backfill
3. **Duplicate detection**: Repeated events (from reconnects) must be skipped
4. **Exactly-once semantics**: Combined with idempotent writes

Without proper sequence handling, the index could:
- Miss events (gaps)
- Process events out of order
- Duplicate events

## Decision

### Sequence Number Properties

We treat sequence numbers as having these invariants:

1. **Monotonically increasing within a stream**: `seq(n+1) > seq(n)`
2. **Globally unique per stream**: No two events share a sequence number
3. **No guaranteed starting point**: First observed sequence may not be 0

### Tracking Implementation

```rust
pub struct SequenceTracker {
    last_seen: Option<SequenceNumber>,
    gaps: Vec<SequenceRange>,
}

impl SequenceTracker {
    pub fn process(&mut self, seq: SequenceNumber) -> SequenceResult {
        match self.last_seen {
            None => {
                // First event - accept as baseline
                self.last_seen = Some(seq);
                SequenceResult::Processed
            }
            Some(last) => {
                if seq == last {
                    // Exact duplicate
                    SequenceResult::Duplicate
                } else if seq < last {
                    // Regression - possible reorg or reconnect
                    Err(SequenceRegression { current: last, received: seq })
                } else if seq == last + 1 {
                    // Perfect - next in order
                    self.last_seen = Some(seq);
                    SequenceResult::Processed
                } else {
                    // Gap detected
                    let gap = SequenceRange::new(last + 1, seq - 1);
                    self.gaps.push(gap);
                    self.last_seen = Some(seq);
                    SequenceResult::Gap { missing: gap }
                }
            }
        }
    }
}
```

### Gap Handling

When a gap is detected:

1. **Log the gap** with slot context for debugging
2. **Emit metric** for monitoring
3. **Queue for backfill** (gap range added to backfill queue)
4. **Continue processing** current event (don't block)

### Duplicate Handling

Duplicates occur after:
- Reconnection to an earlier point in the stream
- Reprocessing after checkpoint recovery

Strategy: **Skip silently**
- Log at DEBUG level
- Increment counter metric
- Do not propagate to downstream

### Regression Handling

Sequence going backwards indicates:
- Reconnection to an earlier stream position
- Possible reorg (fork with different sequence space)

Strategy: **Alert and investigate**
- Log at WARN level
- Check for concurrent reorg detection
- May need to reset tracker from checkpoint

## Consequences

### Positive

- **Gap detection**: Missing events identified immediately
- **Backfill triggering**: Gaps automatically queue backfill work
- **Duplicate safety**: Reconnects don't cause double-processing
- **Observability**: Metrics expose sequence health

### Negative

- **Memory for gaps**: Large gaps could accumulate if backfill fails
- **Complexity**: Multiple code paths for different sequence results

### Mitigations

- Limit stored gap ranges (merge adjacent, cap total count)
- Alert on excessive pending gaps
- Periodic gap reconciliation against database

## Alternatives Considered

### 1. Rely on Database Deduplication Only

Skip tracking, let UNIQUE constraint handle duplicates.

**Rejected because**: Misses gap detection, doesn't trigger backfill.

### 2. Strict In-Order Processing

Block processing until gaps are filled.

**Rejected because**: Creates unbounded backpressure, defeats real-time goal.

### 3. External Sequence Coordination

Use Redis/Kafka for sequence tracking.

**Rejected because**: Adds operational dependency, latency, and failure modes.

## References

- Exactly-once semantics in stream processing: https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
