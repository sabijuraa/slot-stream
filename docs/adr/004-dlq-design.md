# ADR 004: Dead Letter Queue Design

## Status

Accepted

## Context

Events can fail processing for various reasons:
- Malformed data from the stream
- Schema validation failures
- Database write errors
- Handler exceptions

Without a DLQ:
- Failed events are silently dropped
- No visibility into failure patterns
- No way to recover/replay failed events
- Debugging production issues is difficult

## Decision

### DLQ Entry Structure

```rust
pub struct DlqEntry {
    pub id: Uuid,
    pub raw_payload: Vec<u8>,      // Original bytes
    pub sequence: Option<u64>,      // For ordering context
    pub slot: Option<u64>,          // For chain context
    pub kind: Option<EventKind>,    // Event type if known
    pub error_message: String,      // What went wrong
    pub error_category: String,     // Categorization for filtering
    pub retry_count: u32,           // Attempts made
    pub max_retries: u32,           // Limit before exhausted
    pub failed_at: DateTime,        // When failure occurred
    pub status: DlqStatus,          // Current lifecycle state
}
```

### Error Categories

Events are categorized for efficient filtering and handling:

| Category | Examples | Auto-Retry | Manual Action |
|----------|----------|------------|---------------|
| `malformed` | Parse errors, invalid encoding | No | Fix producer |
| `validation` | Schema mismatch, missing fields | No | Update schema |
| `persistence` | DB timeout, constraint error | Yes | Check DB |
| `processing` | Handler exception, timeout | Maybe | Debug handler |
| `unknown` | Unexpected errors | No | Investigate |

### Lifecycle States

```
                    ┌──────────────┐
                    │   PENDING    │◀──────────────────────┐
                    └──────┬───────┘                       │
                           │                               │
                      [retry]                              │
                           │                               │
                    ┌──────▼───────┐                       │
                    │   RETRYING   │                       │
                    └──────┬───────┘                       │
                           │                               │
              ┌────────────┼────────────┐                  │
              │            │            │                  │
         [success]    [failure]    [exhausted]             │
              │            │            │                  │
              ▼            │            ▼                  │
       ┌──────────┐        │     ┌──────────┐              │
       │ RESOLVED │        │     │EXHAUSTED │              │
       └──────────┘        │     └──────────┘              │
                           │                               │
                           └───────────────────────────────┘
```

### When Events Go to DLQ

```rust
// In ingester - parse failures
match parser.parse(raw_event) {
    Ok(indexed) => process(indexed),
    Err(e) if e.should_dlq() => {
        dlq.enqueue(raw_event, &e).await?;
    }
    Err(e) => return Err(e), // Fatal, propagate
}

// In processor - handler failures
match handler.handle(&event).await {
    Ok(()) => continue,
    Err(e) if e.should_dlq() => {
        dlq.enqueue_indexed(event, &e).await?;
    }
    Err(e) if e.is_retryable() => {
        retry_queue.push(event);
    }
    Err(e) => return Err(e), // Fatal
}

// In persister - after retries exhausted
match writer.write_with_retry(&event, 3).await {
    Ok(_) => continue,
    Err(e) => {
        dlq.enqueue_indexed(event, &e).await?;
    }
}
```

### Replay Mechanism

Events can be replayed through several selectors:

```rust
pub enum ReplaySelector {
    ByIds(Vec<Uuid>),        // Specific entries
    ByCategory(String),       // All parse errors
    BySlots(Vec<u64>),       // Specific slots
    AllUnresolved,           // Everything pending
}

// Usage
dlq.replay(ReplaySelector::ByCategory("persistence")).await?;
```

### Storage Backend

Two implementations provided:

1. **MemoryStorage**: For testing
2. **PostgresStorage**: For production

```sql
CREATE TABLE dead_letter_queue (
    id UUID PRIMARY KEY,
    raw_payload BYTEA NOT NULL,
    sequence BIGINT,
    slot BIGINT,
    kind VARCHAR(50),
    error_message TEXT NOT NULL,
    error_category VARCHAR(50) NOT NULL,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    failed_at TIMESTAMPTZ NOT NULL,
    is_resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMPTZ
);

CREATE INDEX idx_dlq_unresolved ON dead_letter_queue (failed_at)
WHERE is_resolved = false;
```

## Consequences

### Positive

- **No silent failures**: Every failed event is captured
- **Visibility**: Query DLQ for failure patterns
- **Recovery**: Replay failed events after fixing issues
- **Debugging**: Full context preserved for investigation

### Negative

- **Storage growth**: Failed events accumulate
- **Operational burden**: DLQ needs monitoring and maintenance
- **Replay complexity**: Some events may be order-dependent

### Mitigations

- Auto-purge resolved entries after retention period
- Alerts on DLQ growth rate and unresolved count
- Replay in batch with ordering by slot/sequence

## Alternatives Considered

### 1. Log and Drop

Just log failed events, don't store.

**Rejected because**: No recovery path, logs get rotated.

### 2. Separate Error Stream

Publish failures to Kafka topic.

**Rejected because**: Adds operational dependency, overkill for this use case.

### 3. In-Memory Only

Keep DLQ in memory, persist on shutdown.

**Rejected because**: Lost on crash, memory pressure.

## Monitoring

| Metric | Meaning | Alert |
|--------|---------|-------|
| `dlq.enqueue_total` | Events entering DLQ | High rate |
| `dlq.unresolved_count` | Pending items | > 100 |
| `dlq.oldest_unresolved_age` | Staleness | > 1 hour |
| `dlq.by_category` | Breakdown | Spikes |

## References

- Dead Letter Queue pattern: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
