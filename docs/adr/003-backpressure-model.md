# ADR 003: Backpressure and Buffer Model

## Status

Accepted

## Context

The ingestion rate from a Geyser stream can spike dramatically:
- Normal: ~5,000 events/second
- Peak: ~50,000+ events/second (during high activity)

Meanwhile, downstream processing has variable latency:
- Database writes batch and flush periodically
- Handlers may invoke external services
- Reorg detection adds occasional pauses

Without backpressure management:
- Memory grows unboundedly during spikes
- OOM crashes lose in-flight events
- Stream falls behind and can't catch up

## Decision

### Bounded Channel Architecture

```
┌──────────────┐     bounded      ┌──────────────┐     bounded      ┌──────────────┐
│   Ingester   │────channel──────▶│   Processor  │────channel──────▶│  Persister   │
│              │   (100K cap)     │              │   (10K cap)      │              │
└──────────────┘                  └──────────────┘                  └──────────────┘
```

Each stage is connected by a bounded `tokio::sync::mpsc` channel.

### Overflow Policies

Three policies are supported (configurable per deployment):

#### 1. DropOldest (Default)

When buffer is full, discard the oldest event to make room for new.

```rust
match sender.try_send(event) {
    Ok(()) => Ok(()),
    Err(TrySendError::Full(_)) => {
        // Buffer full - new event takes priority
        // Oldest was already dropped by channel mechanics
        metrics::counter!("buffer.dropped").increment(1);
        Ok(())
    }
}
```

**Use case**: Real-time dashboards where latest data matters most.

#### 2. DropNewest

When buffer is full, reject the incoming event.

```rust
match sender.try_send(event) {
    Ok(()) => Ok(()),
    Err(TrySendError::Full(event)) => {
        // Discard the new event
        metrics::counter!("buffer.dropped").increment(1);
        Ok(())
    }
}
```

**Use case**: Historical completeness matters, gaps will be backfilled.

#### 3. Block

When buffer is full, wait for space (with timeout).

```rust
sender.send(event).await.map_err(|_| Error::ChannelClosed)
```

**Use case**: Guaranteed delivery, can tolerate latency.

### Buffer Sizing Guidelines

```
Buffer size = max_throughput × expected_processing_latency × safety_factor

Example:
  max_throughput = 50,000 events/sec
  processing_latency = 0.5 sec (p99)
  safety_factor = 2

  Buffer = 50,000 × 0.5 × 2 = 50,000 events
```

Recommended defaults:
- Ingester → Processor: 100,000 events
- Processor → Persister: 10,000 events

### Monitoring

Key metrics for backpressure health:

| Metric | Meaning | Alert |
|--------|---------|-------|
| `buffer.size` | Current buffer utilization | > 80% |
| `buffer.dropped` | Events dropped per overflow | > 0 |
| `buffer.wait_time` | Time spent waiting for space | p99 > 100ms |

### Adaptive Response

When backpressure is detected:

1. **Log warning** with context (slot, buffer utilization)
2. **Increment metrics** for dashboards
3. **Consider scaling** (if running multiple instances)
4. **Review downstream** for bottlenecks

## Consequences

### Positive

- **Memory bounded**: No OOM risk from traffic spikes
- **Graceful degradation**: Explicit handling of overload
- **Configurable trade-offs**: Different policies for different needs
- **Observable**: Metrics expose pressure points

### Negative

- **Data loss possible**: DropOldest/DropNewest discard events
- **Latency impact**: Block policy adds latency
- **Tuning required**: Buffer sizes need adjustment for workload

### Mitigations

- Dropped events queue backfill work (gaps detected by sequence tracker)
- Timeouts on Block policy prevent indefinite hangs
- Auto-scaling can add capacity during sustained spikes

## Alternatives Considered

### 1. Unbounded Buffers

Use unbounded channels, rely on system memory.

**Rejected because**: OOM crashes in production, unpredictable failure.

### 2. Disk-Based Spillover

Overflow to disk when memory fills.

**Rejected because**: Complexity, disk I/O latency, operational overhead.

### 3. External Queue (Kafka/Redis)

Use external message queue for buffering.

**Rejected because**: Adds latency, operational dependency, doesn't solve core problem.

## Implementation Notes

```rust
pub enum OverflowPolicy {
    DropOldest,
    DropNewest,
    Block,
}

pub struct EventBuffer {
    capacity: usize,
    policy: OverflowPolicy,
    sender: mpsc::Sender<RawEvent>,
}
```

The `mpsc::channel` already implements ring-buffer semantics for `DropOldest` when using `try_send`.

## References

- Backpressure in reactive streams: https://www.reactivemanifesto.org/glossary#Back-Pressure
- Tokio channel documentation: https://docs.rs/tokio/latest/tokio/sync/mpsc/
