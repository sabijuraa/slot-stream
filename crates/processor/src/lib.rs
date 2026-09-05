//! # slot-stream-processor
//!
//! The pipeline core: it stamps ordering onto events, tracks the shape of the
//! chain, and turns a fork into a rollback the persister can execute.
//!
//! ## The ordering that makes reorgs survivable
//!
//! Everything the processor emits goes down one channel, in the order it decided
//! on. For a fork that order is: the events of the abandoned branch, then the
//! rollback that invalidates them, then the events of the branch that replaced
//! it. The persister applies commands in that order and flushes before every
//! rollback, so the database passes through the same states the processor saw.
//!
//! Splitting rollbacks onto their own channel would break this, because two
//! channels have no order relative to each other and the rollback could arrive
//! after the replacement events and invalidate them.
//!
//! ## Sequence numbers
//!
//! The stream's own sequence describes the validator's view and is not
//! monotonic across reconnects or forks. The processor stamps its own instead,
//! resumed from the database high-water mark on startup, and that is the order
//! readers see. Row identity stays keyed on the stream's sequence so replays
//! stay idempotent — see the persister.

pub mod handler;
pub mod state;

use slot_stream_common::{
    ChainUpdate, Error, EventKind, IndexedEvent, RawEvent, Result, RollbackPlan, SequenceAssigner,
    SequenceNumber, SlotChainTracker, SlotInfo,
};
use slot_stream_dlq::DeadLetterQueue;
use slot_stream_persister::PersistCommand;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, warn};

pub use handler::{EventHandler, HandlerRegistry, LoggingHandler, MetricsHandler};
pub use state::{ProcessorState, ProcessorStats};

/// How the processor is configured.
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    /// How many slots of chain history to retain for fork detection.
    ///
    /// This bounds both memory and how deep a reorg we can resolve exactly.
    pub chain_window: usize,

    /// Refuse to act on a rollback deeper than this, and surface it instead.
    /// A reorg of hundreds of slots is a symptom, not a routine event.
    pub max_rollback_depth: usize,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            chain_window: 4_096,
            max_rollback_depth: 512,
        }
    }
}

/// Processes stream events into ordered, fork-aware database commands.
pub struct Processor {
    chain: parking_lot::Mutex<SlotChainTracker>,
    assigner: Arc<SequenceAssigner>,
    handlers: HandlerRegistry,
    dlq: Arc<DeadLetterQueue>,
    state: Arc<parking_lot::RwLock<ProcessorState>>,
    output: mpsc::Sender<PersistCommand>,
    config: ProcessorConfig,
}

impl Processor {
    /// Create a processor that emits into `output`.
    pub fn new(
        dlq: Arc<DeadLetterQueue>,
        output: mpsc::Sender<PersistCommand>,
        config: ProcessorConfig,
    ) -> Self {
        Self {
            chain: parking_lot::Mutex::new(SlotChainTracker::new(config.chain_window)),
            assigner: Arc::new(SequenceAssigner::new()),
            handlers: HandlerRegistry::new(),
            dlq,
            state: Arc::new(parking_lot::RwLock::new(ProcessorState::new())),
            output,
            config,
        }
    }

    /// Create a processor resuming from persisted state after a restart.
    ///
    /// Both halves matter. Resuming the assigner keeps new sequences above
    /// everything committed. Rebuilding the chain keeps fork detection working
    /// across the restart — a processor that starts blind treats the next slot as
    /// the beginning of a fresh chain and silently misses a reorg spanning it.
    pub fn resuming(
        dlq: Arc<DeadLetterQueue>,
        output: mpsc::Sender<PersistCommand>,
        config: ProcessorConfig,
        max_seq: SequenceNumber,
        canonical_slots: &[(u64, u64)],
    ) -> Self {
        let mut chain = SlotChainTracker::new(config.chain_window);
        for &(slot, parent) in canonical_slots {
            // Ascending order, so each is an extension of the one before.
            let _ = chain.process_slot(SlotInfo::new(slot, parent));
        }

        info!(
            resumed_seq = max_seq.0,
            canonical_slots = canonical_slots.len(),
            head = ?chain.head(),
            "processor resumed"
        );

        Self {
            chain: parking_lot::Mutex::new(chain),
            assigner: Arc::new(SequenceAssigner::resuming_from(max_seq)),
            handlers: HandlerRegistry::new(),
            dlq,
            state: Arc::new(parking_lot::RwLock::new(ProcessorState::new())),
            output,
            config,
        }
    }

    /// Register an event handler.
    pub fn register_handler<H: EventHandler + 'static>(&mut self, handler: H) {
        self.handlers.register(handler);
    }

    /// Current statistics.
    pub fn stats(&self) -> ProcessorStats {
        self.state.read().stats()
    }

    /// The canonical chain as the processor currently sees it.
    pub fn canonical_chain(&self) -> Vec<u64> {
        self.chain.lock().canonical_chain()
    }

    /// The head of the canonical chain.
    pub fn head(&self) -> Option<u64> {
        self.chain.lock().head()
    }

    /// Consume raw events until the channel closes.
    pub async fn run(&self, mut rx: mpsc::Receiver<RawEvent>) -> Result<()> {
        info!(
            chain_window = self.config.chain_window,
            "processor started"
        );

        while let Some(event) = rx.recv().await {
            if let Err(e) = self.process(event).await {
                if matches!(e, Error::ChannelClosed) {
                    warn!("output channel closed, stopping processor");
                    break;
                }
                // Anything else has already been routed to the DLQ or logged;
                // one bad event must not take the pipeline down.
                warn!(error = %e, "event processing failed");
                self.state.write().events_failed += 1;
            }
        }

        info!(stats = ?self.stats(), "processor stopped");
        Ok(())
    }

    /// Process a single raw event.
    #[instrument(skip(self, raw), fields(slot = raw.slot, source_seq = raw.sequence.0))]
    pub async fn process(&self, raw: RawEvent) -> Result<()> {
        let parsed = match self.parse(&raw) {
            Ok(data) => data,
            Err(e) => {
                warn!(error = %e, "parse failed, routing to DLQ");
                self.dlq
                    .enqueue(raw, &e)
                    .await
                    .map_err(|dlq_err| Error::DlqWriteFailed(dlq_err.to_string()))?;
                self.state.write().events_dlq += 1;
                return Ok(());
            }
        };

        // Fork detection runs before the event is emitted, so a rollback is
        // always queued ahead of the events of the branch that caused it.
        self.advance_chain(&raw).await?;

        let event = IndexedEvent::from_raw(raw, parsed, self.assigner.next());

        if let Err(e) = self.handlers.handle(&event).await {
            if e.should_dlq() {
                warn!(error = %e, "handler failed, routing to DLQ");
                self.dlq
                    .enqueue_indexed(event, &e)
                    .await
                    .map_err(|dlq_err| Error::DlqWriteFailed(dlq_err.to_string()))?;
                self.state.write().events_dlq += 1;
                return Ok(());
            }
            return Err(e);
        }

        let slot = event.slot;
        let seq = event.seq;
        self.emit(PersistCommand::event(event)).await?;

        let mut state = self.state.write();
        state.events_processed += 1;
        state.last_slot = Some(slot);
        state.last_seq = Some(seq);
        Ok(())
    }

    /// Feed the slot into the chain tracker and emit whatever it implies.
    async fn advance_chain(&self, raw: &RawEvent) -> Result<()> {
        // Without a parent there is nothing to check the chain against. Treating
        // `slot - 1` as the parent would manufacture a chain shape the stream
        // never claimed and produce phantom forks, so we leave the chain alone.
        let Some(parent) = raw.parent_slot else {
            return Ok(());
        };

        let info = SlotInfo::new(raw.slot, parent);
        let update = {
            let mut chain = self.chain.lock();
            chain.process_slot(info.clone())?
        };

        match update {
            ChainUpdate::Duplicate { .. } | ChainUpdate::Ignored { .. } => return Ok(()),
            ChainUpdate::Initialised { slot } | ChainUpdate::Extended { slot } => {
                debug!(slot, "chain extended");
                self.emit(PersistCommand::slot(info)).await?;
            }
            ChainUpdate::Reorg(fork) => {
                let plan = RollbackPlan::from_fork(&fork);

                if plan.depth() > self.config.max_rollback_depth {
                    // Refusing is the honest response: a rollback this deep means
                    // our view and the cluster's have diverged further than the
                    // window we retain can explain, and guessing would corrupt
                    // state that a human could still repair.
                    return Err(Error::RollbackFailed {
                        slot: plan.fork_slot,
                        reason: format!(
                            "rollback depth {} exceeds the configured maximum {}; \
                             refusing to act on it automatically",
                            plan.depth(),
                            self.config.max_rollback_depth
                        ),
                    });
                }

                warn!(
                    fork_slot = fork.fork_slot,
                    divergence_point = fork.divergence_point,
                    depth = plan.depth(),
                    bounded = fork.divergence_is_bound,
                    "reorg detected"
                );

                {
                    let mut state = self.state.write();
                    state.reorgs_detected += 1;
                    state.slots_rolled_back += plan.depth() as u64;
                    state.last_reorg_slot = Some(fork.fork_slot);
                    state.max_rollback_depth = state.max_rollback_depth.max(plan.depth());
                }
                metrics::counter!("processor.reorgs").increment(1);
                metrics::histogram!("processor.rollback_depth").record(plan.depth() as f64);

                // Order is the whole point: rollback first, then the slot that
                // replaced the orphaned branch.
                self.emit(PersistCommand::rollback(plan)).await?;
                self.emit(PersistCommand::slot(info)).await?;
            }
        }

        Ok(())
    }

    async fn emit(&self, command: PersistCommand) -> Result<()> {
        self.output
            .send(command)
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Turn a raw payload into the JSON we store.
    fn parse(&self, raw: &RawEvent) -> Result<serde_json::Value> {
        if raw.payload.is_empty() {
            return Err(Error::EventParse(format!(
                "empty payload for {:?} at slot {}",
                raw.kind, raw.slot
            )));
        }

        // Payloads reach us already encoded as JSON by the stream source. Kinds
        // that carry structure get it preserved; the rest keep the raw bytes so
        // nothing is lost on the way to the database.
        let body: serde_json::Value = match serde_json::from_slice(&raw.payload) {
            Ok(value) => value,
            Err(e) => {
                return Err(Error::EventParse(format!(
                    "payload is not valid JSON: {e}"
                )))
            }
        };

        Ok(serde_json::json!({
            "kind": raw.kind.as_str(),
            "slot": raw.slot,
            "parent_slot": raw.parent_slot,
            "source_seq": raw.sequence.0,
            "body": body,
        }))
    }

    /// Kinds this processor understands. Used by the ingester's filter.
    pub fn supported_kinds() -> &'static [EventKind] {
        &[
            EventKind::SlotUpdate,
            EventKind::AccountUpdate,
            EventKind::Transaction,
            EventKind::BlockMeta,
            EventKind::Entry,
        ]
    }
}
