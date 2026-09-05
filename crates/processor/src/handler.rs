//! Event handlers for processing different event types.

use async_trait::async_trait;
use slot_stream_common::{EventKind, IndexedEvent, Result};
use std::sync::Arc;

/// Trait for event handlers.
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Get the name of this handler.
    fn name(&self) -> &str;

    /// Get the event kinds this handler processes.
    fn handles(&self) -> Vec<EventKind>;

    /// Process an event.
    async fn handle(&self, event: &IndexedEvent) -> Result<()>;
}

/// Registry of event handlers.
pub struct HandlerRegistry {
    handlers: Vec<Arc<dyn EventHandler>>,
}

impl HandlerRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            handlers: Vec::new(),
        }
    }

    /// Register a handler.
    pub fn register<H: EventHandler + 'static>(&mut self, handler: H) {
        self.handlers.push(Arc::new(handler));
    }

    /// Handle an event by routing to all applicable handlers.
    pub async fn handle(&self, event: &IndexedEvent) -> Result<()> {
        for handler in &self.handlers {
            if handler.handles().contains(&event.kind) || handler.handles().is_empty() {
                handler.handle(event).await?;
            }
        }
        Ok(())
    }

    /// Get the number of registered handlers.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Check if registry is empty.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// A logging handler for debugging.
pub struct LoggingHandler {
    name: String,
}

impl LoggingHandler {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[async_trait]
impl EventHandler for LoggingHandler {
    fn name(&self) -> &str {
        &self.name
    }

    fn handles(&self) -> Vec<EventKind> {
        vec![] // Handles all events
    }

    async fn handle(&self, event: &IndexedEvent) -> Result<()> {
        tracing::debug!(
            handler = self.name,
            slot = event.slot,
            kind = ?event.kind,
            "Processing event"
        );
        Ok(())
    }
}

/// A metrics handler that records event statistics.
pub struct MetricsHandler;

#[async_trait]
impl EventHandler for MetricsHandler {
    fn name(&self) -> &str {
        "metrics"
    }

    fn handles(&self) -> Vec<EventKind> {
        vec![] // All events
    }

    async fn handle(&self, event: &IndexedEvent) -> Result<()> {
        metrics::counter!("processor.events", "kind" => event.kind.as_str()).increment(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHandler {
        name: String,
        kinds: Vec<EventKind>,
    }

    #[async_trait]
    impl EventHandler for TestHandler {
        fn name(&self) -> &str {
            &self.name
        }

        fn handles(&self) -> Vec<EventKind> {
            self.kinds.clone()
        }

        async fn handle(&self, _event: &IndexedEvent) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_handler_registry() {
        let mut registry = HandlerRegistry::new();

        registry.register(TestHandler {
            name: "test1".into(),
            kinds: vec![EventKind::Transaction],
        });

        registry.register(TestHandler {
            name: "test2".into(),
            kinds: vec![EventKind::AccountUpdate],
        });

        assert_eq!(registry.len(), 2);
    }
}
