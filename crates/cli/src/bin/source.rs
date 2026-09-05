//! A gRPC server that emits a described chain.
//!
//! This is the source the compose stack and the end-to-end tests point the
//! indexer at. It speaks the real protocol over a real socket; only the chain it
//! emits is chosen rather than observed, which is what makes a reorg reproducible.

use anyhow::{Context, Result};
use slot_stream_ingester::{ChainScript, ChainSourceServer};
use std::net::SocketAddr;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr: SocketAddr = std::env::var("SOURCE_BIND")
        .unwrap_or_else(|_| "0.0.0.0:10000".into())
        .parse()
        .context("parsing SOURCE_BIND")?;

    let script = build_script();
    let pace = std::env::var("SOURCE_PACE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis);

    info!(
        %addr,
        slots = script.slots.len(),
        events = script.event_count(),
        canonical_head = ?script.canonical_chain().last(),
        "serving chain source"
    );

    let mut server = ChainSourceServer::new(script);
    if let Some(pace) = pace {
        server = server.paced(pace);
    }

    tonic::transport::Server::builder()
        .add_service(server.into_service())
        .serve(addr)
        .await
        .context("serving gRPC")?;

    Ok(())
}

/// Build the chain to serve.
///
/// By default a straight run of slots, then a reorg, so the stack demonstrates
/// the property the project exists for rather than only the happy path.
fn build_script() -> ChainScript {
    let slots: u64 = env_or("SCRIPT_SLOTS", 40);
    let events: usize = env_or("SCRIPT_EVENTS_PER_SLOT", 4);
    let reorg_depth: u64 = env_or("SCRIPT_REORG_DEPTH", 5);

    let start = 1;
    let script = ChainScript::new().extend_from(start, slots, events);

    if reorg_depth == 0 {
        return script;
    }

    // Fork back `reorg_depth` slots from the tip and build a new branch past it.
    let head = start + slots - 1;
    let fork_parent = head.saturating_sub(reorg_depth);
    script.fork_run(head + 1, fork_parent, reorg_depth + 2, events)
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
