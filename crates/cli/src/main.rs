//! The slot-stream indexer.
//!
//! Wires the pipeline, serves the read API, and shuts down cleanly.

use anyhow::{Context, Result};
use slot_stream_api::{ApiState, EventStore};
use slot_stream_common::PipelineConfig;
use slot_stream_ingester::{ChainScript, GrpcEventSource, ScriptedSource};
use slot_stream_pipeline::{self as pipeline, Pipeline};
use std::net::SocketAddr;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    init_tracing(&config);

    let metrics_handle = init_metrics(&config)?;

    info!(
        database = %pipeline::redact(&config.database.url),
        source = %config.grpc.endpoint,
        "starting slot-stream"
    );

    let (pipeline, mut done_rx) = Pipeline::build(&config)
        .await
        .context("building the pipeline")?;

    // The API reads the same database the pipeline writes, through its own pool
    // handle. It is intentionally decoupled: a slow query cannot stall ingestion.
    let api_task = if config.api.enabled {
        let store = EventStore::new(
            pipeline.pool.clone(),
            config.api.max_page_size,
            config.api.default_page_size,
        );
        let mut state = ApiState::new(store);
        if let Some(handle) = metrics_handle {
            state = state.with_metrics(handle);
        }

        let addr: SocketAddr = format!("{}:{}", config.api.bind_address, config.api.port)
            .parse()
            .context("parsing the API bind address")?;

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding the API to {addr}"))?;
        info!(%addr, "read API listening");

        Some(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, slot_stream_api::router(state)).await {
                error!(error = %e, "API server stopped");
            }
        }))
    } else {
        info!("read API disabled by configuration");
        None
    };

    let resume_from = pipeline.resume.resume_source_seq().0;

    let ingest = async {
        match source_mode() {
            SourceMode::Grpc => {
                let source =
                    GrpcEventSource::connect_from(&config.grpc.endpoint, resume_from, Vec::new())
                        .await
                        .with_context(|| {
                            format!("connecting to source {}", config.grpc.endpoint)
                        })?;
                pipeline.ingest(source).await
            }
            SourceMode::Scripted(script) => {
                warn!("running against the built-in scripted source; this is for demos only");
                pipeline
                    .ingest(ScriptedSource::new(*script).resuming_from(resume_from))
                    .await
            }
        }
    };

    tokio::select! {
        result = ingest => {
            match result {
                Ok(()) => info!("source exhausted"),
                Err(e) => error!(error = ?e, "ingestion failed"),
            }
        }
        _ = shutdown_signal() => {
            info!("shutdown signal received");
        }
        _ = done_rx.recv() => {
            warn!("a pipeline stage exited unexpectedly");
        }
    }

    pipeline.shutdown().await?;

    if let Some(task) = api_task {
        task.abort();
    }

    info!("slot-stream stopped");
    Ok(())
}

enum SourceMode {
    Grpc,
    Scripted(Box<ChainScript>),
}

/// Choose the source. gRPC unless explicitly asked for the demo script.
fn source_mode() -> SourceMode {
    match std::env::var("SOURCE_MODE").as_deref() {
        Ok("scripted") => {
            let slots: u64 = std::env::var("SCRIPT_SLOTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50);
            let events: usize = std::env::var("SCRIPT_EVENTS_PER_SLOT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4);
            SourceMode::Scripted(Box::new(ChainScript::new().extend_from(1, slots, events)))
        }
        _ => SourceMode::Grpc,
    }
}

fn load_config() -> Result<PipelineConfig> {
    // A file if one is named, defaults otherwise, and in both cases the
    // environment is layered on top: a container image overrides DATABASE_URL
    // without having to rewrite the file it ships with.
    match std::env::var("CONFIG_FILE") {
        Ok(path) => {
            PipelineConfig::from_file(&path).with_context(|| format!("loading config from {path}"))
        }
        Err(_) => PipelineConfig::from_env().context("loading config from the environment"),
    }
}

fn init_tracing(config: &PipelineConfig) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.observability.log_level));

    let registry = tracing_subscriber::registry().with(filter);

    if config.observability.log_format == "json" {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }
}

fn init_metrics(
    config: &PipelineConfig,
) -> Result<Option<metrics_exporter_prometheus::PrometheusHandle>> {
    if !config.observability.metrics_enabled {
        return Ok(None);
    }

    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .context("installing the Prometheus recorder")?;

    Ok(Some(handle))
}

/// Resolve on SIGINT or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                // Without SIGTERM we can still stop on SIGINT, so log and wait
                // rather than taking the process down.
                warn!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
