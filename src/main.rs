use l3_object_storage::prelude::*;
use tokio::sync::oneshot;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "l3_object_storage=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let config = Config::from_env()?;

    tracing::info!(
        "Starting L3ObjectStorage on {}:{}",
        config.host,
        config.port
    );
    tracing::info!("Data directory: {}", config.data_dir.display());
    tracing::info!(
        "Graceful shutdown timeout: {:?}",
        config.shutdown_timeout
    );

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Spawn signal handler
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(());
    });

    // Create and run server with graceful shutdown
    let server = Server::new(config).await?;
    server.run_with_shutdown(shutdown_rx).await?;

    tracing::info!("Server shutdown complete");
    Ok(())
}

/// Wait for shutdown signals (SIGTERM, SIGINT, Ctrl+C)
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM, initiating graceful shutdown...");
        }
    }
}
