//! Graceful shutdown integration tests
//!
//! These tests verify that the server properly handles shutdown signals
//! and allows in-flight requests to complete.

use bytes::Bytes;
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use l3_object_storage::config::Config;
use l3_object_storage::server::Server;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

/// Helper to create a test server configuration
fn test_config(temp_dir: &TempDir, port: u16) -> Config {
    Config::new()
        .with_data_dir(temp_dir.path())
        .with_port(port)
        .with_shutdown_timeout(Duration::from_secs(5))
}

/// Helper to find an available port
async fn find_available_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Helper to make an HTTP request to the server
async fn make_request(addr: SocketAddr, method: &str, path: &str) -> Result<Response<Incoming>, Box<dyn std::error::Error + Send + Sync>> {
    let stream = TcpStream::connect(addr).await?;
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!("Connection error: {}", e);
        }
    });

    let req = Request::builder()
        .method(method)
        .uri(format!("http://{}{}", addr, path))
        .header("Host", addr.to_string())
        .body(Empty::<Bytes>::new())?;

    let response = sender.send_request(req).await?;
    Ok(response)
}

#[tokio::test]
async fn test_shutdown_stops_accepting_new_connections() {
    let temp_dir = TempDir::new().unwrap();
    let port = find_available_port().await;
    let config = test_config(&temp_dir, port);
    let addr = config.socket_addr();

    let server = Server::new(config).await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Start server in background
    let server_handle = tokio::spawn(async move {
        server.run_with_shutdown(shutdown_rx).await
    });

    // Wait for server to be ready
    sleep(Duration::from_millis(100)).await;

    // Make a request before shutdown - should succeed
    let response = make_request(addr, "GET", "/").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Send shutdown signal
    shutdown_tx.send(()).unwrap();

    // Wait for server to stop
    let result = timeout(Duration::from_secs(2), server_handle).await;
    assert!(result.is_ok(), "Server should shut down within timeout");

    // New connections after shutdown should fail
    sleep(Duration::from_millis(100)).await;
    let result = TcpStream::connect(addr).await;
    assert!(result.is_err(), "Connection after shutdown should fail");
}

#[tokio::test]
async fn test_inflight_requests_complete_before_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let port = find_available_port().await;
    let config = test_config(&temp_dir, port);
    let addr = config.socket_addr();

    let server = Server::new(config).await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Start server in background
    let server_handle = tokio::spawn(async move {
        server.run_with_shutdown(shutdown_rx).await
    });

    // Wait for server to be ready
    sleep(Duration::from_millis(100)).await;

    // Make a request and verify it completes
    let response = make_request(addr, "PUT", "/test-bucket").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Send shutdown signal
    shutdown_tx.send(()).unwrap();

    // Server should complete shutdown gracefully
    let result = timeout(Duration::from_secs(5), server_handle).await;
    assert!(result.is_ok(), "Server should shut down gracefully");

    // Verify the server stopped cleanly (run_with_shutdown returns Ok)
    let inner_result = result.unwrap();
    assert!(inner_result.is_ok(), "Server should return Ok from shutdown");
}

#[tokio::test]
async fn test_shutdown_timeout_forces_termination() {
    let temp_dir = TempDir::new().unwrap();
    let port = find_available_port().await;

    // Use a very short shutdown timeout
    let config = Config::new()
        .with_data_dir(temp_dir.path())
        .with_port(port)
        .with_shutdown_timeout(Duration::from_millis(100));

    let addr = config.socket_addr();
    let server = Server::new(config).await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Start server in background
    let server_handle = tokio::spawn(async move {
        server.run_with_shutdown(shutdown_rx).await
    });

    // Wait for server to be ready
    sleep(Duration::from_millis(100)).await;

    // Make initial connection (keep it open but idle)
    let _stream = TcpStream::connect(addr).await.unwrap();

    // Send shutdown signal
    shutdown_tx.send(()).unwrap();

    // Server should shut down within timeout + grace period
    let result = timeout(Duration::from_secs(2), server_handle).await;
    assert!(result.is_ok(), "Server should force shutdown after timeout");
}

#[tokio::test]
async fn test_server_run_returns_when_shutdown_triggered() {
    let temp_dir = TempDir::new().unwrap();
    let port = find_available_port().await;
    let config = test_config(&temp_dir, port);

    let server = Server::new(config).await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Start server in background
    let server_handle = tokio::spawn(async move {
        server.run_with_shutdown(shutdown_rx).await
    });

    // Wait for server to be ready
    sleep(Duration::from_millis(100)).await;

    // Send shutdown signal
    shutdown_tx.send(()).unwrap();

    // Server should return successfully (not hang)
    let result = timeout(Duration::from_secs(2), server_handle).await;
    assert!(result.is_ok(), "Server should return after shutdown signal");

    let inner_result = result.unwrap();
    assert!(inner_result.is_ok(), "Server::run_with_shutdown should return Ok");
}

#[tokio::test]
async fn test_multiple_connections_during_shutdown() {
    let temp_dir = TempDir::new().unwrap();
    let port = find_available_port().await;
    let config = test_config(&temp_dir, port);
    let addr = config.socket_addr();

    let server = Server::new(config).await.unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Start server in background
    let server_handle = tokio::spawn(async move {
        server.run_with_shutdown(shutdown_rx).await
    });

    // Wait for server to be ready
    sleep(Duration::from_millis(100)).await;

    // Create multiple concurrent connections
    let mut handles = Vec::new();
    for i in 0..5 {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let response = make_request(addr, "GET", "/").await;
            (i, response.is_ok())
        }));
    }

    // Wait for all requests to start
    sleep(Duration::from_millis(50)).await;

    // Send shutdown signal
    shutdown_tx.send(()).unwrap();

    // All requests should complete
    for handle in handles {
        let (i, ok) = handle.await.unwrap();
        assert!(ok, "Request {} should complete", i);
    }

    // Server should shut down
    let result = timeout(Duration::from_secs(5), server_handle).await;
    assert!(result.is_ok(), "Server should shut down after all requests complete");
}

#[tokio::test]
async fn test_shutdown_config_default() {
    let config = Config::new();
    // Default shutdown timeout should be 30 seconds
    assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
}

#[tokio::test]
async fn test_shutdown_config_custom() {
    let config = Config::new()
        .with_shutdown_timeout(Duration::from_secs(60));
    assert_eq!(config.shutdown_timeout, Duration::from_secs(60));
}
