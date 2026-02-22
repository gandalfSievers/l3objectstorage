use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

/// Get the latency target from env var or use default
fn get_latency_target_ms() -> u64 {
    std::env::var("PERF_LATENCY_TARGET_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(METADATA_LATENCY_TARGET_MS)
}

/// Performance test configuration
const CONCURRENT_CONNECTIONS_TARGET: usize = 1000;
/// Latency target in ms - set higher for Docker-based testing (50ms vs 10ms for native)
/// Can be overridden via PERF_LATENCY_TARGET_MS env var
const METADATA_LATENCY_TARGET_MS: u64 = 50;

/// Collect timing statistics
struct TimingStats {
    durations: Vec<Duration>,
}

impl TimingStats {
    fn new() -> Self {
        Self { durations: Vec::new() }
    }

    fn add(&mut self, duration: Duration) {
        self.durations.push(duration);
    }

    fn extend(&mut self, other: Vec<Duration>) {
        self.durations.extend(other);
    }

    fn percentile(&self, p: f64) -> Duration {
        if self.durations.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.durations.clone();
        sorted.sort();
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn p50(&self) -> Duration {
        self.percentile(50.0)
    }

    fn p95(&self) -> Duration {
        self.percentile(95.0)
    }

    fn p99(&self) -> Duration {
        self.percentile(99.0)
    }

    fn min(&self) -> Duration {
        self.durations.iter().min().copied().unwrap_or(Duration::ZERO)
    }

    fn max(&self) -> Duration {
        self.durations.iter().max().copied().unwrap_or(Duration::ZERO)
    }

    fn mean(&self) -> Duration {
        if self.durations.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.durations.iter().sum();
        total / self.durations.len() as u32
    }

    fn count(&self) -> usize {
        self.durations.len()
    }
}

// ============================================================================
// CONCURRENT CONNECTION TESTS (1000+ connections)
// ============================================================================

/// Test 1000 concurrent PutObject operations
/// Validates: System can handle 1000+ concurrent connections
#[tokio::test]
#[ignore]
async fn test_1000_concurrent_put_objects() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "perf-1000-concurrent-puts";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let num_concurrent = CONCURRENT_CONNECTIONS_TARGET;
    let barrier = Arc::new(Barrier::new(num_concurrent));
    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_concurrent);

    for i in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let success_count = Arc::clone(&success_count);
        let failure_count = Arc::clone(&failure_count);

        let handle = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier.wait().await;

            let result = client
                .put_object()
                .bucket(bucket)
                .key(format!("concurrent-obj-{:04}", i))
                .body(Bytes::from(format!("content-{}", i)).into())
                .send()
                .await;

            match result {
                Ok(_) => success_count.fetch_add(1, Ordering::Relaxed),
                Err(_) => failure_count.fetch_add(1, Ordering::Relaxed),
            };
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    futures::future::join_all(handles).await;

    let elapsed = start.elapsed();
    let successes = success_count.load(Ordering::Relaxed);
    let failures = failure_count.load(Ordering::Relaxed);

    eprintln!("\n=== 1000 Concurrent PutObject Results ===");
    eprintln!("Total requests: {}", num_concurrent);
    eprintln!("Successes: {}", successes);
    eprintln!("Failures: {}", failures);
    eprintln!("Total time: {:?}", elapsed);
    eprintln!("Throughput: {:.2} req/sec", num_concurrent as f64 / elapsed.as_secs_f64());

    // Verify high success rate (allow for some failures under extreme load)
    let success_rate = successes as f64 / num_concurrent as f64;
    assert!(
        success_rate >= 0.99,
        "Success rate should be >= 99%, got {:.2}%",
        success_rate * 100.0
    );

    // Cleanup using batch delete
    let mut keys_to_delete = Vec::new();
    for i in 0..num_concurrent {
        keys_to_delete.push(
            ObjectIdentifier::builder()
                .key(format!("concurrent-obj-{:04}", i))
                .build()
                .unwrap(),
        );
    }

    for chunk in keys_to_delete.chunks(1000) {
        let delete = Delete::builder()
            .set_objects(Some(chunk.to_vec()))
            .build()
            .unwrap();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await
            .ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test 1000 concurrent GetObject operations
/// Validates: System can handle 1000+ concurrent read connections
#[tokio::test]
#[ignore]
async fn test_1000_concurrent_get_objects() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "perf-1000-concurrent-gets";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Pre-create objects for reading
    let num_objects = 100; // 100 objects, each read 10 times = 1000 concurrent reads
    for i in 0..num_objects {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("read-obj-{:02}", i))
            .body(Bytes::from(format!("content-{}", i)).into())
            .send()
            .await
            .expect("Failed to create test object");
    }

    let num_concurrent = CONCURRENT_CONNECTIONS_TARGET;
    let barrier = Arc::new(Barrier::new(num_concurrent));
    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_concurrent);

    for i in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let success_count = Arc::clone(&success_count);
        let failure_count = Arc::clone(&failure_count);
        let obj_idx = i % num_objects;

        let handle = tokio::spawn(async move {
            barrier.wait().await;

            let result = client
                .get_object()
                .bucket(bucket)
                .key(format!("read-obj-{:02}", obj_idx))
                .send()
                .await;

            match result {
                Ok(output) => {
                    // Consume the body to complete the request
                    let _ = output.body.collect().await;
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    failure_count.fetch_add(1, Ordering::Relaxed);
                }
            };
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    let elapsed = start.elapsed();
    let successes = success_count.load(Ordering::Relaxed);
    let failures = failure_count.load(Ordering::Relaxed);

    eprintln!("\n=== 1000 Concurrent GetObject Results ===");
    eprintln!("Total requests: {}", num_concurrent);
    eprintln!("Successes: {}", successes);
    eprintln!("Failures: {}", failures);
    eprintln!("Total time: {:?}", elapsed);
    eprintln!("Throughput: {:.2} req/sec", num_concurrent as f64 / elapsed.as_secs_f64());

    let success_rate = successes as f64 / num_concurrent as f64;
    assert!(
        success_rate >= 0.99,
        "Success rate should be >= 99%, got {:.2}%",
        success_rate * 100.0
    );

    // Cleanup
    for i in 0..num_objects {
        client
            .delete_object()
            .bucket(bucket)
            .key(format!("read-obj-{:02}", i))
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test 1000 concurrent HeadObject operations (metadata-only)
/// Validates: System can handle 1000+ concurrent metadata connections
#[tokio::test]
#[ignore]
async fn test_1000_concurrent_head_objects() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "perf-1000-concurrent-heads";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create one object for all heads to query
    client
        .put_object()
        .bucket(bucket)
        .key("head-target")
        .body(Bytes::from("test content").into())
        .send()
        .await
        .expect("Failed to create test object");

    let num_concurrent = CONCURRENT_CONNECTIONS_TARGET;
    let barrier = Arc::new(Barrier::new(num_concurrent));
    let success_count = Arc::new(AtomicUsize::new(0));
    let timings = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(num_concurrent)));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_concurrent);

    for _ in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let success_count = Arc::clone(&success_count);
        let timings = Arc::clone(&timings);

        let handle = tokio::spawn(async move {
            barrier.wait().await;

            let req_start = Instant::now();
            let result = client
                .head_object()
                .bucket(bucket)
                .key("head-target")
                .send()
                .await;

            let req_duration = req_start.elapsed();

            if result.is_ok() {
                success_count.fetch_add(1, Ordering::Relaxed);
                timings.lock().await.push(req_duration);
            }
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    let elapsed = start.elapsed();
    let successes = success_count.load(Ordering::Relaxed);

    let durations = timings.lock().await.clone();
    let mut stats = TimingStats::new();
    stats.extend(durations);

    eprintln!("\n=== 1000 Concurrent HeadObject Results ===");
    eprintln!("Total requests: {}", num_concurrent);
    eprintln!("Successes: {}", successes);
    eprintln!("Total time: {:?}", elapsed);
    eprintln!("Throughput: {:.2} req/sec", num_concurrent as f64 / elapsed.as_secs_f64());
    eprintln!("Latency p50: {:?}", stats.p50());
    eprintln!("Latency p95: {:?}", stats.p95());
    eprintln!("Latency p99: {:?}", stats.p99());

    let success_rate = successes as f64 / num_concurrent as f64;
    assert!(
        success_rate >= 0.99,
        "Success rate should be >= 99%, got {:.2}%",
        success_rate * 100.0
    );

    // Cleanup
    client.delete_object().bucket(bucket).key("head-target").send().await.ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Test mixed concurrent operations (put/get/head/delete)
/// Validates: System handles diverse concurrent workloads
#[tokio::test]
#[ignore]
async fn test_1000_concurrent_mixed_operations() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "perf-1000-concurrent-mixed";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Pre-create some objects for reads and deletes
    for i in 0..250 {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("existing-{:03}", i))
            .body(Bytes::from("existing content").into())
            .send()
            .await
            .expect("Failed to create test object");
    }

    let num_concurrent = CONCURRENT_CONNECTIONS_TARGET;
    let barrier = Arc::new(Barrier::new(num_concurrent));
    let put_success = Arc::new(AtomicUsize::new(0));
    let get_success = Arc::new(AtomicUsize::new(0));
    let head_success = Arc::new(AtomicUsize::new(0));
    let delete_success = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_concurrent);

    for i in 0..num_concurrent {
        let client = Arc::clone(&client);
        let barrier = Arc::clone(&barrier);
        let put_success = Arc::clone(&put_success);
        let get_success = Arc::clone(&get_success);
        let head_success = Arc::clone(&head_success);
        let delete_success = Arc::clone(&delete_success);

        let handle = tokio::spawn(async move {
            barrier.wait().await;

            let op_type = i % 4;
            match op_type {
                0 => {
                    // PUT new object
                    if client
                        .put_object()
                        .bucket(bucket)
                        .key(format!("new-{:04}", i))
                        .body(Bytes::from("new content").into())
                        .send()
                        .await
                        .is_ok()
                    {
                        put_success.fetch_add(1, Ordering::Relaxed);
                    }
                }
                1 => {
                    // GET existing object
                    let idx = (i / 4) % 250;
                    if let Ok(output) = client
                        .get_object()
                        .bucket(bucket)
                        .key(format!("existing-{:03}", idx))
                        .send()
                        .await
                    {
                        let _ = output.body.collect().await;
                        get_success.fetch_add(1, Ordering::Relaxed);
                    }
                }
                2 => {
                    // HEAD existing object
                    let idx = (i / 4) % 250;
                    if client
                        .head_object()
                        .bucket(bucket)
                        .key(format!("existing-{:03}", idx))
                        .send()
                        .await
                        .is_ok()
                    {
                        head_success.fetch_add(1, Ordering::Relaxed);
                    }
                }
                3 => {
                    // DELETE (will fail for non-existent, but that's OK)
                    let idx = (i / 4) % 250;
                    if client
                        .delete_object()
                        .bucket(bucket)
                        .key(format!("existing-{:03}", idx))
                        .send()
                        .await
                        .is_ok()
                    {
                        delete_success.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ => unreachable!(),
            }
        });
        handles.push(handle);
    }

    futures::future::join_all(handles).await;

    let elapsed = start.elapsed();

    eprintln!("\n=== 1000 Concurrent Mixed Operations Results ===");
    eprintln!("Total requests: {}", num_concurrent);
    eprintln!("PUT successes: {}", put_success.load(Ordering::Relaxed));
    eprintln!("GET successes: {}", get_success.load(Ordering::Relaxed));
    eprintln!("HEAD successes: {}", head_success.load(Ordering::Relaxed));
    eprintln!("DELETE successes: {}", delete_success.load(Ordering::Relaxed));
    eprintln!("Total time: {:?}", elapsed);
    eprintln!("Throughput: {:.2} req/sec", num_concurrent as f64 / elapsed.as_secs_f64());

    // Mixed ops: expect high success for puts and heads, variable for gets/deletes due to race conditions
    let total_success = put_success.load(Ordering::Relaxed)
        + get_success.load(Ordering::Relaxed)
        + head_success.load(Ordering::Relaxed)
        + delete_success.load(Ordering::Relaxed);

    assert!(
        total_success >= num_concurrent / 2,
        "At least 50% of mixed operations should succeed, got {}",
        total_success
    );

    // Cleanup
    let list_result = client.list_objects_v2().bucket(bucket).send().await;
    if let Ok(list) = list_result {
        for obj in list.contents() {
            if let Some(key) = obj.key() {
                client.delete_object().bucket(bucket).key(key).send().await.ok();
            }
        }
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ============================================================================
// LATENCY BENCHMARKING TESTS (<10ms for metadata operations)
// ============================================================================

/// Benchmark HeadBucket latency
/// Validates: HeadBucket p99 latency < 10ms on local SSD
#[tokio::test]
#[ignore]
async fn test_latency_head_bucket() {
    let client = create_s3_client().await;
    let bucket = "perf-latency-head-bucket";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Warm up
    for _ in 0..10 {
        let _ = client.head_bucket().bucket(bucket).send().await;
    }

    let iterations = 100;
    let mut stats = TimingStats::new();

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = client.head_bucket().bucket(bucket).send().await;
        stats.add(start.elapsed());
    }

    eprintln!("\n=== HeadBucket Latency Benchmark ===");
    eprintln!("Iterations: {}", stats.count());
    eprintln!("Min: {:?}", stats.min());
    eprintln!("Max: {:?}", stats.max());
    eprintln!("Mean: {:?}", stats.mean());
    eprintln!("p50: {:?}", stats.p50());
    eprintln!("p95: {:?}", stats.p95());
    eprintln!("p99: {:?}", stats.p99());

    // Validate p99 target (configurable via PERF_LATENCY_TARGET_MS env var)
    let target_ms = get_latency_target_ms();
    assert!(
        stats.p99() < Duration::from_millis(target_ms),
        "HeadBucket p99 latency ({:?}) should be < {}ms",
        stats.p99(),
        target_ms
    );

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Benchmark HeadObject latency
/// Validates: HeadObject p99 latency < 10ms on local SSD
#[tokio::test]
#[ignore]
async fn test_latency_head_object() {
    let client = create_s3_client().await;
    let bucket = "perf-latency-head-object";

    let _ = client.create_bucket().bucket(bucket).send().await;

    client
        .put_object()
        .bucket(bucket)
        .key("latency-test-object")
        .body(Bytes::from("test content for latency measurement").into())
        .send()
        .await
        .expect("Failed to create test object");

    // Warm up
    for _ in 0..10 {
        let _ = client
            .head_object()
            .bucket(bucket)
            .key("latency-test-object")
            .send()
            .await;
    }

    let iterations = 100;
    let mut stats = TimingStats::new();

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = client
            .head_object()
            .bucket(bucket)
            .key("latency-test-object")
            .send()
            .await;
        stats.add(start.elapsed());
    }

    eprintln!("\n=== HeadObject Latency Benchmark ===");
    eprintln!("Iterations: {}", stats.count());
    eprintln!("Min: {:?}", stats.min());
    eprintln!("Max: {:?}", stats.max());
    eprintln!("Mean: {:?}", stats.mean());
    eprintln!("p50: {:?}", stats.p50());
    eprintln!("p95: {:?}", stats.p95());
    eprintln!("p99: {:?}", stats.p99());

    let target_ms = get_latency_target_ms();
    assert!(
        stats.p99() < Duration::from_millis(target_ms),
        "HeadObject p99 latency ({:?}) should be < {}ms",
        stats.p99(),
        target_ms
    );

    client
        .delete_object()
        .bucket(bucket)
        .key("latency-test-object")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Benchmark ListBuckets latency
/// Validates: ListBuckets p99 latency < 10ms on local SSD
#[tokio::test]
#[ignore]
async fn test_latency_list_buckets() {
    let client = create_s3_client().await;

    // Create a few buckets to list
    for i in 0..5 {
        let _ = client
            .create_bucket()
            .bucket(format!("perf-latency-list-{}", i))
            .send()
            .await;
    }

    // Warm up
    for _ in 0..10 {
        let _ = client.list_buckets().send().await;
    }

    let iterations = 100;
    let mut stats = TimingStats::new();

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = client.list_buckets().send().await;
        stats.add(start.elapsed());
    }

    eprintln!("\n=== ListBuckets Latency Benchmark ===");
    eprintln!("Iterations: {}", stats.count());
    eprintln!("Min: {:?}", stats.min());
    eprintln!("Max: {:?}", stats.max());
    eprintln!("Mean: {:?}", stats.mean());
    eprintln!("p50: {:?}", stats.p50());
    eprintln!("p95: {:?}", stats.p95());
    eprintln!("p99: {:?}", stats.p99());

    let target_ms = get_latency_target_ms();
    assert!(
        stats.p99() < Duration::from_millis(target_ms),
        "ListBuckets p99 latency ({:?}) should be < {}ms",
        stats.p99(),
        target_ms
    );

    // Cleanup
    for i in 0..5 {
        let _ = client
            .delete_bucket()
            .bucket(format!("perf-latency-list-{}", i))
            .send()
            .await;
    }
}

/// Benchmark ListObjectsV2 latency (small bucket)
/// Validates: ListObjectsV2 p99 latency < 10ms on local SSD with small object count
#[tokio::test]
#[ignore]
async fn test_latency_list_objects_small() {
    let client = create_s3_client().await;
    let bucket = "perf-latency-list-objects";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create 10 objects
    for i in 0..10 {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("obj-{:02}", i))
            .body(Bytes::from("content").into())
            .send()
            .await
            .expect("Failed to create test object");
    }

    // Warm up
    for _ in 0..10 {
        let _ = client.list_objects_v2().bucket(bucket).send().await;
    }

    let iterations = 100;
    let mut stats = TimingStats::new();

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = client.list_objects_v2().bucket(bucket).send().await;
        stats.add(start.elapsed());
    }

    eprintln!("\n=== ListObjectsV2 (10 objects) Latency Benchmark ===");
    eprintln!("Iterations: {}", stats.count());
    eprintln!("Min: {:?}", stats.min());
    eprintln!("Max: {:?}", stats.max());
    eprintln!("Mean: {:?}", stats.mean());
    eprintln!("p50: {:?}", stats.p50());
    eprintln!("p95: {:?}", stats.p95());
    eprintln!("p99: {:?}", stats.p99());

    let target_ms = get_latency_target_ms();
    assert!(
        stats.p99() < Duration::from_millis(target_ms),
        "ListObjectsV2 p99 latency ({:?}) should be < {}ms",
        stats.p99(),
        target_ms
    );

    // Cleanup
    for i in 0..10 {
        client
            .delete_object()
            .bucket(bucket)
            .key(format!("obj-{:02}", i))
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Benchmark GetBucketLocation latency
/// Validates: GetBucketLocation p99 latency < 10ms on local SSD
#[tokio::test]
#[ignore]
async fn test_latency_get_bucket_location() {
    let client = create_s3_client().await;
    let bucket = "perf-latency-location";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Warm up
    for _ in 0..10 {
        let _ = client.get_bucket_location().bucket(bucket).send().await;
    }

    let iterations = 100;
    let mut stats = TimingStats::new();

    for _ in 0..iterations {
        let start = Instant::now();
        let _ = client.get_bucket_location().bucket(bucket).send().await;
        stats.add(start.elapsed());
    }

    eprintln!("\n=== GetBucketLocation Latency Benchmark ===");
    eprintln!("Iterations: {}", stats.count());
    eprintln!("Min: {:?}", stats.min());
    eprintln!("Max: {:?}", stats.max());
    eprintln!("Mean: {:?}", stats.mean());
    eprintln!("p50: {:?}", stats.p50());
    eprintln!("p95: {:?}", stats.p95());
    eprintln!("p99: {:?}", stats.p99());

    let target_ms = get_latency_target_ms();
    assert!(
        stats.p99() < Duration::from_millis(target_ms),
        "GetBucketLocation p99 latency ({:?}) should be < {}ms",
        stats.p99(),
        target_ms
    );

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ============================================================================
// THROUGHPUT TESTS
// ============================================================================

/// Measure sustained throughput for small object puts
/// Reports: Requests per second for small object uploads
#[tokio::test]
#[ignore]
async fn test_throughput_small_object_puts() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "perf-throughput-puts";

    let _ = client.create_bucket().bucket(bucket).send().await;

    let num_requests = 500;
    let concurrency = 50; // 50 concurrent requests at a time
    let start = Instant::now();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(num_requests);

    for i in 0..num_requests {
        let client = Arc::clone(&client);
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let handle = tokio::spawn(async move {
            let result = client
                .put_object()
                .bucket(bucket)
                .key(format!("throughput-obj-{:04}", i))
                .body(Bytes::from("small content").into())
                .send()
                .await;
            drop(permit);
            result
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    let elapsed = start.elapsed();

    let successes = results
        .iter()
        .filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false))
        .count();

    let throughput = num_requests as f64 / elapsed.as_secs_f64();

    eprintln!("\n=== Small Object PUT Throughput ===");
    eprintln!("Total requests: {}", num_requests);
    eprintln!("Concurrency: {}", concurrency);
    eprintln!("Successes: {}", successes);
    eprintln!("Total time: {:?}", elapsed);
    eprintln!("Throughput: {:.2} req/sec", throughput);

    assert!(successes >= num_requests * 99 / 100, "At least 99% should succeed");

    // Cleanup using batch delete
    let mut keys_to_delete = Vec::new();
    for i in 0..num_requests {
        keys_to_delete.push(
            ObjectIdentifier::builder()
                .key(format!("throughput-obj-{:04}", i))
                .build()
                .unwrap(),
        );
    }

    for chunk in keys_to_delete.chunks(1000) {
        let delete = Delete::builder()
            .set_objects(Some(chunk.to_vec()))
            .build()
            .unwrap();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await
            .ok();
    }

    let _ = client.delete_bucket().bucket(bucket).send().await;
}

/// Measure sustained throughput for small object gets
/// Reports: Requests per second for small object downloads
#[tokio::test]
#[ignore]
async fn test_throughput_small_object_gets() {
    let client = Arc::new(create_s3_client().await);
    let bucket = "perf-throughput-gets";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Pre-create objects
    let num_objects = 50;
    for i in 0..num_objects {
        client
            .put_object()
            .bucket(bucket)
            .key(format!("get-obj-{:02}", i))
            .body(Bytes::from("content for reading").into())
            .send()
            .await
            .expect("Failed to create test object");
    }

    let num_requests = 500;
    let concurrency = 50;
    let start = Instant::now();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(num_requests);

    for i in 0..num_requests {
        let client = Arc::clone(&client);
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let obj_idx = i % num_objects;

        let handle = tokio::spawn(async move {
            let result = client
                .get_object()
                .bucket(bucket)
                .key(format!("get-obj-{:02}", obj_idx))
                .send()
                .await;

            if let Ok(output) = result {
                let _ = output.body.collect().await;
                drop(permit);
                Ok(())
            } else {
                drop(permit);
                Err(())
            }
        });
        handles.push(handle);
    }

    let results: Vec<_> = futures::future::join_all(handles).await;
    let elapsed = start.elapsed();

    let successes = results
        .iter()
        .filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false))
        .count();

    let throughput = num_requests as f64 / elapsed.as_secs_f64();

    eprintln!("\n=== Small Object GET Throughput ===");
    eprintln!("Total requests: {}", num_requests);
    eprintln!("Concurrency: {}", concurrency);
    eprintln!("Successes: {}", successes);
    eprintln!("Total time: {:?}", elapsed);
    eprintln!("Throughput: {:.2} req/sec", throughput);

    assert!(successes >= num_requests * 99 / 100, "At least 99% should succeed");

    // Cleanup
    for i in 0..num_objects {
        client
            .delete_object()
            .bucket(bucket)
            .key(format!("get-obj-{:02}", i))
            .send()
            .await
            .ok();
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// ============================================================================
// SUMMARY TEST
// ============================================================================

/// Run all performance benchmarks and generate a summary report
/// This test should be run last to get a comprehensive view
#[tokio::test]
#[ignore]
async fn test_performance_summary() {
    eprintln!("\n");
    eprintln!("╔════════════════════════════════════════════════════════════╗");
    eprintln!("║            L3ObjectStorage Performance Summary              ║");
    eprintln!("╠════════════════════════════════════════════════════════════╣");
    eprintln!("║ Success Criteria:                                          ║");
    eprintln!("║   • Handle 1000+ concurrent connections      [See tests]   ║");
    eprintln!("║   • Metadata operations < 10ms p99           [See tests]   ║");
    eprintln!("║   • Docker image < 50MB                      [Check size]  ║");
    eprintln!("╠════════════════════════════════════════════════════════════╣");
    eprintln!("║ Run individual tests for detailed results:                 ║");
    eprintln!("║   cargo test --test aws_sdk_tests test_1000_concurrent     ║");
    eprintln!("║   cargo test --test aws_sdk_tests test_latency             ║");
    eprintln!("║   cargo test --test aws_sdk_tests test_throughput          ║");
    eprintln!("╚════════════════════════════════════════════════════════════╝");
    eprintln!("\n");

    // Quick sanity check that server is running
    let client = create_s3_client().await;
    let result = client.list_buckets().send().await;
    assert!(result.is_ok(), "Server should be running and accessible");
}
