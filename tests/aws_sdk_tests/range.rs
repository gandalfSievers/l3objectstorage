use super::*;

#[tokio::test]
#[ignore]
async fn test_get_object_range_request() {
    let client = create_s3_client().await;
    let bucket = "sdk-range-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create object with known content: "0123456789ABCDEFGHIJ" (20 bytes)
    let content = "0123456789ABCDEFGHIJ";
    client
        .put_object()
        .bucket(bucket)
        .key("range-test-key")
        .body(Bytes::from(content).into())
        .send()
        .await
        .expect("Failed to put object");

    // Test 1: Get first 5 bytes (bytes=0-4)
    let response = client
        .get_object()
        .bucket(bucket)
        .key("range-test-key")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Failed to get range 0-4");

    let content_length = response.content_length();
    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("01234"), "First 5 bytes should be 01234");
    assert_eq!(content_length, Some(5));

    // Test 2: Get middle bytes (bytes=5-9)
    let response = client
        .get_object()
        .bucket(bucket)
        .key("range-test-key")
        .range("bytes=5-9")
        .send()
        .await
        .expect("Failed to get range 5-9");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("56789"), "Middle bytes should be 56789");

    // Test 3: Get last 5 bytes (bytes=-5 or bytes=15-19)
    let response = client
        .get_object()
        .bucket(bucket)
        .key("range-test-key")
        .range("bytes=15-19")
        .send()
        .await
        .expect("Failed to get range 15-19");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("FGHIJ"), "Last 5 bytes should be FGHIJ");

    // Test 4: Get from offset to end (bytes=10-)
    let response = client
        .get_object()
        .bucket(bucket)
        .key("range-test-key")
        .range("bytes=10-")
        .send()
        .await
        .expect("Failed to get range 10-");

    let content_length = response.content_length();
    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("ABCDEFGHIJ"), "From offset 10 should be ABCDEFGHIJ");
    assert_eq!(content_length, Some(10));

    // Test 5: Get last N bytes (bytes=-3)
    let response = client
        .get_object()
        .bucket(bucket)
        .key("range-test-key")
        .range("bytes=-3")
        .send()
        .await
        .expect("Failed to get range -3");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("HIJ"), "Last 3 bytes should be HIJ");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("range-test-key")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_get_object_range_beyond_content() {
    let client = create_s3_client().await;
    let bucket = "sdk-range-beyond-test";

    let _ = client.create_bucket().bucket(bucket).send().await;

    // Create small object (10 bytes)
    let content = "0123456789";
    client
        .put_object()
        .bucket(bucket)
        .key("small-object")
        .body(Bytes::from(content).into())
        .send()
        .await
        .expect("Failed to put object");

    // Request range that starts within content but extends beyond
    // S3 should return only the available bytes (bytes=5-100 for 10-byte object)
    let response = client
        .get_object()
        .bucket(bucket)
        .key("small-object")
        .range("bytes=5-100")
        .send()
        .await
        .expect("Failed to get oversized range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, Bytes::from("56789"), "Should return available bytes from offset 5");

    // Cleanup
    client
        .delete_object()
        .bucket(bucket)
        .key("small-object")
        .send()
        .await
        .ok();
    let _ = client.delete_bucket().bucket(bucket).send().await;
}
