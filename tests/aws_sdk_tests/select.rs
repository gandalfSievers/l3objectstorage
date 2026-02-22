//! SelectObjectContent integration tests
//!
//! Tests for S3 Select functionality - SQL queries on CSV and JSON objects

use super::*;
use aws_sdk_s3::types::{
    CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization,
    JsonInput, JsonOutput, JsonType, OutputSerialization,
};

/// Helper to collect SelectObjectContent results into a string
async fn collect_select_results(
    client: &Client,
    bucket: &str,
    key: &str,
    expression: &str,
    input_serialization: InputSerialization,
    output_serialization: OutputSerialization,
) -> Result<String, String> {
    let result = client
        .select_object_content()
        .bucket(bucket)
        .key(key)
        .expression(expression)
        .expression_type(ExpressionType::Sql)
        .input_serialization(input_serialization)
        .output_serialization(output_serialization)
        .send()
        .await
        .map_err(|e| format!("SelectObjectContent failed: {:?}", e))?;

    let mut output = String::new();
    let mut stream = result.payload;

    while let Some(event) = stream.recv().await.map_err(|e| format!("Stream error: {:?}", e))? {
        use aws_sdk_s3::types::SelectObjectContentEventStream;
        match event {
            SelectObjectContentEventStream::Records(records) => {
                if let Some(payload) = records.payload() {
                    output.push_str(std::str::from_utf8(payload.as_ref()).unwrap_or(""));
                }
            }
            SelectObjectContentEventStream::Stats(_) => {
                // Stats received - success indicator
            }
            SelectObjectContentEventStream::End(_) => {
                // End of stream
                break;
            }
            _ => {}
        }
    }

    Ok(output)
}

// =============================================================================
// CSV Input Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_select_csv_select_all() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-csv-all";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT * FROM s3object
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT * should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("Bob"), "Output should contain Bob");
    assert!(output.contains("Charlie"), "Output should contain Charlie");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_csv_specific_columns() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-csv-cols";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT specific columns
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT name, city FROM s3object",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT columns should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("NYC"), "Output should contain NYC");
    // Should NOT contain age values as standalone fields
    assert!(!output.contains(",30,"), "Output should not contain age column");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_csv_where_clause() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-csv-where";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT with WHERE clause (age > 28)
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object WHERE CAST(age AS INT) > 28",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT WHERE should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice (age 30)");
    assert!(output.contains("Charlie"), "Output should contain Charlie (age 35)");
    assert!(!output.contains("Bob"), "Output should NOT contain Bob (age 25)");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_csv_where_string_equals() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-csv-str";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,NYC\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT with string equality
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object WHERE city = 'NYC'",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT WHERE string should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("Charlie"), "Output should contain Charlie");
    assert!(!output.contains("Bob"), "Output should NOT contain Bob (LA)");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_csv_no_header() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-csv-nohdr";
    let key = "data.csv";

    // Setup - CSV without header row
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "Alice,30,NYC\nBob,25,LA\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT using column indices (_1, _2, _3)
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT _1, _3 FROM s3object",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::None)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT with indices should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("NYC"), "Output should contain NYC");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_csv_limit() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-csv-limit";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\nDave,40,Boston\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT with LIMIT
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object LIMIT 2",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT LIMIT should succeed: {:?}", result);
    let output = result.unwrap();
    let lines: Vec<&str> = output.trim().lines().collect();
    assert_eq!(lines.len(), 2, "Should return exactly 2 rows");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// =============================================================================
// JSON Input Tests
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_select_json_document() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-json-doc";
    let key = "data.json";

    // Setup - JSON document
    let _ = client.create_bucket().bucket(bucket).send().await;
    let json_content = r#"{"name": "Alice", "age": 30, "city": "NYC"}"#;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(json_content.as_bytes().to_vec().into())
        .content_type("application/json")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT from JSON
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object",
        InputSerialization::builder()
            .json(JsonInput::builder().r#type(JsonType::Document).build())
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .json(JsonOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT JSON should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("30"), "Output should contain age 30");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_json_lines() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-json-lines";
    let key = "data.jsonl";

    // Setup - JSON Lines format
    let _ = client.create_bucket().bucket(bucket).send().await;
    let jsonl_content = r#"{"name": "Alice", "age": 30, "city": "NYC"}
{"name": "Bob", "age": 25, "city": "LA"}
{"name": "Charlie", "age": 35, "city": "Chicago"}"#;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(jsonl_content.as_bytes().to_vec().into())
        .content_type("application/x-ndjson")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT from JSON Lines
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object",
        InputSerialization::builder()
            .json(JsonInput::builder().r#type(JsonType::Lines).build())
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .json(JsonOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT JSON Lines should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("Bob"), "Output should contain Bob");
    assert!(output.contains("Charlie"), "Output should contain Charlie");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_json_where_clause() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-json-where";
    let key = "data.jsonl";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let jsonl_content = r#"{"name": "Alice", "age": 30, "city": "NYC"}
{"name": "Bob", "age": 25, "city": "LA"}
{"name": "Charlie", "age": 35, "city": "NYC"}"#;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(jsonl_content.as_bytes().to_vec().into())
        .content_type("application/x-ndjson")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT with WHERE on JSON
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object s WHERE s.city = 'NYC'",
        InputSerialization::builder()
            .json(JsonInput::builder().r#type(JsonType::Lines).build())
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .json(JsonOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT JSON WHERE should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("Charlie"), "Output should contain Charlie");
    assert!(!output.contains("Bob"), "Output should NOT contain Bob (LA)");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_json_specific_fields() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-json-fields";
    let key = "data.jsonl";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let jsonl_content = r#"{"name": "Alice", "age": 30, "city": "NYC"}
{"name": "Bob", "age": 25, "city": "LA"}"#;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(jsonl_content.as_bytes().to_vec().into())
        .content_type("application/x-ndjson")
        .send()
        .await
        .expect("Failed to put object");

    // Test SELECT specific fields from JSON
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT s.name, s.city FROM s3object s",
        InputSerialization::builder()
            .json(JsonInput::builder().r#type(JsonType::Lines).build())
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .json(JsonOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SELECT JSON fields should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("Alice"), "Output should contain Alice");
    assert!(output.contains("NYC"), "Output should contain NYC");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// =============================================================================
// Cross-format Tests (CSV to JSON, JSON to CSV)
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_select_csv_to_json_output() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-csv-json";
    let key = "data.csv";

    // Setup - CSV input
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test CSV input with JSON output
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT * FROM s3object",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .json(JsonOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "CSV to JSON should succeed: {:?}", result);
    let output = result.unwrap();
    // Output should be JSON format with field names from CSV header
    assert!(output.contains("\"name\""), "JSON should have name field");
    assert!(output.contains("Alice"), "JSON should contain Alice");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// =============================================================================
// Error Cases
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_select_bucket_not_found() {
    let client = create_s3_client().await;

    // Test with non-existent bucket
    let result = client
        .select_object_content()
        .bucket("nonexistent-bucket-12345")
        .key("data.csv")
        .expression("SELECT * FROM s3object")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await;

    assert!(result.is_err(), "Should fail with bucket not found");
}

#[tokio::test]
#[ignore]
async fn test_select_key_not_found() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-nokey";

    // Setup bucket only
    let _ = client.create_bucket().bucket(bucket).send().await;

    // Test with non-existent key
    let result = client
        .select_object_content()
        .bucket(bucket)
        .key("nonexistent-key.csv")
        .expression("SELECT * FROM s3object")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await;

    assert!(result.is_err(), "Should fail with key not found");

    // Cleanup
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_invalid_sql() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-badsql";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test with invalid SQL syntax
    let result = client
        .select_object_content()
        .bucket(bucket)
        .key(key)
        .expression("INVALID SQL SYNTAX HERE")
        .expression_type(ExpressionType::Sql)
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .csv(CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await;

    assert!(result.is_err(), "Should fail with invalid SQL");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

// =============================================================================
// Aggregate Functions (COUNT, SUM, AVG, MIN, MAX)
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_select_count() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-count";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test COUNT(*)
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT COUNT(*) FROM s3object",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "COUNT should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("3"), "COUNT(*) should return 3");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[ignore]
async fn test_select_sum_avg() {
    let client = create_s3_client().await;
    let bucket = "sdk-select-sum";
    let key = "data.csv";

    // Setup
    let _ = client.create_bucket().bucket(bucket).send().await;
    let csv_content = "name,age,city\nAlice,30,NYC\nBob,20,LA\nCharlie,40,Chicago\n";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(csv_content.as_bytes().to_vec().into())
        .content_type("text/csv")
        .send()
        .await
        .expect("Failed to put object");

    // Test SUM
    let result = collect_select_results(
        &client,
        bucket,
        key,
        "SELECT SUM(CAST(age AS INT)) FROM s3object",
        InputSerialization::builder()
            .csv(
                CsvInput::builder()
                    .file_header_info(FileHeaderInfo::Use)
                    .build(),
            )
            .compression_type(CompressionType::None)
            .build(),
        OutputSerialization::builder()
            .csv(CsvOutput::builder().build())
            .build(),
    )
    .await;

    assert!(result.is_ok(), "SUM should succeed: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("90"), "SUM(age) should return 90");

    // Cleanup
    let _ = client.delete_object().bucket(bucket).key(key).send().await;
    let _ = client.delete_bucket().bucket(bucket).send().await;
}
