# L3 Object Storage

A lightweight, local S3-compatible object storage server written in Rust.

**L3** stands for **Local Local Local** - emphasizing this is for local development only.

> **Warning**: This project is designed for local development and testing. It is **not production-ready**. Data loss is possible. Use at your own risk.

## Overview

L3 Object Storage provides a fully compatible Amazon S3 API implementation for local development and testing. Instead of connecting to AWS during development, point your application at L3 and enjoy:

- Fast local storage with no network latency
- No AWS credentials or costs during development
- Full control over test data
- Easy CI/CD integration

## Features

- **59 S3 API operations** implemented
- **Bucket operations**: Create, delete, list, head, location, ACL, policy, CORS, tagging, versioning, lifecycle, encryption, website, ownership controls, public access block
- **Object operations**: Put, get, head, delete, copy, list (v1/v2), tagging, ACL, attributes, rename
- **Versioning**: Full support for object versions and delete markers
- **Multipart uploads**: Create, upload parts, complete, abort, list parts/uploads, copy parts
- **Object Lock**: Legal hold, retention periods, compliance/governance modes
- **S3 Select**: SQL queries on CSV/JSON data
- **Authentication**: AWS Signature Version 4 (optional)
- **Event notifications**: SNS/SQS dispatch on object events with prefix/suffix filters
- **Pre-signed URLs**: GET, PUT, DELETE, HEAD with expiration
- **Range requests**: Partial object retrieval (HTTP 206)
- **Conditional requests**: If-Match, If-None-Match headers
- **Virtual hosted-style**: Optional `<bucket>.domain` addressing alongside path-style

## Quick Start

### Using Docker (Recommended)

```bash
# Run with Docker
docker run -d \
  -p 9000:9000 \
  -v ./data:/data \
  l3objectstorage:latest

# Or use docker-compose
docker-compose up -d
```

### Building from Source

```bash
# Build
cargo build --release

# Run
./target/release/l3-object-storage
```

### Using Make

```bash
# See all available commands
make help

# Build Docker images
make docker-build

# Run tests
make test
```

### Connect with AWS CLI

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
```

### Connect with AWS SDK

```rust
let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("http://localhost:9000")
    .region("us-east-1")
    .credentials_provider(Credentials::new("localadmin", "localadmin", None, None, "static"))
    .load()
    .await;

let client = aws_sdk_s3::Client::new(&config);
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LOCAL_S3_HOST` | `0.0.0.0` | Bind address |
| `LOCAL_S3_PORT` | `9000` | HTTP port |
| `LOCAL_S3_DATA_DIR` | `/data` | Storage directory |
| `LOCAL_S3_REGION` | `us-east-1` | AWS region for responses |
| `LOCAL_S3_ACCESS_KEY` | `localadmin` | Access key ID |
| `LOCAL_S3_SECRET_KEY` | `localadmin` | Secret access key |
| `LOCAL_S3_REQUIRE_AUTH` | `false` | Require SigV4 authentication |
| `LOCAL_S3_ENCRYPTION_KEY` | (none) | Master key for SSE-S3 (32 bytes, hex or base64) |
| `LOCAL_S3_SHUTDOWN_TIMEOUT` | `30` | Graceful shutdown timeout (seconds) |
| `LOCAL_S3_SNS_ENDPOINT` | (none) | SNS endpoint for event notifications |
| `LOCAL_S3_SQS_ENDPOINT` | (none) | SQS endpoint for event notifications |
| `LOCAL_S3_DOMAIN` | (none) | Base domain for virtual hosted-style addressing (e.g., `s3.local`) |
| `RUST_LOG` | `info` | Log level (`debug`, `info`, `warn`, `error`) |

## Virtual Hosted-Style Addressing

By default, L3 uses **path-style** requests (`http://localhost:9000/my-bucket/my-key`). You can also enable **virtual hosted-style** addressing where the bucket name is part of the hostname (`http://my-bucket.s3.local:9000/my-key`).

### Setup

Set the `LOCAL_S3_DOMAIN` environment variable to your chosen base domain:

```bash
docker run -d \
  -p 9000:9000 \
  -e LOCAL_S3_DOMAIN=s3.local \
  l3objectstorage:latest
```

With this configured, requests to `<bucket>.s3.local:9000` will extract the bucket from the hostname. Path-style requests continue to work as a fallback.

### DNS Resolution

The hostname `<bucket>.s3.local` must resolve to your L3 server. Options:

- **Docker Compose**: Add network aliases for each bucket (see `docker-compose.yml` for an example)
- **Local `/etc/hosts`**: Add entries like `127.0.0.1 my-bucket.s3.local`
- **dnsmasq / local DNS**: Wildcard `*.s3.local` to `127.0.0.1`

### AWS SDK Example

```rust
let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("http://s3.local:9000")
    .region("us-east-1")
    .credentials_provider(Credentials::new("localadmin", "localadmin", None, None, "static"))
    .load()
    .await;

// Use force_path_style(false) to enable virtual hosted-style
let s3_config = aws_sdk_s3::config::Builder::from(&config)
    .force_path_style(false)
    .build();

let client = aws_sdk_s3::Client::from_conf(s3_config);

// Requests will go to http://my-bucket.s3.local:9000/my-key
client.put_object()
    .bucket("my-bucket")
    .key("my-key")
    .body(ByteStream::from(b"hello".to_vec()))
    .send()
    .await?;
```

### Docker Compose Example

```yaml
services:
  s3:
    image: l3objectstorage:latest
    ports:
      - "9000:9000"
    environment:
      - LOCAL_S3_DOMAIN=s3.local
    networks:
      default:
        aliases:
          - s3.local
          - my-bucket.s3.local

  my-app:
    # your app container can reach s3 at http://my-bucket.s3.local:9000
    environment:
      - AWS_ENDPOINT_URL=http://s3.local:9000
```

## Event Notifications

L3 Object Storage can dispatch S3 event notifications to SNS topics and SQS queues when objects are created, deleted, or copied. This mirrors the real AWS S3 event notification system.

Supported events:
- `s3:ObjectCreated:Put`
- `s3:ObjectCreated:Copy`
- `s3:ObjectCreated:CompleteMultipartUpload`
- `s3:ObjectRemoved:Delete`

Configure notification endpoints via environment variables, then use the standard S3 API to set up bucket notification configurations with optional prefix/suffix key filters.

### Local Development Setup

Use `docker-compose up` to start L3 with local SNS ([local-sns](https://github.com/jameskbride/local-sns)) and SQS ([ElasticMQ](https://github.com/softwaremill/elasticmq)) emulators:

```bash
docker-compose up -d
```

Or start the infrastructure for integration testing:

```bash
make docker-up    # starts S3 + SNS + SQS on a shared Docker network
```

## Supported S3 Operations

### Bucket Operations (36)
CreateBucket, DeleteBucket, HeadBucket, ListBuckets, GetBucketLocation, GetBucketAcl, PutBucketAcl, GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy, GetBucketPolicyStatus, GetBucketCors, PutBucketCors, DeleteBucketCors, GetBucketTagging, PutBucketTagging, DeleteBucketTagging, GetBucketVersioning, PutBucketVersioning, GetBucketLifecycleConfiguration, PutBucketLifecycleConfiguration, DeleteBucketLifecycle, GetBucketNotificationConfiguration, PutBucketNotificationConfiguration, GetBucketWebsite, PutBucketWebsite, DeleteBucketWebsite, GetBucketEncryption, PutBucketEncryption, DeleteBucketEncryption, GetBucketOwnershipControls, PutBucketOwnershipControls, DeleteBucketOwnershipControls, GetPublicAccessBlock, PutPublicAccessBlock, DeletePublicAccessBlock

### Object Operations (16)
PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects, CopyObject, ListObjects, ListObjectsV2, ListObjectVersions, GetObjectAcl, PutObjectAcl, GetObjectTagging, PutObjectTagging, DeleteObjectTagging, GetObjectAttributes, RenameObject

### Object Lock Operations (6)
GetObjectLegalHold, PutObjectLegalHold, GetObjectRetention, PutObjectRetention, GetObjectLockConfiguration, PutObjectLockConfiguration

### Multipart Upload Operations (7)
CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads

See [S3_ACTIONS.md](./S3_ACTIONS.md) for the complete implementation status.

## Testing

```bash
# Run unit tests
cargo test

# Run all tests including integration tests (requires Docker)
make test-all

# Run only integration tests
make test-integration
```

**Test Coverage**: 373 unit tests + 212 AWS SDK integration tests

## Docker Images

Two variants are available:

- **Debian** (`l3objectstorage:latest`, `l3objectstorage:debian`) - Standard glibc-based image
- **Alpine** (`l3objectstorage:alpine`) - Smaller musl-based image

```bash
# Build both variants
make docker-build

# Build specific variant
make docker-build-debian
make docker-build-alpine
```

## Architecture

```
L3ObjectStorage
├── HTTP Server (hyper + tokio)
├── S3 API Router
├── Auth Module (SigV4)
├── Notification Dispatcher (SNS/SQS)
└── Storage Engine (filesystem-based)
    ├── Bucket Manager
    ├── Object Manager
    └── Metadata Store (JSON)
```

Data is stored in the configured data directory:
```
/data/
├── buckets/
│   └── {bucket-name}/
│       ├── objects/{hash}/...
│       └── .metadata/bucket.json
└── .system/
```

## License

MIT License. See [LICENSE](./LICENSE) for details.
