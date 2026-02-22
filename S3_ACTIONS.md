# S3 API Actions Implementation Tracker

This document tracks the implementation status of all S3 API actions in L3ObjectStorage.

## Status Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Implemented |
| 🚧 | In Progress |
| 📋 | Planned |
| ❌ | Not Planned |
| ❓ | Under Consideration |

## Priority Legend

| Priority | Description |
|----------|-------------|
| P0 | Must Have - Core functionality required for basic S3 compatibility |
| P1 | Should Have - Common features used by most applications |
| P2 | Nice to Have - Advanced features for specific use cases |
| P3 | Future - Specialized features, may not implement |

---

## Amazon S3 API Actions (110 actions)

### Bucket Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateBucket | ✅ | P0 | Implemented |
| DeleteBucket | ✅ | P0 | Implemented |
| HeadBucket | ✅ | P0 | Implemented |
| ListBuckets | ✅ | P0 | Implemented |
| ListDirectoryBuckets | ❌ | - | Directory buckets not supported |
| GetBucketLocation | ✅ | P0 | Implemented |
| GetBucketAcl | ✅ | P1 | Implemented |
| PutBucketAcl | ✅ | P1 | Implemented (canned ACLs) |
| GetBucketPolicy | ✅ | P1 | Implemented |
| PutBucketPolicy | ✅ | P1 | Implemented |
| DeleteBucketPolicy | ✅ | P1 | Implemented |
| GetBucketPolicyStatus | ✅ | P2 | Implemented (public access detection) |
| GetBucketCors | ✅ | P1 | Implemented |
| PutBucketCors | ✅ | P1 | Implemented |
| DeleteBucketCors | ✅ | P1 | Implemented |
| GetBucketTagging | ✅ | P1 | Implemented |
| PutBucketTagging | ✅ | P1 | Implemented |
| DeleteBucketTagging | ✅ | P1 | Implemented |
| GetBucketVersioning | ✅ | P1 | Implemented |
| PutBucketVersioning | ✅ | P1 | Implemented |
| GetBucketLifecycle | ✅ | P2 | Deprecated, use LifecycleConfiguration |
| GetBucketLifecycleConfiguration | ✅ | P2 | Implemented |
| PutBucketLifecycle | ✅ | P2 | Deprecated |
| PutBucketLifecycleConfiguration | ✅ | P2 | Implemented |
| DeleteBucketLifecycle | ✅ | P2 | Implemented |
| GetBucketLogging | ✅ | P3 | Implemented (config storage only) |
| PutBucketLogging | ✅ | P3 | Implemented (config storage only) |
| GetBucketNotification | 📋 | P3 | Deprecated |
| GetBucketNotificationConfiguration | ✅ | P3 | Implemented (config storage only) |
| PutBucketNotification | 📋 | P3 | Deprecated |
| PutBucketNotificationConfiguration | ✅ | P3 | Implemented (config storage only) |
| GetBucketReplication | ✅ | P3 | Implemented (config storage only) |
| PutBucketReplication | ✅ | P3 | Implemented (config storage only) |
| DeleteBucketReplication | ✅ | P3 | Implemented (config storage only) |
| GetBucketRequestPayment | ✅ | P3 | Implemented (config storage only) |
| PutBucketRequestPayment | ✅ | P3 | Implemented (config storage only) |
| GetBucketWebsite | ✅ | P2 | Implemented |
| PutBucketWebsite | ✅ | P2 | Implemented |
| DeleteBucketWebsite | ✅ | P2 | Implemented |
| GetBucketEncryption | ✅ | P2 | Implemented (SSE-S3 / AES256) |
| PutBucketEncryption | ✅ | P2 | Implemented |
| DeleteBucketEncryption | ✅ | P2 | Implemented |
| GetBucketAccelerateConfiguration | ❌ | - | Not applicable locally |
| PutBucketAccelerateConfiguration | ❌ | - | Not applicable locally |
| GetBucketOwnershipControls | ✅ | P2 | Implemented |
| PutBucketOwnershipControls | ✅ | P2 | Implemented |
| DeleteBucketOwnershipControls | ✅ | P2 | Implemented |
| GetPublicAccessBlock | ✅ | P2 | Implemented |
| PutPublicAccessBlock | ✅ | P2 | Implemented |
| DeletePublicAccessBlock | ✅ | P2 | Implemented |
| GetBucketAnalyticsConfiguration | ❌ | - | Analytics not supported |
| PutBucketAnalyticsConfiguration | ❌ | - | |
| DeleteBucketAnalyticsConfiguration | ❌ | - | |
| ListBucketAnalyticsConfigurations | ❌ | - | |
| GetBucketMetricsConfiguration | ❌ | - | Metrics not supported |
| PutBucketMetricsConfiguration | ❌ | - | |
| DeleteBucketMetricsConfiguration | ❌ | - | |
| ListBucketMetricsConfigurations | ❌ | - | |
| GetBucketInventoryConfiguration | ❌ | - | Inventory not supported |
| PutBucketInventoryConfiguration | ❌ | - | |
| DeleteBucketInventoryConfiguration | ❌ | - | |
| ListBucketInventoryConfigurations | ❌ | - | |
| GetBucketIntelligentTieringConfiguration | ❌ | - | Tiering not supported |
| PutBucketIntelligentTieringConfiguration | ❌ | - | |
| DeleteBucketIntelligentTieringConfiguration | ❌ | - | |
| ListBucketIntelligentTieringConfigurations | ❌ | - | |
| CreateBucketMetadataConfiguration | ❌ | - | Metadata tables feature |
| GetBucketMetadataConfiguration | ❌ | - | |
| DeleteBucketMetadataConfiguration | ❌ | - | |
| CreateBucketMetadataTableConfiguration | ❌ | - | Metadata tables feature |
| GetBucketMetadataTableConfiguration | ❌ | - | |
| DeleteBucketMetadataTableConfiguration | ❌ | - | |
| UpdateBucketMetadataInventoryTableConfiguration | ❌ | - | |
| UpdateBucketMetadataJournalTableConfiguration | ❌ | - | |
| GetBucketAbac | ❌ | - | ABAC not supported |
| PutBucketAbac | ❌ | - | |

### Object Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| PutObject | ✅ | P0 | Implemented (If-None-Match: * for create-only) |
| GetObject | ✅ | P0 | Implemented (Range requests, If-Match, If-None-Match, versionId) |
| HeadObject | ✅ | P0 | Implemented |
| DeleteObject | ✅ | P0 | Implemented (supports versionId) |
| DeleteObjects | ✅ | P0 | Implemented - Batch delete |
| CopyObject | ✅ | P0 | Implemented |
| ListObjects | ✅ | P0 | Implemented |
| ListObjectsV2 | ✅ | P0 | Implemented |
| ListObjectVersions | ✅ | P1 | Implemented |
| GetObjectAcl | ✅ | P1 | Implemented (supports versionId) |
| PutObjectAcl | ✅ | P1 | Implemented (canned ACLs) |
| GetObjectTagging | ✅ | P1 | Implemented (supports versionId) |
| PutObjectTagging | ✅ | P1 | Implemented (supports versionId) |
| DeleteObjectTagging | ✅ | P1 | Implemented (supports versionId) |
| GetObjectAttributes | ✅ | P1 | Implemented (ETag, ObjectSize, StorageClass, versionId) |
| GetObjectLegalHold | ✅ | P2 | Object Lock feature - Implemented |
| PutObjectLegalHold | ✅ | P2 | Object Lock feature - Implemented |
| GetObjectRetention | ✅ | P2 | Object Lock feature - Implemented |
| PutObjectRetention | ✅ | P2 | Object Lock feature - Implemented |
| GetObjectLockConfiguration | ✅ | P2 | Object Lock feature - Implemented |
| PutObjectLockConfiguration | ✅ | P2 | Object Lock feature - Implemented |
| RestoreObject | ❌ | - | Glacier not applicable |
| GetObjectTorrent | ❌ | - | Torrent not supported |
| SelectObjectContent | ✅ | P3 | Implemented (SQL on CSV/JSON with WHERE, LIMIT, aggregates) |
| RenameObject | ✅ | P2 | Implemented (within same bucket) |
| WriteGetObjectResponse | ❌ | - | Lambda integration |

### Multipart Upload Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateMultipartUpload | ✅ | P0 | Implemented |
| UploadPart | ✅ | P0 | Implemented |
| UploadPartCopy | ✅ | P1 | Implemented (with range support) |
| CompleteMultipartUpload | ✅ | P0 | Implemented |
| AbortMultipartUpload | ✅ | P0 | Implemented |
| ListParts | ✅ | P0 | Implemented |
| ListMultipartUploads | ✅ | P0 | Implemented |

### Session Operations (Express One Zone)

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateSession | ❌ | - | Express One Zone not supported |

---

## Amazon S3 Control API Actions (96 actions)

These are management plane operations accessed via s3-control endpoints. Most are not applicable for local storage.

### Access Point Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateAccessPoint | ❌ | - | Access Points not supported |
| DeleteAccessPoint | ❌ | - | |
| GetAccessPoint | ❌ | - | |
| ListAccessPoints | ❌ | - | |
| ListAccessPointsForDirectoryBuckets | ❌ | - | |
| PutAccessPointPolicy | ❌ | - | |
| GetAccessPointPolicy | ❌ | - | |
| DeleteAccessPointPolicy | ❌ | - | |
| GetAccessPointPolicyStatus | ❌ | - | |
| GetAccessPointScope | ❌ | - | |
| PutAccessPointScope | ❌ | - | |
| DeleteAccessPointScope | ❌ | - | |

### Object Lambda Access Point Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateAccessPointForObjectLambda | ❌ | - | Object Lambda not supported |
| DeleteAccessPointForObjectLambda | ❌ | - | |
| GetAccessPointForObjectLambda | ❌ | - | |
| ListAccessPointsForObjectLambda | ❌ | - | |
| GetAccessPointConfigurationForObjectLambda | ❌ | - | |
| PutAccessPointConfigurationForObjectLambda | ❌ | - | |
| GetAccessPointPolicyForObjectLambda | ❌ | - | |
| PutAccessPointPolicyForObjectLambda | ❌ | - | |
| DeleteAccessPointPolicyForObjectLambda | ❌ | - | |
| GetAccessPointPolicyStatusForObjectLambda | ❌ | - | |

### Multi-Region Access Point Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateMultiRegionAccessPoint | ❌ | - | Multi-region not applicable |
| DeleteMultiRegionAccessPoint | ❌ | - | |
| GetMultiRegionAccessPoint | ❌ | - | |
| ListMultiRegionAccessPoints | ❌ | - | |
| DescribeMultiRegionAccessPointOperation | ❌ | - | |
| GetMultiRegionAccessPointPolicy | ❌ | - | |
| PutMultiRegionAccessPointPolicy | ❌ | - | |
| GetMultiRegionAccessPointPolicyStatus | ❌ | - | |
| GetMultiRegionAccessPointRoutes | ❌ | - | |
| SubmitMultiRegionAccessPointRoutes | ❌ | - | |

### S3 Batch Operations (Jobs)

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateJob | ❌ | - | Batch Operations not supported |
| DescribeJob | ❌ | - | |
| ListJobs | ❌ | - | |
| UpdateJobPriority | ❌ | - | |
| UpdateJobStatus | ❌ | - | |
| GetJobTagging | ❌ | - | |
| PutJobTagging | ❌ | - | |
| DeleteJobTagging | ❌ | - | |

### Storage Lens Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| GetStorageLensConfiguration | ❌ | - | Storage Lens not supported |
| PutStorageLensConfiguration | ❌ | - | |
| DeleteStorageLensConfiguration | ❌ | - | |
| ListStorageLensConfigurations | ❌ | - | |
| GetStorageLensConfigurationTagging | ❌ | - | |
| PutStorageLensConfigurationTagging | ❌ | - | |
| DeleteStorageLensConfigurationTagging | ❌ | - | |
| CreateStorageLensGroup | ❌ | - | |
| DeleteStorageLensGroup | ❌ | - | |
| GetStorageLensGroup | ❌ | - | |
| ListStorageLensGroups | ❌ | - | |
| UpdateStorageLensGroup | ❌ | - | |

### Access Grants Operations

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateAccessGrant | ❌ | - | Access Grants not supported |
| DeleteAccessGrant | ❌ | - | |
| GetAccessGrant | ❌ | - | |
| ListAccessGrants | ❌ | - | |
| ListCallerAccessGrants | ❌ | - | |
| GetDataAccess | ❌ | - | |
| CreateAccessGrantsInstance | ❌ | - | |
| DeleteAccessGrantsInstance | ❌ | - | |
| GetAccessGrantsInstance | ❌ | - | |
| ListAccessGrantsInstances | ❌ | - | |
| GetAccessGrantsInstanceForPrefix | ❌ | - | |
| GetAccessGrantsInstanceResourcePolicy | ❌ | - | |
| PutAccessGrantsInstanceResourcePolicy | ❌ | - | |
| DeleteAccessGrantsInstanceResourcePolicy | ❌ | - | |
| AssociateAccessGrantsIdentityCenter | ❌ | - | |
| DissociateAccessGrantsIdentityCenter | ❌ | - | |
| CreateAccessGrantsLocation | ❌ | - | |
| DeleteAccessGrantsLocation | ❌ | - | |
| GetAccessGrantsLocation | ❌ | - | |
| ListAccessGrantsLocations | ❌ | - | |
| UpdateAccessGrantsLocation | ❌ | - | |

### Account-Level Public Access Block

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| GetPublicAccessBlock | ❌ | - | Account-level not applicable (single-tenant) |
| PutPublicAccessBlock | ❌ | - | |
| DeletePublicAccessBlock | ❌ | - | |

### Outposts Bucket Operations (S3 Control)

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateBucket | ❌ | - | Outposts buckets not supported |
| DeleteBucket | ❌ | - | |
| GetBucket | ❌ | - | |
| ListRegionalBuckets | ❌ | - | |
| GetBucketLifecycleConfiguration | ❌ | - | |
| PutBucketLifecycleConfiguration | ❌ | - | |
| DeleteBucketLifecycleConfiguration | ❌ | - | |
| GetBucketPolicy | ❌ | - | |
| PutBucketPolicy | ❌ | - | |
| DeleteBucketPolicy | ❌ | - | |
| GetBucketReplication | ❌ | - | |
| PutBucketReplication | ❌ | - | |
| DeleteBucketReplication | ❌ | - | |
| GetBucketTagging | ❌ | - | |
| PutBucketTagging | ❌ | - | |
| DeleteBucketTagging | ❌ | - | |
| GetBucketVersioning | ❌ | - | |
| PutBucketVersioning | ❌ | - | |

### Resource Tagging (S3 Control)

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| ListTagsForResource | ❌ | - | Generic resource tagging API |
| TagResource | ❌ | - | |
| UntagResource | ❌ | - | |

---

## Amazon S3 on Outposts Actions (5 actions)

Not applicable for local storage.

| Action | Status | Priority | Notes |
|--------|--------|----------|-------|
| CreateEndpoint | ❌ | - | Outposts not applicable |
| DeleteEndpoint | ❌ | - | |
| ListEndpoints | ❌ | - | |
| ListOutpostsWithS3 | ❌ | - | |
| ListSharedEndpoints | ❌ | - | |

---

## Implementation Summary

### Overall Statistics

| Category | Implemented | Not Planned | Total in AWS |
|----------|-------------|-------------|--------------|
| Amazon S3 API | 57 | 53 | 110 |
| S3 Control API | 0 | 96 | 96 |
| S3 on Outposts | 0 | 5 | 5 |

### Phase 1: Foundation (P0 - Must Have) ✅ COMPLETE

| Category | Actions | Status |
|----------|---------|--------|
| Bucket Ops | CreateBucket, DeleteBucket, HeadBucket, ListBuckets, GetBucketLocation | **5/5** ✅ |
| Object Ops | PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects, CopyObject, ListObjects, ListObjectsV2 | **8/8** ✅ |
| Multipart | CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads | **6/6** ✅ |
| **Total P0** | **19 actions** | **19/19** ✅ |

### Phase 2: Common Features (P1 - Should Have) ✅ COMPLETE

| Category | Actions | Status |
|----------|---------|--------|
| Bucket Config | ACL, Policy, CORS, Tagging, Versioning | 13/13 |
| Object Config | ACL, Tagging, Attributes, ListObjectVersions | 6/6 |
| Multipart | UploadPartCopy | 1/1 |
| **Total P1** | **20 actions** | **20/20** ✅ |

### Phase 3: Advanced Features (P2 - Nice to Have) ✅ COMPLETE

| Category | Actions | Status |
|----------|---------|--------|
| Bucket Features | Lifecycle, Encryption, Website, Ownership, PublicAccess, PolicyStatus | 16/16 |
| Object Features | Object Lock (6 actions), RenameObject | 7/7 |
| **Total P2** | **23 actions** | **23/23** ✅ |

### Phase 4: Specialized Features (P3 - Config Storage Only) ✅ COMPLETE

| Category | Actions | Status |
|----------|---------|--------|
| Logging | GetBucketLogging, PutBucketLogging | 2/2 |
| Notifications | Get/PutBucketNotificationConfiguration | 2/2 |
| Replication | Get/Put/DeleteBucketReplication | 3/3 |
| Request Payment | Get/PutBucketRequestPayment | 2/2 |
| S3 Select | SelectObjectContent | 1/1 |
| **Total P3** | **10 actions** | **10/10** ✅ |

---

## Quick Reference: All Implemented Actions (57 total)

### Bucket Operations (34)
1. CreateBucket
2. DeleteBucket
3. HeadBucket
4. ListBuckets
5. GetBucketLocation
6. GetBucketAcl
7. PutBucketAcl
8. GetBucketPolicy
9. PutBucketPolicy
10. DeleteBucketPolicy
11. GetBucketPolicyStatus
12. GetBucketCors
13. PutBucketCors
14. DeleteBucketCors
15. GetBucketTagging
16. PutBucketTagging
17. DeleteBucketTagging
18. GetBucketVersioning
19. PutBucketVersioning
20. GetBucketLifecycleConfiguration
21. PutBucketLifecycleConfiguration
22. DeleteBucketLifecycle
23. GetBucketWebsite
24. PutBucketWebsite
25. DeleteBucketWebsite
26. GetBucketEncryption
27. PutBucketEncryption
28. DeleteBucketEncryption
29. GetBucketOwnershipControls
30. PutBucketOwnershipControls
31. DeleteBucketOwnershipControls
32. GetPublicAccessBlock
33. PutPublicAccessBlock
34. DeletePublicAccessBlock

### Object Operations (16)
1. PutObject
2. GetObject
3. HeadObject
4. DeleteObject
5. DeleteObjects
6. CopyObject
7. ListObjects
8. ListObjectsV2
9. ListObjectVersions
10. GetObjectAcl
11. PutObjectAcl
12. GetObjectTagging
13. PutObjectTagging
14. DeleteObjectTagging
15. GetObjectAttributes
16. RenameObject

### Object Lock Operations (6)
1. GetObjectLegalHold
2. PutObjectLegalHold
3. GetObjectRetention
4. PutObjectRetention
5. GetObjectLockConfiguration
6. PutObjectLockConfiguration

### Multipart Upload Operations (7)
1. CreateMultipartUpload
2. UploadPart
3. UploadPartCopy
4. CompleteMultipartUpload
5. AbortMultipartUpload
6. ListParts
7. ListMultipartUploads

### Config-Storage-Only Operations (9)
*API compatible but no actual functionality*
1. GetBucketLifecycle (deprecated)
2. PutBucketLifecycle (deprecated)
3. GetBucketLogging
4. PutBucketLogging
5. GetBucketNotificationConfiguration
6. PutBucketNotificationConfiguration
7. GetBucketReplication
8. PutBucketReplication
9. DeleteBucketReplication
10. GetBucketRequestPayment
11. PutBucketRequestPayment
12. SelectObjectContent

---

## Notes

### Version-Specific Operations
The following operations support version-specific access via the `versionId` query parameter. These are NOT separate API actions - they use the same API endpoint with an optional parameter:
- GetObject (with versionId)
- DeleteObject (with versionId)
- GetObjectAcl (with versionId)
- GetObjectTagging (with versionId)
- PutObjectTagging (with versionId)
- DeleteObjectTagging (with versionId)
- GetObjectAttributes (with versionId)

### IAM Permission Actions (Not API Endpoints)
The following are IAM permission actions used for access control, NOT API endpoints:
- BypassGovernanceRetention
- ObjectOwnerOverrideToBucketOwner
- GetObjectVersion, GetObjectVersionAcl, etc. (use base operations with versionId)

---

## Testing Checklist

For each implemented action, ensure:

- [x] Unit tests pass
- [x] Integration tests pass (AWS SDK tests)
- [x] AWS SDK compatibility verified (including checksum headers)
- [x] SigV4 authentication verified
- [x] Error cases handled
- [x] XML response format correct
- [x] Headers correct (ETag, Content-Type, etc.)

### Test Results

All P0, P1, P2, and P3 features have been tested and validated:
- **327 unit tests** - All passing
- **119 AWS SDK integration tests** - All passing (1 auth test requires LOCAL_S3_REQUIRE_AUTH=true, 4 encryption tests require LOCAL_S3_ENCRYPTION_KEY)
- **SigV4 Authentication** - Configurable via `LOCAL_S3_REQUIRE_AUTH` environment variable
- **Checksum Headers** - Full support for AWS SDK's default `x-amz-checksum-*` headers
- **Range Requests** - Full support for partial content retrieval (HTTP 206)
- **Conditional Requests** - If-Match (412), If-None-Match (304, 412 for PUT)
- **Pre-signed URLs** - Full support for GET, PUT, DELETE, HEAD with response header overrides

### SigV4 Authentication Details

The implementation correctly handles:
- AWS SDK Rust's default checksum headers (`x-amz-checksum-crc32`, `x-amz-sdk-checksum-algorithm`)
- Query parameters with empty values (e.g., `?tagging` -> `tagging=` in canonical request)
- Multi-value headers (combined with commas per SigV4 spec)
- Pre-signed URL authentication via query string
- Response header overrides (response-content-type, response-content-disposition, etc.)

### Integration Test Coverage

| Category | Tests | Status | Notes |
|----------|-------|--------|-------|
| Bucket Operations | 8 | ✅ | Create, delete, head, list, location, non-empty delete |
| Object Operations | 12 | ✅ | CRUD, copy, list (v1/v2), pagination, delimiter |
| Multipart Upload | 8 | ✅ | Create, upload parts, complete, abort, list parts/uploads |
| Versioning | 8 | ✅ | Enable/suspend, put versions, delete markers, list versions |
| Tagging | 4 | ✅ | Bucket and object tagging with versioning |
| ACLs | 6 | ✅ | Bucket and object ACLs, canned ACLs |
| CORS/Policy | 3 | ✅ | CORS configuration, bucket policy, policy status |
| Lifecycle | 3 | ✅ | Put/Get/Delete lifecycle configuration, multiple rules |
| Range Requests | 2 | ✅ | Full support for partial content (HTTP 206) |
| Conditional Requests | 3 | ✅ | If-Match, If-None-Match for GET and PUT |
| Pre-signed URLs | 9 | ✅ | GET, PUT, DELETE, HEAD, response headers, multipart, expiry, invalid sig |
| Authentication | 3 | ✅ | Require LOCAL_S3_REQUIRE_AUTH=true |
| Workflow Tests | 5 | ✅ | Full lifecycle, multipart+versioning, concurrent uploads, batch delete |
| Error Handling | 3 | ✅ | 404, bucket not found, non-empty bucket |
| Object Lock | 3 | ✅ | Legal hold, retention, compliance mode |
| Encryption | 5 | ✅ | Bucket encryption config (Get/Put/Delete), object SSE headers |
| Public Access Block | 4 | ✅ | Put/Get/Delete configuration, no config error |
| Bucket Website | 5 | ✅ | Put/Get/Delete, redirect, routing rules |
| Ownership Controls | 5 | ✅ | Put/Get/Delete, all ObjectOwnership values |
| Rename Object | 7 | ✅ | Basic rename, URL encoding, overwrites, content-type preservation |
| Bucket Logging | 4 | ✅ | Put/Get, enable/disable, bucket not found |
| Bucket Notifications | 5 | ✅ | Put/Get, topic/queue/lambda, clear config |
| Bucket Replication | 4 | ✅ | Put/Get/Delete, multiple rules |
| Request Payment | 4 | ✅ | Put/Get, Requester/BucketOwner, default value |
| S3 Select | 1 | ✅ | SQL query on CSV/JSON |

**Note**: All 119 integration tests pass (110 with default config). The 1 auth test requires `LOCAL_S3_REQUIRE_AUTH=true` to validate authentication rejection. The 4 encryption tests require `LOCAL_S3_ENCRYPTION_KEY` to be set.
