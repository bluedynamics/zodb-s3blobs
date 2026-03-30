# Changelog

## 1.1.0

- Add `multipart_threshold` parameter (default: 5 GB) to disable multipart
  uploads. Hetzner S3 still returns intermittent `400 Bad Request` on
  `UploadPart` despite the checksum fix in 1.0.6. Single PUT requests up to
  5 GB (S3 limit) are reliable on all providers. Fixes #13.

## 1.0.7

- Increase default `read_timeout` from 60 to 300 seconds. The 60s timeout
  caused false retries during concurrent uploads — botocore interpreted slow
  S3 responses as timeouts and retried, making every affected upload take
  exactly ~60 seconds regardless of blob size.

## 1.0.6

- Fix S3 multipart uploads failing on Ceph-based providers (Hetzner,
  DigitalOcean Spaces, etc.) with `400 Bad Request`. Root cause: boto3
  >= 1.36.0 sends CRC checksum headers that non-AWS backends reject.
  Fixed by setting `request_checksum_calculation="when_required"` in the
  botocore Config. Multipart uploads now work correctly on all providers.
- Remove `multipart_threshold` parameter and `S3_MULTIPART_THRESHOLD`
  env var (workaround no longer needed).
- Add `s3_max_concurrency` parameter (default: 1) to control parallel
  part upload threads per file.
- Require `boto3 >= 1.36.0`, `s3transfer >= 0.11.2`.
- Fix `AttributeError: 'RelStorage' object has no attribute '_tid'` when using
  RelStorage as base storage (#8). S3BlobStorage now extracts the TID from
  RelStorage's internal TPC phase object and forces `LOCK_EARLY` so the TID is
  available during `tpc_vote` for S3 key construction.

## 1.0.5

- Add `multipart_threshold` parameter to `S3Client` (default: 500 MB).
  Hetzner Object Storage (and some other S3-compatible providers) return
  400 Bad Request on `UploadPart` operations. The high default avoids
  multipart uploads for typical blob sizes. AWS users who want multipart
  for large files can lower the threshold.

## 1.0.4

- Increase S3 `max_pool_connections` from 10 to 50 to prevent connection pool
  exhaustion during parallel blob migrations (boto3 multipart uploads use up to
  10 threads per upload).

## 1.0.3

Security review fixes (addresses #6):

- **S3-H1:** Restrict cache subdirectory permissions to `0o700` (cache and s3client).
- **S3-H2:** Fix TOCTOU race in cache `get()` — use atomic `os.utime()` instead of `os.path.exists()`.
- **S3-H3:** Document AWS SSE-C deprecation (April 2026) in README and ZConfig schema.
- **S3-M1:** Validate `s3-prefix` against safe character set; reject `..` path traversal.
- **S3-M2:** Document SSE-C key memory lifetime limitation.
- **S3-M3:** Add `close()` method to `S3BlobCache`; call it from `S3BlobStorage.close()`.
- **S3-M4:** Strict regex validation in `_oid_from_key()` for GC key parsing.
- **S3-M5:** Wrap boto3 `ClientError` in `S3OperationError` to avoid leaking infrastructure details.
- **S3-L1:** Document reproducible deployment lockfile workflow.
- **S3-L2:** Add `pip-audit` dependency scanning to CI.
- **S3-L3:** Already addressed — `connect_timeout` and `read_timeout` configurable since 1.0.0.


## 1.0.2

- Security hardening: restrict temp and cache directory permissions to `0o700`.
- Fix `_oid_from_key` crash on oversized hex values during `pack()`.
- Keep S3 object listing lazy in `pack()` GC to avoid memory issues with large buckets.
- Clean up temp directory on `close()` to prevent disk space leakage.


## 1.0.1

- Fix `loadBlob` to check pending (in-transaction) blobs before S3/cache, preventing `POSKeyError` during savepoint commits.


## 1.0.0

- Initial release.
- Wraps any ZODB base storage to store blobs in S3-compatible object storage.
- Local LRU filesystem cache with background eviction.
- Full ZODB two-phase commit integration (upload in `tpc_vote`, no S3 ops in `tpc_finish`).
- MVCC support via `new_instance()`.
- Garbage collection of orphaned S3 objects during `pack()`.
- ZConfig integration (`<s3blobstorage>` section) with environment variable substitution.
- SSE-C (Server-Side Encryption with Customer-Provided Keys) support.
- Works with AWS S3, MinIO, Ceph, DigitalOcean Spaces, Hetzner Object Storage.
