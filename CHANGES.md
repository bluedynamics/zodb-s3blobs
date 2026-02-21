# Changelog

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
