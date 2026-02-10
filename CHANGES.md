# Changelog

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
