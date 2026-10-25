# zodb-s3blobs

Store ZODB blobs in S3-compatible object storage.

## Features

- Wraps any ZODB base storage (FileStorage, RelStorage, MappingStorage, ...)
- Works with any S3-compatible service (AWS S3, MinIO, Ceph, DigitalOcean Spaces)
- Local LRU filesystem cache for fast reads
- Full ZODB two-phase commit integration (transactional safety)
- ZConfig integration for `zope.conf` configuration
- Provides `IMVCCStorage` when the base storage does (RelStorage), so every connection gets its own instance
- Garbage collection during `pack()`: blobs of unreachable objects and of superseded revisions

## Installation

```bash
pip install zodb-s3blobs
```

## Configuration via zope.conf

Add `%import zodb_s3blobs` and use the `<s3blobstorage>` section wrapping any base storage.

### With FileStorage

```xml
%import zodb_s3blobs

<zodb_db main>
    <s3blobstorage>
        bucket-name my-zodb-blobs
        s3-endpoint-url http://minio:9000
        s3-access-key $(S3_ACCESS_KEY)
        s3-secret-key $(S3_SECRET_KEY)
        cache-dir /var/cache/zodb-s3-blobs
        cache-size 2GB
        <filestorage>
            path /var/lib/zodb/Data.fs
        </filestorage>
    </s3blobstorage>
</zodb_db>
```

ZConfig expands `$(VARIABLE)` from the process environment.
Plain `$VARIABLE` and `${VARIABLE}` only expand names defined with `%define`;
an undefined name fails at startup with `SubstitutionReplacementError`.
For production, consider omitting `s3-access-key` and `s3-secret-key` entirely
and relying on the boto3 credential chain (IAM roles, instance profiles,
`~/.aws/credentials`, or the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
environment variables).

### With RelStorage

When wrapping RelStorage, `zodb-s3blobs` overrides RelStorage's blob handling.
Blobs go to S3 instead of the `blob_chunk` table. RelStorage still handles object data (pickles) in the RDBMS.

```xml
%import zodb_s3blobs

<zodb_db main>
    <s3blobstorage>
        bucket-name my-zodb-blobs
        cache-dir /var/cache/zodb-s3-blobs
        cache-size 2GB
        <relstorage>
            <postgresql>
                dsn dbname='zodb' user='zodb' host='localhost'
            </postgresql>
        </relstorage>
    </s3blobstorage>
</zodb_db>
```

**Note:** When wrapping RelStorage, `zodb-s3blobs` automatically enables
RelStorage's `LOCK_EARLY` mode (equivalent to `RELSTORAGE_LOCK_EARLY=1`). This
causes the database commit lock to be held slightly longer — through the S3 blob
upload — to ensure transactional consistency. This is the same behavior as
RelStorage 2.x and is required so that blob uploads complete before the
transaction commits.

`S3BlobStorage` provides `IMVCCStorage` when RelStorage does. ZODB then gives
every connection its own `S3BlobStorage` instance on top of its own RelStorage
instance and polls it for invalidations, so each process sees the commits of the
others.

## Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bucket-name` | *(required)* | S3 bucket name |
| `s3-prefix` | `""` | Key prefix in bucket |
| `s3-endpoint-url` | `None` | For MinIO, Ceph, etc. |
| `s3-region` | `None` | AWS region |
| `s3-access-key` | `None` | Uses boto3 credential chain if omitted. Use `$(ENV_VAR)` substitution — never hardcode credentials. |
| `s3-secret-key` | `None` | Uses boto3 credential chain if omitted. Use `$(ENV_VAR)` substitution — never hardcode credentials. |
| `s3-use-ssl` | `true` | Whether to use SSL for S3 connections |
| `s3-addressing-style` | `auto` | S3 addressing style: `path`, `virtual`, or `auto` |
| `s3-sse-customer-key` | `None` | Base64-encoded 256-bit key for SSE-C encryption. Requires SSL. |
| `s3-connect-timeout` | `60` | Connect timeout (seconds) for uploads, deletes and listings |
| `s3-read-timeout` | `300` | Socket read timeout (seconds) for uploads, deletes and listings |
| `s3-download-connect-timeout` | `5` | Connect timeout (seconds) for downloads and HEAD requests |
| `s3-download-read-timeout` | `10` | Socket read timeout (seconds) for downloads and HEAD requests |
| `s3-download-max-attempts` | `2` | Total attempts per download and HEAD request (botocore standard retry mode) |
| `cache-dir` | *(required)* | Local cache directory path |
| `cache-size` | `1GB` | Maximum local cache size |

## How It Works

`zodb-s3blobs` uses the same proxy/wrapper pattern as ZODB's built-in `BlobStorage`. It wraps any base storage via `__getattr__` and explicitly overrides all blob methods so they always take precedence.

### Two-Phase Commit Flow

1. **`storeBlob`**: Object data (pickle) is stored in the base storage. The blob file is staged locally.
2. **`tpc_vote`**: Staged blobs are uploaded to S3. If any upload fails, the transaction aborts cleanly.
3. **`tpc_finish`**: No S3 operations (this method must not fail per ZODB contract). Staged files are moved into the local cache.
4. **`tpc_abort`**: Uploaded S3 objects are deleted (best-effort). Local staged files are cleaned up.

### S3 Key Layout

```
blobs/{oid_hex}/{tid_hex}.blob
```

With a configured prefix: `{prefix}/blobs/{oid_hex}/{tid_hex}.blob`

### Local Cache

The local filesystem cache provides fast reads after the first access. It uses LRU eviction with a background daemon thread that removes the oldest files (by access time) when the total size exceeds the configured maximum. The cache is required -- S3 latency makes direct access impractical for ZODB's synchronous access patterns.

### Timeouts and Retries

Uploads and reads use separate S3 clients with separate settings.

- **Uploads** (and deletes, listings) run during `tpc_vote` or `pack()`. They keep long timeouts because some providers (Hetzner Object Storage) answer PUT requests under concurrent load only after more than 60 seconds.
- **Downloads** (and HEAD requests) run when a blob is not in the local cache, which usually means inside a web request. They use short timeouts so that a stalled connection is abandoned quickly and the retry runs on a fresh one.

The read timeout is an inactivity timeout per socket operation, not a limit on the total download time: a download that keeps receiving data never hits it. Retries also nest. s3transfer restarts a failed GET once (two attempts), and botocore makes up to `s3-download-max-attempts` attempts inside each of those. When every attempt stalls, a download gives up after roughly

```
2 × s3-download-max-attempts × (s3-download-connect-timeout + s3-download-read-timeout)
```

plus a retry backoff of up to one second per botocore retry. Objects larger than 8 MB are fetched in 8 MB ranged GETs, and this bound applies to each of them. With the defaults a stalled connection costs about 10 seconds, and four stalls in a row about 40 seconds.

Keep the worst case below any timeout that applies to the whole request. zodb-pgjsonb, for example, holds a PostgreSQL transaction open during the request and terminates it after `idle_in_transaction_session_timeout` (60 seconds by default). A cold-cache download that takes longer in total fails there, whatever the S3 timeouts are. A wall-clock deadline for downloads is tracked in [#26](https://github.com/bluedynamics/zodb-s3blobs/issues/26).

### Garbage Collection

During `pack()`, the base storage is packed first. Then every S3 key whose revision (`oid`, `tid`) the base storage no longer has is deleted: blobs of unreachable objects, of revisions superseded before the pack time, and of aborted transactions whose cleanup failed. History-preserving storages keep the revision current at the pack time and any later ones, so those blobs stay too. Keys newer than the last committed transaction are left alone, as they may belong to a transaction that is still committing.

Versions before 1.2.0 never deleted the blobs of superseded revisions, so the first `pack()` after upgrading can delete a large number of S3 objects at once. Enable bucket versioning or take a backup before that first pack if you want a way back.

### S3 Bucket Security

Ensure your S3 bucket has appropriate access controls (Block Public Access enabled, restrictive bucket policy). The minimum IAM policy required by `zodb-s3blobs`:

```json
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket"
        ],
        "Resource": [
            "arn:aws:s3:::BUCKET_NAME",
            "arn:aws:s3:::BUCKET_NAME/*"
        ]
    }]
}
```

### Encryption at Rest (SSE-C)

`zodb-s3blobs` supports SSE-C (Server-Side Encryption with Customer-Provided Keys).
The S3 service encrypts/decrypts data using your key but never stores it.
Works with AWS S3, Hetzner Object Storage, MinIO (with KES), and other S3-compatible services.

> **Warning — AWS SSE-C deprecation (April 2026):** AWS will disable SSE-C by default
> on new S3 buckets starting April 2026. Existing buckets are unaffected. For new buckets,
> you must explicitly enable SSE-C in the bucket policy, or consider migrating to SSE-KMS.
> If you receive 403 errors with SSE-C configured, this is the likely cause.
> See the [AWS announcement](https://aws.amazon.com/blogs/storage/advanced-notice-amazon-s3-to-disable-the-use-of-sse-c-encryption-by-default-for-all-new-buckets-and-select-existing-buckets-in-april-2026/) for details.

Generate a 256-bit key:

```bash
python -c "import base64, os; print(base64.b64encode(os.urandom(32)).decode())"
```

Configure via environment variable:

```xml
s3-sse-customer-key $(S3_SSE_KEY)
```

**Important:** If you lose the key, encrypted data is irrecoverable. SSL is required (enforced at startup).

**Security note:** The SSE-C key is held in process memory for the lifetime of the storage instance. In long-running servers, consider using IAM-based encryption (SSE-KMS) instead if memory exposure is a concern. Python's string handling makes secure memory clearing impractical.

## Using with MinIO (dev setup)

**Warning:** The credentials below are MinIO defaults for local development only. Never use default credentials in production.

```yaml
# docker-compose.yml
services:
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
```

Create the bucket:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/zodb-blobs
```

## Development

```bash
git clone https://github.com/bluedynamics/zodb-s3blobs.git
cd zodb-s3blobs
uv venv
uv pip install -e ".[test]"
pytest
```

For **reproducible deployments** (production), pin dependencies with a lockfile:

```bash
uv pip compile pyproject.toml -o requirements.txt
uv pip install -r requirements.txt
```

## Source Code and Contributions

The source code is managed in a Git repository, with its main branches hosted on GitHub.
Issues can be reported there too.

We'd be happy to see many forks and pull requests to make this package even better.
We welcome AI-assisted contributions, but expect every contributor to fully understand and be able to explain the code they submit.
Please don't send bulk auto-generated pull requests.

Maintainers are Jens Klein and the BlueDynamics Alliance developer team.
We appreciate any contribution and if a release on PyPI is needed, please just contact one of us.
We also offer commercial support if any training, coaching, integration or adaptations are needed.

## License

ZPL-2.1
