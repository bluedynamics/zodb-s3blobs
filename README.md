# zodb-s3blobs

Store ZODB blobs in S3-compatible object storage.

## Features

- Wraps any ZODB base storage (FileStorage, RelStorage, MappingStorage, ...)
- Works with any S3-compatible service (AWS S3, MinIO, Ceph, DigitalOcean Spaces)
- Local LRU filesystem cache for fast reads
- Full ZODB two-phase commit integration (transactional safety)
- ZConfig integration for `zope.conf` configuration
- Supports MVCC storages (`new_instance()`)
- Garbage collection of orphaned S3 objects during `pack()`

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
        s3-access-key $S3_ACCESS_KEY
        s3-secret-key $S3_SECRET_KEY
        cache-dir /var/cache/zodb-s3-blobs
        cache-size 2GB
        <filestorage>
            path /var/lib/zodb/Data.fs
        </filestorage>
    </s3blobstorage>
</zodb_db>
```

ZConfig expands `$VARIABLE` and `${VARIABLE}` from the process environment.
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

## Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bucket-name` | *(required)* | S3 bucket name |
| `s3-prefix` | `""` | Key prefix in bucket |
| `s3-endpoint-url` | `None` | For MinIO, Ceph, etc. |
| `s3-region` | `None` | AWS region |
| `s3-access-key` | `None` | Uses boto3 credential chain if omitted. Use `$ENV_VAR` substitution — never hardcode credentials. |
| `s3-secret-key` | `None` | Uses boto3 credential chain if omitted. Use `$ENV_VAR` substitution — never hardcode credentials. |
| `s3-use-ssl` | `true` | Whether to use SSL for S3 connections |
| `s3-addressing-style` | `auto` | S3 addressing style: `path`, `virtual`, or `auto` |
| `s3-sse-customer-key` | `None` | Base64-encoded 256-bit key for SSE-C encryption. Requires SSL. |
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

### Garbage Collection

During `pack()`, the base storage is packed first, then S3 is scanned for keys referencing OIDs that are no longer reachable. Orphaned keys are deleted. This also cleans up any objects left behind by failed abort operations.

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

Generate a 256-bit key:

```bash
python -c "import base64, os; print(base64.b64encode(os.urandom(32)).decode())"
```

Configure via environment variable:

```xml
s3-sse-customer-key $S3_SSE_KEY
```

**Important:** If you lose the key, encrypted data is irrecoverable. SSL is required (enforced at startup).

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

## License

ZPL-2.1
