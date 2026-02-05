"""End-to-end integration tests using ZODB + moto S3."""

from moto import mock_aws
from ZODB.MappingStorage import MappingStorage
from ZODB.utils import p64
from zodb_s3blobs.cache import S3BlobCache
from zodb_s3blobs.s3client import S3Client
from zodb_s3blobs.storage import S3BlobStorage

import boto3
import os
import pytest
import transaction
import ZODB


@pytest.fixture
def s3_env():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


@pytest.fixture
def s3_client(s3_env):
    return S3Client(bucket_name="test-bucket", region_name="us-east-1")


@pytest.fixture
def blob_cache(tmp_path):
    return S3BlobCache(str(tmp_path / "cache"), max_size=10 * 1024 * 1024)


@pytest.fixture
def storage(s3_client, blob_cache, tmp_path):
    base = MappingStorage()
    return S3BlobStorage(
        base, s3_client, blob_cache, temp_dir=str(tmp_path / "staging")
    )


@pytest.fixture
def db(storage):
    database = ZODB.DB(storage)
    yield database
    database.close()


def _write_blob_file(tmp_path, name, content):
    p = tmp_path / name
    p.write_bytes(content)
    return str(p)


class TestStoreAndLoadRoundtrip:
    def test_store_and_load_via_low_level(self, storage, s3_client, tmp_path):
        """Full low-level 2PC: store blob, commit, load, verify content."""
        oid = p64(1)
        blob_content = b"integration test blob content"
        blob_path = _write_blob_file(tmp_path, "blob.bin", blob_content)

        # Store
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)

        # Load
        result_path = storage.loadBlob(oid, tid)
        assert os.path.exists(result_path)
        with open(result_path, "rb") as f:
            assert f.read() == blob_content

        # Verify S3 has the blob
        keys = list(s3_client.list_objects("blobs/"))
        assert len(keys) == 1


class TestAbortCleanup:
    def test_abort_cleans_s3(self, storage, s3_client, tmp_path):
        """Store blob, vote (uploads to S3), abort -> S3 should be clean."""
        oid = p64(1)
        blob_path = _write_blob_file(tmp_path, "abort.bin", b"abort me")

        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)

        # Verify blob was uploaded during vote
        keys_during = list(s3_client.list_objects("blobs/"))
        assert len(keys_during) == 1

        # Abort
        storage.tpc_abort(txn)

        # S3 should be clean
        keys_after = list(s3_client.list_objects("blobs/"))
        assert len(keys_after) == 0


class TestCacheBehavior:
    def test_cache_miss_then_hit(self, storage, blob_cache, s3_client, tmp_path):
        """First load is cache miss (downloads from S3), second is cache hit."""
        oid = p64(1)
        blob_path = _write_blob_file(tmp_path, "cache.bin", b"cache test")

        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)

        # Remove from cache to simulate cache miss
        cached = blob_cache.get(oid, tid)
        if cached:
            os.remove(cached)

        # First load: cache miss -> S3 download
        path1 = storage.loadBlob(oid, tid)
        assert os.path.exists(path1)

        # Second load: cache hit
        path2 = storage.loadBlob(oid, tid)
        assert path2 == path1  # Same cached path


class TestCacheEviction:
    def test_cache_stays_bounded(self, s3_client, tmp_path):
        """With small cache, many blobs should trigger eviction."""
        small_cache = S3BlobCache(str(tmp_path / "cache"), max_size=500)
        base = MappingStorage()
        store = S3BlobStorage(
            base, s3_client, small_cache, temp_dir=str(tmp_path / "staging")
        )

        for i in range(10):
            oid = p64(i + 1)
            blob_path = _write_blob_file(tmp_path, f"evict{i}.bin", b"x" * 200)
            txn = transaction.get()
            store.tpc_begin(txn)
            store.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
            store.tpc_vote(txn)
            store.tpc_finish(txn)

        small_cache.wait_for_cleanup()
        assert small_cache.current_size() <= small_cache.max_size


class TestMultipleBlobsTransaction:
    def test_multiple_blobs_single_transaction(self, storage, s3_client, tmp_path):
        """Store 3 blobs in one transaction."""
        txn = transaction.get()
        storage.tpc_begin(txn)

        for i in range(3):
            oid = p64(i + 1)
            content = f"blob {i}".encode()
            blob_path = _write_blob_file(tmp_path, f"multi{i}.bin", content)
            storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)

        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)

        # All 3 should be in S3
        keys = list(s3_client.list_objects("blobs/"))
        assert len(keys) == 3

        # All 3 should be loadable
        for i in range(3):
            path = storage.loadBlob(p64(i + 1), tid)
            with open(path, "rb") as f:
                assert f.read() == f"blob {i}".encode()


class TestMVCC:
    def test_new_instance_works(self, storage, s3_client, tmp_path):
        """new_instance returns a working storage that shares S3/cache."""
        oid = p64(1)
        blob_path = _write_blob_file(tmp_path, "mvcc.bin", b"mvcc test")

        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)

        # Create new instance and load the blob
        new_storage = storage.new_instance()
        path = new_storage.loadBlob(oid, tid)
        with open(path, "rb") as f:
            assert f.read() == b"mvcc test"
