import os

import boto3
import pytest
import transaction
from moto import mock_aws
from ZODB.MappingStorage import MappingStorage
from ZODB.utils import p64

import ZODB.interfaces
from zodb_s3blobs.cache import S3BlobCache
from zodb_s3blobs.s3client import S3Client
from zodb_s3blobs.storage import S3BlobStorage


@pytest.fixture
def s3_env():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(
            Bucket="test-bucket"
        )
        yield


@pytest.fixture
def base_storage():
    return MappingStorage()


@pytest.fixture
def s3_client(s3_env):
    return S3Client(bucket_name="test-bucket", region_name="us-east-1")


@pytest.fixture
def blob_cache(tmp_path):
    return S3BlobCache(str(tmp_path / "cache"), max_size=10 * 1024 * 1024)


@pytest.fixture
def storage(base_storage, s3_client, blob_cache, tmp_path):
    return S3BlobStorage(
        base_storage, s3_client, blob_cache, temp_dir=str(tmp_path / "staging")
    )


def _make_blob_file(tmp_path, content=b"blob content"):
    """Create a temp blob file and return its path."""
    p = tmp_path / f"blob_{id(content)}.bin"
    p.write_bytes(content)
    return str(p)


class TestProxy:
    def test_getattr_delegates_to_base(self, storage, base_storage):
        assert storage.sortKey() == base_storage.sortKey()

    def test_implements_iblobstorage(self, storage):
        assert ZODB.interfaces.IBlobStorage.providedBy(storage)

    def test_len(self, storage):
        assert len(storage) == len(MappingStorage())

    def test_repr(self, storage):
        r = repr(storage)
        assert "S3BlobStorage" in r
        assert "proxy" in r.lower()

    def test_temporary_directory(self, storage, tmp_path):
        td = storage.temporaryDirectory()
        assert os.path.isdir(td)


class TestStoreBlob:
    def test_store_blob_stages_file(self, storage, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)

        # Blob should be staged (original file consumed)
        assert not os.path.exists(blob_path)

    def test_store_blob_calls_base_store(self, storage, base_storage, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)

        # After tpc_vote + tpc_finish, the object data should be in base storage
        storage.tpc_vote(txn)
        storage.tpc_finish(txn)

        data, tid = base_storage.load(oid)
        assert data == b"pickle"


class TestTwoPhaseCommit:
    def test_tpc_vote_uploads_to_s3(self, storage, s3_client, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path, b"vote test data")
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)

        # After vote, blob should be in S3
        keys = list(s3_client.list_objects("blobs/"))
        assert len(keys) == 1
        assert keys[0].endswith(".blob")

        storage.tpc_finish(txn)

    def test_tpc_finish_populates_cache(self, storage, blob_cache, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path, b"cache after finish")
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)

        # After finish, blob should be in cache
        cached = blob_cache.get(oid, tid)
        assert cached is not None
        with open(cached, "rb") as f:
            assert f.read() == b"cache after finish"

    def test_tpc_finish_clears_pending(self, storage, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        storage.tpc_finish(txn)

        assert storage._pending_blobs == {}
        assert storage._uploaded_keys == []

    def test_tpc_finish_returns_tid(self, storage, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)

        assert tid is not None
        assert isinstance(tid, bytes)
        assert len(tid) == 8

    def test_tpc_abort_deletes_s3_keys(self, storage, s3_client, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path, b"abort test")
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)

        # Before abort, S3 should have the blob
        keys_before = list(s3_client.list_objects("blobs/"))
        assert len(keys_before) == 1

        storage.tpc_abort(txn)

        # After abort, S3 should be clean
        keys_after = list(s3_client.list_objects("blobs/"))
        assert len(keys_after) == 0

    def test_tpc_abort_cleans_staged_files(self, storage, tmp_path):
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)

        # Abort before vote (no S3 uploads yet)
        storage.tpc_abort(txn)
        assert storage._pending_blobs == {}

    def test_tpc_abort_best_effort_s3(self, storage, s3_client, tmp_path):
        """S3 delete failure during abort should not raise."""
        oid = p64(1)
        blob_path = _make_blob_file(tmp_path)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)

        # Monkey-patch delete to fail
        original_delete = s3_client.delete_object
        s3_client.delete_object = lambda key: (_ for _ in ()).throw(
            Exception("S3 down")
        )

        # Abort should not raise despite S3 failure
        storage.tpc_abort(txn)
        assert storage._pending_blobs == {}
        assert storage._uploaded_keys == []

        s3_client.delete_object = original_delete

    def test_multiple_blobs_single_transaction(self, storage, s3_client, tmp_path):
        txn = transaction.get()
        storage.tpc_begin(txn)

        for i in range(3):
            oid = p64(i + 1)
            blob_path = _make_blob_file(tmp_path, f"blob {i}".encode())
            storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)

        storage.tpc_vote(txn)

        keys = list(s3_client.list_objects("blobs/"))
        assert len(keys) == 3

        storage.tpc_finish(txn)


class TestLoadBlob:
    def _store_and_commit(self, storage, oid, blob_content, tmp_path):
        """Helper: store a blob and commit, return tid."""
        blob_path = _make_blob_file(tmp_path, blob_content)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        return storage.tpc_finish(txn)

    def test_load_blob_from_cache(self, storage, blob_cache, tmp_path):
        oid = p64(1)
        tid = self._store_and_commit(storage, oid, b"cached blob", tmp_path)

        # Should be in cache already (from tpc_finish)
        result = storage.loadBlob(oid, tid)
        assert os.path.exists(result)
        with open(result, "rb") as f:
            assert f.read() == b"cached blob"

    def test_load_blob_from_s3(self, storage, blob_cache, s3_client, tmp_path):
        oid = p64(1)
        tid = self._store_and_commit(storage, oid, b"s3 blob", tmp_path)

        # Remove from cache to force S3 download
        cached = blob_cache.get(oid, tid)
        if cached and os.path.exists(cached):
            os.remove(cached)

        result = storage.loadBlob(oid, tid)
        assert os.path.exists(result)
        with open(result, "rb") as f:
            assert f.read() == b"s3 blob"

    def test_load_blob_not_found(self, storage):
        from ZODB.POSException import POSKeyError

        with pytest.raises(POSKeyError):
            storage.loadBlob(p64(999), p64(999))

    def test_open_committed_blob_file(self, storage, tmp_path):
        oid = p64(1)
        tid = self._store_and_commit(storage, oid, b"open test", tmp_path)

        f = storage.openCommittedBlobFile(oid, tid)
        try:
            assert f.read() == b"open test"
        finally:
            f.close()

    def test_open_committed_blob_file_with_blob(self, storage, tmp_path):
        from ZODB.blob import Blob

        oid = p64(1)
        tid = self._store_and_commit(storage, oid, b"blob file", tmp_path)

        blob = Blob()
        f = storage.openCommittedBlobFile(oid, tid, blob=blob)
        try:
            assert f.read() == b"blob file"
        finally:
            f.close()


class TestNewInstance:
    def test_new_instance_shares_s3_and_cache(self, storage, s3_client, blob_cache):
        new = storage.new_instance()
        assert new._s3_client is s3_client
        assert new._cache is blob_cache

    def test_new_instance_returns_different_wrapper(self, storage):
        new = storage.new_instance()
        assert new is not storage
        assert isinstance(new, S3BlobStorage)


class TestPack:
    def _store_blob_and_commit(self, storage, oid, blob_content, tmp_path):
        blob_path = _make_blob_file(tmp_path, blob_content)
        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.storeBlob(oid, p64(0), b"pickle", blob_path, "", txn)
        storage.tpc_vote(txn)
        return storage.tpc_finish(txn)

    def _store_root(self, storage):
        """Store a root object (oid 0) required for MappingStorage GC pack."""
        from ZODB.utils import z64

        txn = transaction.get()
        storage.tpc_begin(txn)
        storage.store(z64, z64, b"root", "", txn)
        storage.tpc_vote(txn)
        storage.tpc_finish(txn)

    def test_pack_delegates_to_base(self, storage, tmp_path):
        import time

        self._store_root(storage)
        oid = p64(1)
        self._store_blob_and_commit(storage, oid, b"pack test", tmp_path)

        # Pack should not raise
        storage.pack(time.time(), lambda p: [])

    def test_pack_keeps_reachable_blobs(self, storage, s3_client, tmp_path):
        import time

        self._store_root(storage)
        oid = p64(1)
        self._store_blob_and_commit(storage, oid, b"keep me", tmp_path)

        # referencesf that returns oid=1 from root, so it stays reachable
        def referencesf(pickle):
            return [p64(1)]

        storage.pack(time.time(), referencesf)

        keys_after = list(s3_client.list_objects("blobs/"))
        assert len(keys_after) == 1  # Blob still there

    def test_pack_gc_cleans_orphaned_keys(self, storage, s3_client, tmp_path):
        """Test S3 GC by manually placing an orphan key in S3."""
        import time

        self._store_root(storage)

        # Manually upload an orphan blob to S3 (oid 999 doesn't exist in base)
        orphan_src = _make_blob_file(tmp_path, b"orphan")
        s3_client.upload_file(orphan_src, "blobs/3e7/1.blob")

        keys_before = list(s3_client.list_objects("blobs/"))
        assert len(keys_before) == 1

        storage.pack(time.time(), lambda p: [])

        # Orphan should be removed by GC
        keys_after = list(s3_client.list_objects("blobs/"))
        assert len(keys_after) == 0


class TestClose:
    def test_close(self, storage, base_storage):
        storage.close()
        # MappingStorage.close() sets _is_open to False
        # After closing, operations should fail
        assert not base_storage.opened()
