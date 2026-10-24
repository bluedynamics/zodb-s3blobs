"""Pack / GC against RelStorage (SQLite), with two storages on one database
standing in for two processes."""

from moto import mock_aws
from ZODB.Connection import TransactionMetaData
from ZODB.utils import p64
from ZODB.utils import z64
from zodb_s3blobs.cache import S3BlobCache
from zodb_s3blobs.s3client import S3Client
from zodb_s3blobs.storage import _oid_hex
from zodb_s3blobs.storage import _tid_hex
from zodb_s3blobs.storage import S3BlobStorage

import boto3
import pytest
import time


pytest.importorskip("relstorage")

from relstorage.adapters.sqlite.adapter import Sqlite3Adapter
from relstorage.options import Options
from relstorage.storage import RelStorage


@pytest.fixture
def s3_client():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield S3Client(bucket_name="test-bucket", region_name="us-east-1")


@pytest.fixture(params=[False, True], ids=["history-free", "history-preserving"])
def make_storage(request, s3_client, tmp_path):
    storages = []

    def make(name):
        # pack_gc=False: the test pickles aren't real, so pack can't follow
        # references.
        options = Options(keep_history=request.param, pack_gc=False)
        adapter = Sqlite3Adapter(
            data_dir=str(tmp_path / "db"), pragmas={}, options=options
        )
        storage = S3BlobStorage(
            RelStorage(adapter, name=name, options=options),
            s3_client,
            S3BlobCache(str(tmp_path / f"{name}-cache"), max_size=10 * 1024 * 1024),
            temp_dir=str(tmp_path / f"{name}-staging"),
        )
        storages.append(storage)
        return storage

    yield make
    for storage in storages:
        storage.close()


def _commit_blob(storage, oid, serial, tmp_path):
    path = tmp_path / "blob.bin"
    path.write_bytes(b"data")
    txn = TransactionMetaData()
    storage.tpc_begin(txn)
    storage.storeBlob(oid, serial, b"pickle", str(path), "", txn)
    storage.tpc_vote(txn)
    tid = storage.tpc_finish(txn)
    time.sleep(0.01)
    return tid


def _key(oid, tid):
    return f"blobs/{_oid_hex(oid)}/{_tid_hex(tid)}.blob"


def test_pack_keeps_only_blobs_of_kept_revisions(make_storage, s3_client, tmp_path):
    storage = make_storage("a")
    oid = p64(1)
    tid1 = _commit_blob(storage, oid, z64, tmp_path)
    tid2 = _commit_blob(storage, oid, tid1, tmp_path)

    storage.pack(time.time(), None)

    assert set(s3_client.list_objects("blobs/")) == {_key(oid, tid2)}


def test_pack_history_preserving_keeps_revisions_after_pack_time(
    make_storage, s3_client, tmp_path
):
    storage = make_storage("a")
    if not storage._options.keep_history:
        pytest.skip("history-preserving only")
    oid = p64(1)
    tid1 = _commit_blob(storage, oid, z64, tmp_path)
    tid2 = _commit_blob(storage, oid, tid1, tmp_path)
    pack_time = time.time()
    time.sleep(0.01)
    tid3 = _commit_blob(storage, oid, tid2, tmp_path)

    storage.pack(pack_time, None)

    assert set(s3_client.list_objects("blobs/")) == {_key(oid, tid2), _key(oid, tid3)}


def test_pack_sees_commits_from_another_process(make_storage, s3_client, tmp_path):
    """The packer's own RelStorage instance doesn't see these commits, so its
    lastTransaction() would leave tid2 for a later pack."""
    packer = make_storage("a")
    other = make_storage("b")
    oid = p64(1)
    tid1 = _commit_blob(packer, oid, z64, tmp_path)
    tid2 = _commit_blob(other, oid, tid1, tmp_path)
    tid3 = _commit_blob(other, oid, tid2, tmp_path)

    packer.pack(time.time(), None)

    assert set(s3_client.list_objects("blobs/")) == {_key(oid, tid3)}


def test_pack_keeps_blob_of_transaction_in_progress(make_storage, s3_client, tmp_path):
    """tpc_vote has uploaded the blob of a new object; tpc_finish hasn't run."""
    storage = make_storage("a")
    tid = _commit_blob(storage, p64(1), z64, tmp_path)
    in_flight = _key(p64(2), p64(int.from_bytes(tid, "big") + 1))
    (tmp_path / "in-flight").write_bytes(b"data")
    s3_client.upload_file(str(tmp_path / "in-flight"), in_flight)

    storage.pack(time.time(), None)

    assert in_flight in set(s3_client.list_objects("blobs/"))
