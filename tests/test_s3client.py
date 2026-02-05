import os

import boto3
import pytest
from moto import mock_aws

from zodb_s3blobs.interfaces import IS3Client
from zodb_s3blobs.s3client import S3Client


@pytest.fixture
def s3_env():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(
            Bucket="test-bucket"
        )
        yield


@pytest.fixture
def client(s3_env):
    return S3Client(bucket_name="test-bucket", region_name="us-east-1")


@pytest.fixture
def prefixed_client(s3_env):
    return S3Client(
        bucket_name="test-bucket", prefix="myprefix", region_name="us-east-1"
    )


class TestS3ClientInterface:
    def test_interface_provided(self, client):
        assert IS3Client.providedBy(client)


class TestUploadDownload:
    def test_upload_and_download_roundtrip(self, client, tmp_path):
        src = tmp_path / "source.bin"
        src.write_bytes(b"hello blob data")

        client.upload_file(str(src), "test/key.blob")

        dst = tmp_path / "downloaded.bin"
        client.download_file("test/key.blob", str(dst))

        assert dst.read_bytes() == b"hello blob data"

    def test_download_atomic_rename(self, client, tmp_path):
        """Download should write to .tmp first, then rename."""
        src = tmp_path / "source.bin"
        src.write_bytes(b"atomic test")
        client.upload_file(str(src), "atomic/key.blob")

        dst = tmp_path / "final.bin"
        client.download_file("atomic/key.blob", str(dst))

        # Final file exists, temp file does not
        assert dst.exists()
        assert not (tmp_path / "final.bin.tmp").exists()

    def test_download_large_file(self, client, tmp_path):
        """Test download of a file larger than typical buffer sizes."""
        src = tmp_path / "large.bin"
        data = b"x" * (1024 * 1024)  # 1MB
        src.write_bytes(data)
        client.upload_file(str(src), "large/key.blob")

        dst = tmp_path / "large_dl.bin"
        client.download_file("large/key.blob", str(dst))
        assert dst.read_bytes() == data


class TestDeleteObject:
    def test_delete_object(self, client, tmp_path):
        src = tmp_path / "to_delete.bin"
        src.write_bytes(b"delete me")
        client.upload_file(str(src), "del/key.blob")

        assert client.head_object("del/key.blob") is not None
        client.delete_object("del/key.blob")
        assert client.head_object("del/key.blob") is None

    def test_delete_nonexistent_does_not_raise(self, client):
        """Deleting a non-existent key should not raise."""
        client.delete_object("nonexistent/key.blob")


class TestHeadObject:
    def test_head_object_exists(self, client, tmp_path):
        src = tmp_path / "head.bin"
        src.write_bytes(b"head test")
        client.upload_file(str(src), "head/key.blob")

        result = client.head_object("head/key.blob")
        assert result is not None
        assert "ContentLength" in result
        assert result["ContentLength"] == 9

    def test_head_object_missing(self, client):
        result = client.head_object("missing/key.blob")
        assert result is None


class TestListObjects:
    def test_list_objects(self, client, tmp_path):
        src = tmp_path / "list.bin"
        src.write_bytes(b"list test")
        for i in range(3):
            client.upload_file(str(src), f"list/{i}.blob")

        keys = list(client.list_objects("list/"))
        assert len(keys) == 3
        assert set(keys) == {"list/0.blob", "list/1.blob", "list/2.blob"}

    def test_list_objects_empty(self, client):
        keys = list(client.list_objects("nonexistent/"))
        assert keys == []


class TestPrefix:
    def test_prefix_applied_to_upload(self, prefixed_client, tmp_path):
        src = tmp_path / "prefixed.bin"
        src.write_bytes(b"prefixed data")
        prefixed_client.upload_file(str(src), "blobs/oid/tid.blob")

        # Should be stored under myprefix/blobs/oid/tid.blob
        result = prefixed_client.head_object("blobs/oid/tid.blob")
        assert result is not None

    def test_prefix_applied_to_list(self, prefixed_client, tmp_path):
        src = tmp_path / "prefixed.bin"
        src.write_bytes(b"prefixed data")
        prefixed_client.upload_file(str(src), "blobs/a.blob")
        prefixed_client.upload_file(str(src), "blobs/b.blob")

        keys = list(prefixed_client.list_objects("blobs/"))
        assert len(keys) == 2

    def test_prefix_isolation(self, s3_env, tmp_path):
        """Two clients with different prefixes don't see each other's data."""
        client_a = S3Client(
            bucket_name="test-bucket", prefix="ns_a", region_name="us-east-1"
        )
        client_b = S3Client(
            bucket_name="test-bucket", prefix="ns_b", region_name="us-east-1"
        )

        src = tmp_path / "iso.bin"
        src.write_bytes(b"isolation test")
        client_a.upload_file(str(src), "key.blob")

        assert client_a.head_object("key.blob") is not None
        assert client_b.head_object("key.blob") is None

    def test_no_prefix(self, client, tmp_path):
        """Client with no prefix stores keys as-is."""
        src = tmp_path / "no_prefix.bin"
        src.write_bytes(b"no prefix")
        client.upload_file(str(src), "raw/key.blob")

        # Verify via raw boto3 that the key is exactly "raw/key.blob"
        s3 = boto3.client("s3", region_name="us-east-1")
        resp = s3.list_objects_v2(Bucket="test-bucket", Prefix="raw/")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert "raw/key.blob" in keys
