from moto import mock_aws
from zodb_s3blobs.storage import S3BlobStorage

import boto3
import pytest
import ZODB.config


@pytest.fixture
def s3_env():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


class TestZConfig:
    def test_creates_storage(self, s3_env, tmp_path):
        cache_dir = str(tmp_path / "cache")
        storage = ZODB.config.storageFromString(
            f"""\
            %import zodb_s3blobs
            <s3blobstorage>
                bucket-name test-bucket
                s3-region us-east-1
                cache-dir {cache_dir}
                <mappingstorage>
                </mappingstorage>
            </s3blobstorage>
            """
        )
        assert isinstance(storage, S3BlobStorage)
        storage.close()

    def test_all_options(self, s3_env, tmp_path):
        cache_dir = str(tmp_path / "cache")
        storage = ZODB.config.storageFromString(
            f"""\
            %import zodb_s3blobs
            <s3blobstorage>
                bucket-name test-bucket
                s3-prefix myprefix
                s3-endpoint-url http://localhost:9000
                s3-region us-east-1
                s3-access-key minioadmin
                s3-secret-key minioadmin
                s3-use-ssl false
                s3-addressing-style path
                cache-dir {cache_dir}
                cache-size 512MB
                <mappingstorage>
                </mappingstorage>
            </s3blobstorage>
            """
        )
        assert isinstance(storage, S3BlobStorage)
        assert storage._s3_client._prefix == "myprefix"
        assert storage._s3_client.bucket_name == "test-bucket"
        assert storage._cache.max_size == 512 * 1024 * 1024
        storage.close()

    def test_default_values(self, s3_env, tmp_path):
        cache_dir = str(tmp_path / "cache")
        storage = ZODB.config.storageFromString(
            f"""\
            %import zodb_s3blobs
            <s3blobstorage>
                bucket-name test-bucket
                s3-region us-east-1
                cache-dir {cache_dir}
                <mappingstorage>
                </mappingstorage>
            </s3blobstorage>
            """
        )
        assert isinstance(storage, S3BlobStorage)
        # Default cache size is 1GB
        assert storage._cache.max_size == 1024 * 1024 * 1024
        # Default prefix is empty
        assert storage._s3_client._prefix == ""
        storage.close()
