from botocore.config import Config
from botocore.exceptions import ClientError
from zodb_s3blobs.interfaces import IS3Client
from zope.interface import implementer

import boto3
import logging
import os


logger = logging.getLogger(__name__)


@implementer(IS3Client)
class S3Client:
    """Thin boto3 wrapper for S3-compatible object storage."""

    def __init__(
        self,
        bucket_name,
        prefix="",
        endpoint_url=None,
        region_name=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        use_ssl=True,
        addressing_style="auto",
        connect_timeout=60,
        read_timeout=60,
    ):
        self.bucket_name = bucket_name
        self._prefix = prefix.rstrip("/") if prefix else ""

        config = Config(
            s3={"addressing_style": addressing_style},
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )

        kwargs = {"config": config}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if region_name:
            kwargs["region_name"] = region_name
        if aws_access_key_id:
            kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            kwargs["aws_secret_access_key"] = aws_secret_access_key
        kwargs["use_ssl"] = use_ssl

        self._client = boto3.client("s3", **kwargs)

    def _full_key(self, s3_key):
        if self._prefix:
            return f"{self._prefix}/{s3_key}"
        return s3_key

    def upload_file(self, local_path, s3_key):
        full_key = self._full_key(s3_key)
        self._client.upload_file(local_path, self.bucket_name, full_key)

    def download_file(self, s3_key, local_path):
        full_key = self._full_key(s3_key)
        tmp_path = local_path + ".tmp"
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        self._client.download_file(self.bucket_name, full_key, tmp_path)
        os.rename(tmp_path, local_path)

    def delete_object(self, s3_key):
        full_key = self._full_key(s3_key)
        self._client.delete_object(Bucket=self.bucket_name, Key=full_key)

    def head_object(self, s3_key):
        full_key = self._full_key(s3_key)
        try:
            return self._client.head_object(Bucket=self.bucket_name, Key=full_key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            raise

    def list_objects(self, prefix=""):
        full_prefix = self._full_key(prefix) if prefix else self._prefix
        paginator = self._client.get_paginator("list_objects_v2")
        prefix_len = len(self._prefix) + 1 if self._prefix else 0
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Strip the prefix so callers see logical keys
                if prefix_len:
                    yield key[prefix_len:]
                else:
                    yield key
