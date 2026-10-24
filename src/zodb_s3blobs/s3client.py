from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from zodb_s3blobs.interfaces import IS3Client
from zope.interface import implementer

import base64
import boto3
import contextlib
import logging
import os
import re
import tempfile


logger = logging.getLogger(__name__)


class S3OperationError(Exception):
    """Wraps boto3 ClientError to avoid leaking AWS infrastructure details."""


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
        read_timeout=300,
        download_connect_timeout=5,
        download_read_timeout=10,
        download_max_attempts=2,
        sse_customer_key=None,
        s3_max_concurrency=1,
        multipart_threshold=5 * 1024 * 1024 * 1024,
    ):
        self.bucket_name = bucket_name
        self._prefix = prefix.rstrip("/") if prefix else ""
        self._transfer_config = TransferConfig(
            multipart_threshold=multipart_threshold,
            max_concurrency=s3_max_concurrency,
        )
        # Downloads run in the web request path. s3transfer wraps each GET
        # (including its botocore retries) in its own retry loop, so keep it
        # to one restart; the default of 5 multiplies the worst case.
        self._download_transfer_config = TransferConfig(num_download_attempts=2)

        if self._prefix:
            if not re.fullmatch(r"[a-zA-Z0-9._/-]*", self._prefix):
                raise ValueError(
                    f"s3-prefix contains invalid characters: {self._prefix!r}. "
                    "Only alphanumeric characters, dots, hyphens, underscores, "
                    "and slashes are allowed."
                )
            if ".." in self._prefix:
                raise ValueError(f"s3-prefix must not contain '..': {self._prefix!r}")

        # SSE-C setup
        if sse_customer_key:
            if not use_ssl:
                raise ValueError("SSE-C requires SSL — set s3-use-ssl to true")
            raw_key = base64.b64decode(sse_customer_key)
            if len(raw_key) != 32:
                raise ValueError(
                    f"SSE-C key must be 32 bytes (256-bit), got {len(raw_key)}"
                )
            self._sse_extra_args = {
                "SSECustomerAlgorithm": "AES256",
                "SSECustomerKey": raw_key,
            }
        else:
            self._sse_extra_args = {}

        base_config = Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            s3={"addressing_style": addressing_style},
            max_pool_connections=50,
        )
        # Uploads keep long timeouts: Hetzner answers PUTs under concurrent
        # load after more than 60 s (#14).
        config = base_config.merge(
            Config(connect_timeout=connect_timeout, read_timeout=read_timeout)
        )
        # Reads (download, head) block a web request and must give up on a
        # stalled connection quickly so the retry can use a fresh one (#25).
        download_config = base_config.merge(
            Config(
                connect_timeout=download_connect_timeout,
                read_timeout=download_read_timeout,
                retries={
                    "mode": "standard",
                    "total_max_attempts": download_max_attempts,
                },
            )
        )

        kwargs: dict[str, object] = {}
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if region_name:
            kwargs["region_name"] = region_name
        if aws_access_key_id:
            kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            kwargs["aws_secret_access_key"] = aws_secret_access_key
        kwargs["use_ssl"] = use_ssl
        if not use_ssl:
            logger.warning(
                "S3 SSL is disabled — data and credentials are transmitted in cleartext"
            )

        self._client = boto3.client("s3", config=config, **kwargs)
        self._download_client = boto3.client("s3", config=download_config, **kwargs)

    def _full_key(self, s3_key):
        if self._prefix:
            return f"{self._prefix}/{s3_key}"
        return s3_key

    def _wrap_client_error(self, e, operation, s3_key):
        """Wrap ClientError in a generic error, logging the original at DEBUG."""
        logger.debug("S3 %s failed for key=%s: %s", operation, s3_key, e)
        raise S3OperationError(
            f"S3 {operation} failed for key={s3_key}: "
            f"{e.response['Error'].get('Code', 'Unknown')}"
        ) from e

    def upload_file(self, local_path, s3_key):
        full_key = self._full_key(s3_key)
        try:
            self._client.upload_file(
                local_path,
                self.bucket_name,
                full_key,
                ExtraArgs=self._sse_extra_args or None,
                Config=self._transfer_config,
            )
        except ClientError as e:
            self._wrap_client_error(e, "upload", s3_key)

    def download_file(self, s3_key, local_path):
        full_key = self._full_key(s3_key)
        target_dir = os.path.dirname(local_path) or "."
        os.makedirs(target_dir, exist_ok=True, mode=0o700)
        fd, tmp_path = tempfile.mkstemp(dir=target_dir, suffix=".blob.tmp")
        try:
            os.close(fd)
            try:
                self._download_client.download_file(
                    self.bucket_name,
                    full_key,
                    tmp_path,
                    ExtraArgs=self._sse_extra_args or None,
                    Config=self._download_transfer_config,
                )
            except ClientError as e:
                self._wrap_client_error(e, "download", s3_key)
            os.rename(tmp_path, local_path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise

    def delete_object(self, s3_key):
        full_key = self._full_key(s3_key)
        try:
            self._client.delete_object(Bucket=self.bucket_name, Key=full_key)
        except ClientError as e:
            self._wrap_client_error(e, "delete", s3_key)

    def head_object(self, s3_key):
        full_key = self._full_key(s3_key)
        try:
            return self._download_client.head_object(
                Bucket=self.bucket_name, Key=full_key, **self._sse_extra_args
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            self._wrap_client_error(e, "head", s3_key)

    def list_objects(self, prefix=""):
        full_prefix = self._full_key(prefix) if prefix else self._prefix
        paginator = self._client.get_paginator("list_objects_v2")
        prefix_len = len(self._prefix) + 1 if self._prefix else 0
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=full_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Strip the prefix so callers see logical keys
                    if prefix_len:
                        yield key[prefix_len:]
                    else:
                        yield key
        except ClientError as e:
            self._wrap_client_error(e, "list", prefix)
