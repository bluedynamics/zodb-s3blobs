from ZODB.config import BaseConfig


class S3BlobStorageFactory(BaseConfig):
    """ZConfig factory for S3BlobStorage."""

    def open(self):
        from zodb_s3blobs.cache import S3BlobCache
        from zodb_s3blobs.s3client import S3Client
        from zodb_s3blobs.storage import S3BlobStorage

        config = self.config
        base = config.base.open()

        s3_client = S3Client(
            bucket_name=config.bucket_name,
            prefix=config.s3_prefix,
            endpoint_url=config.s3_endpoint_url,
            region_name=config.s3_region,
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
            use_ssl=config.s3_use_ssl,
            addressing_style=config.s3_addressing_style,
        )
        cache = S3BlobCache(
            cache_dir=config.cache_dir,
            max_size=config.cache_size,
        )
        return S3BlobStorage(base, s3_client, cache)
