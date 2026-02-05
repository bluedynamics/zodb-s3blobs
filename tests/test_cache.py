from ZODB.utils import p64
from zodb_s3blobs.cache import S3BlobCache
from zodb_s3blobs.interfaces import IS3BlobCache

import os
import pytest
import threading
import time


def _make_oid(n):
    return p64(n)


def _make_tid(n):
    return p64(n)


@pytest.fixture
def cache(tmp_path):
    return S3BlobCache(str(tmp_path / "cache"), max_size=1024 * 1024)


@pytest.fixture
def small_cache(tmp_path):
    """Cache with very small max_size for eviction tests."""
    return S3BlobCache(str(tmp_path / "cache"), max_size=500)


def _write_blob(tmp_path, name, size=100):
    """Create a temp file with given size."""
    p = tmp_path / name
    p.write_bytes(b"x" * size)
    return str(p)


class TestInterface:
    def test_interface_provided(self, cache):
        assert IS3BlobCache.providedBy(cache)


class TestGetPut:
    def test_get_missing_returns_none(self, cache):
        result = cache.get(_make_oid(1), _make_tid(1))
        assert result is None

    def test_put_and_get_roundtrip(self, cache, tmp_path):
        blob_path = _write_blob(tmp_path, "blob1.bin", size=42)
        oid = _make_oid(1)
        tid = _make_tid(100)

        cache.put(oid, tid, blob_path)
        result = cache.get(oid, tid)

        assert result is not None
        assert os.path.exists(result)
        with open(result, "rb") as f:
            assert len(f.read()) == 42

    def test_put_creates_subdirectory(self, cache, tmp_path):
        blob_path = _write_blob(tmp_path, "blob2.bin")
        oid = _make_oid(42)
        tid = _make_tid(200)

        cache.put(oid, tid, blob_path)
        result = cache.get(oid, tid)

        # The cached file should be in a subdirectory named by oid_hex
        assert result is not None
        assert os.path.isfile(result)

    def test_get_path_structure(self, cache, tmp_path):
        blob_path = _write_blob(tmp_path, "blob3.bin")
        oid = _make_oid(1)
        tid = _make_tid(1)

        cache.put(oid, tid, blob_path)
        result = cache.get(oid, tid)

        # Path should end with {oid_hex}/{tid_hex}.blob
        assert result.endswith(".blob")
        parts = result.split(os.sep)
        # Should have oid_hex directory and tid_hex.blob file
        assert len(parts) >= 2

    def test_put_different_tids_same_oid(self, cache, tmp_path):
        oid = _make_oid(1)
        blob1 = _write_blob(tmp_path, "v1.bin", size=10)
        blob2 = _write_blob(tmp_path, "v2.bin", size=20)

        cache.put(oid, _make_tid(1), blob1)
        cache.put(oid, _make_tid(2), blob2)

        r1 = cache.get(oid, _make_tid(1))
        r2 = cache.get(oid, _make_tid(2))
        assert r1 is not None
        assert r2 is not None
        assert r1 != r2


class TestEviction:
    def test_cleanup_removes_oldest_files(self, small_cache, tmp_path):
        """Put several files exceeding max_size, verify oldest are removed."""
        oid = _make_oid(1)
        # Put 3 blobs of 200 bytes each (600 total > 500 max_size)
        for i in range(3):
            blob = _write_blob(tmp_path, f"evict{i}.bin", size=200)
            small_cache.put(oid, _make_tid(i + 1), blob)
            # Touch files with increasing atime so eviction order is clear
            time.sleep(0.05)

        # Wait for any background cleanup
        small_cache.wait_for_cleanup()

        # After cleanup, total size should be under max_size
        total = small_cache.current_size()
        assert total <= small_cache.max_size

    def test_cleanup_reaches_target_size(self, tmp_path):
        """After cleanup, size should be under target (90% of max_size)."""
        cache = S3BlobCache(str(tmp_path / "cache"), max_size=1000)
        oid = _make_oid(1)

        for i in range(10):
            blob = _write_blob(tmp_path, f"target{i}.bin", size=200)
            cache.put(oid, _make_tid(i + 1), blob)
            time.sleep(0.02)

        cache.wait_for_cleanup()
        total = cache.current_size()
        # Target is 90% of max_size
        assert total <= cache.max_size * 0.9

    def test_notify_loaded_triggers_cleanup(self, tmp_path):
        """Calling notify_loaded with enough bytes should trigger cleanup."""
        cache = S3BlobCache(str(tmp_path / "cache"), max_size=500)
        oid = _make_oid(1)

        # Manually put files (bypassing notify)
        for i in range(5):
            blob = _write_blob(tmp_path, f"notify{i}.bin", size=200)
            cache.put(oid, _make_tid(i + 1), blob)

        # Trigger explicit notify
        cache.notify_loaded(200)
        cache.wait_for_cleanup()

        total = cache.current_size()
        assert total <= cache.max_size


class TestConcurrency:
    def test_concurrent_puts(self, cache, tmp_path):
        """Multiple threads putting simultaneously should not crash."""
        errors = []

        def worker(thread_id):
            try:
                for i in range(5):
                    blob = _write_blob(tmp_path, f"t{thread_id}_{i}.bin", size=50)
                    cache.put(
                        _make_oid(thread_id * 100 + i),
                        _make_tid(1),
                        blob,
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Errors in threads: {errors}"
