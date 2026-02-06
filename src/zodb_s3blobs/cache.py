from ZODB.utils import oid_repr
from zodb_s3blobs.interfaces import IS3BlobCache
from zope.interface import implementer

import contextlib
import logging
import os
import shutil
import threading


logger = logging.getLogger(__name__)


def _hex(data):
    """Convert oid/tid bytes to hex string without 0x prefix."""
    return oid_repr(data).removeprefix("0x").lstrip("0") or "0"


@implementer(IS3BlobCache)
class S3BlobCache:
    """Local filesystem LRU cache for S3 blobs.

    Files are stored as {cache_dir}/{oid_hex}/{tid_hex}.blob.
    Background cleanup removes oldest files (by atime) when
    total size exceeds max_size.
    """

    def __init__(self, cache_dir, max_size=1024 * 1024 * 1024):
        self.cache_dir = cache_dir
        self.max_size = max_size
        self._target_size = int(max_size * 0.9)
        self._check_threshold = max(int(max_size * 0.1), 1)
        self._bytes_loaded = 0
        self._lock = threading.Lock()
        self._checker_thread = None
        os.makedirs(cache_dir, exist_ok=True)

    def _blob_path(self, oid, tid):
        oid_hex = _hex(oid)
        tid_hex = _hex(tid)
        return os.path.join(self.cache_dir, oid_hex, f"{tid_hex}.blob")

    def get(self, oid, tid):
        path = self._blob_path(oid, tid)
        if os.path.exists(path):
            return path
        return None

    def put(self, oid, tid, source_path):
        path = self._blob_path(oid, tid)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        shutil.copy2(source_path, path)
        size = os.path.getsize(path)
        self.notify_loaded(size)
        return path

    def notify_loaded(self, byte_count):
        with self._lock:
            self._bytes_loaded += byte_count
            if self._bytes_loaded >= self._check_threshold:
                self._bytes_loaded = 0
                self._start_cleanup()

    def _start_cleanup(self):
        """Start background cleanup thread if not already running."""
        if self._checker_thread is not None and self._checker_thread.is_alive():
            return
        t = threading.Thread(target=self._cleanup, daemon=True)
        self._checker_thread = t
        t.start()

    def _cleanup(self):
        """Remove oldest files until total size is under target."""
        try:
            files = []
            for dirpath, _dirnames, filenames in os.walk(self.cache_dir):
                for fn in filenames:
                    if fn.endswith(".blob"):
                        fp = os.path.join(dirpath, fn)
                        try:
                            st = os.stat(fp)
                            files.append((st.st_atime, st.st_size, fp))
                        except OSError:
                            pass

            total_size = sum(size for _, size, _ in files)
            if total_size <= self.max_size:
                return

            # Sort by atime ascending (oldest first)
            files.sort(key=lambda x: x[0])

            for _atime, size, fp in files:
                if total_size <= self._target_size:
                    break
                try:
                    os.remove(fp)
                    total_size -= size
                    # Try to remove empty parent dirs
                    parent = os.path.dirname(fp)
                    if parent != self.cache_dir:
                        with contextlib.suppress(OSError):
                            os.rmdir(parent)
                except OSError:
                    pass
        except Exception:
            logger.exception("Error during cache cleanup")

    def wait_for_cleanup(self):
        """Wait for any running cleanup thread to finish. For testing."""
        with self._lock:
            t = self._checker_thread
        if t is not None:
            t.join(timeout=10)

    def current_size(self):
        """Return total size of cached files. For testing."""
        total = 0
        for dirpath, _dirnames, filenames in os.walk(self.cache_dir):
            for fn in filenames:
                if fn.endswith(".blob"):
                    with contextlib.suppress(OSError):
                        total += os.path.getsize(os.path.join(dirpath, fn))
        return total
