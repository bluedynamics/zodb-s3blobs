import contextlib
import logging
import os
import re
import shutil
import tempfile
import ZODB.blob
import ZODB.interfaces
import ZODB.POSException
import ZODB.utils
import zope.interface


logger = logging.getLogger(__name__)

_BLOB_KEY_RE = re.compile(r"^blobs/([0-9a-f]+)/[0-9a-f]+\.blob$")


@zope.interface.implementer(ZODB.interfaces.IBlobStorage)
class S3BlobStorage:
    """ZODB storage wrapper that redirects blob operations to S3.

    Wraps any base storage via __getattr__ proxy pattern.
    All blob methods are explicitly defined to shadow the base
    storage's methods (if any).
    """

    def __init__(self, base_storage, s3_client, cache, temp_dir=None):
        self.__storage = base_storage
        self._s3_client = s3_client
        self._cache = cache
        self._pending_blobs = {}  # {oid: staged_path}
        self._uploaded_keys = []  # [(oid, tid, s3_key)]
        self._temp_dir = temp_dir or tempfile.mkdtemp()
        os.makedirs(self._temp_dir, exist_ok=True, mode=0o700)

    def __getattr__(self, name):
        return getattr(self.__storage, name)

    def __len__(self):
        return len(self.__storage)

    def __repr__(self):
        return f"<S3BlobStorage proxy for {self.__storage!r}>"

    # -- Blob methods (explicitly defined, shadow base storage) --

    def storeBlob(self, oid, oldserial, data, blobfilename, version, transaction):
        # Store object data (pickle) in base storage
        self.__storage.store(oid, oldserial, data, "", transaction)
        # Stage blob locally
        oid_hex = _oid_hex(oid)
        staged_path = os.path.join(self._temp_dir, f"{oid_hex}.blob")
        shutil.move(blobfilename, staged_path)
        self._pending_blobs[oid] = staged_path

    def loadBlob(self, oid, serial):
        # Check pending blobs first (stored in current txn, not yet in S3)
        pending = self._pending_blobs.get(oid)
        if pending is not None and os.path.exists(pending):
            return pending

        # Check cache
        cached = self._cache.get(oid, serial)
        if cached is not None:
            return cached

        # Download from S3
        key = self._s3_key(oid, serial)
        meta = self._s3_client.head_object(key)
        if meta is None:
            raise ZODB.POSException.POSKeyError(oid, serial)

        # Download to temp, put in cache
        tmp_download = os.path.join(self._temp_dir, f"dl_{_oid_hex(oid)}.tmp")
        self._s3_client.download_file(key, tmp_download)
        path = self._cache.put(oid, serial, tmp_download)
        # Clean up temp download
        with contextlib.suppress(OSError):
            os.remove(tmp_download)
        return path

    def openCommittedBlobFile(self, oid, serial, blob=None):
        filename = self.loadBlob(oid, serial)
        if blob is None:
            return open(filename, "rb")
        return ZODB.blob.BlobFile(filename, "r", blob)

    def temporaryDirectory(self):
        return self._temp_dir

    # -- 2PC hooks --

    def tpc_vote(self, transaction):
        self.__storage.tpc_vote(transaction)
        # _tid is now available from base storage via __getattr__
        tid = self._tid
        for oid, staged_path in self._pending_blobs.items():
            key = self._s3_key(oid, tid)
            self._s3_client.upload_file(staged_path, key)
            self._uploaded_keys.append((oid, tid, key))

    def tpc_finish(self, transaction, func=lambda tid: None):
        tid = self.__storage.tpc_finish(transaction, func)
        # Move staged files into cache (NO S3 ops - must not fail)
        for oid, staged_path in self._pending_blobs.items():
            try:
                self._cache.put(oid, tid, staged_path)
            except Exception:
                logger.warning(
                    "Failed to cache blob for oid=%s tid=%s",
                    _oid_hex(oid),
                    _tid_hex(tid),
                    exc_info=True,
                )
            # Clean staged file
            with contextlib.suppress(OSError):
                os.remove(staged_path)
        self._pending_blobs = {}
        self._uploaded_keys = []
        return tid

    def tpc_abort(self, transaction):
        self.__storage.tpc_abort(transaction)
        # Delete uploaded S3 keys (best-effort)
        for _oid, _tid, key in self._uploaded_keys:
            try:
                self._s3_client.delete_object(key)
            except Exception:
                logger.warning(
                    "Failed to delete S3 key %s during abort", key, exc_info=True
                )
        # Clean staged files
        for _oid, staged_path in self._pending_blobs.items():
            with contextlib.suppress(OSError):
                os.remove(staged_path)
        self._pending_blobs = {}
        self._uploaded_keys = []

    # -- MVCC --

    def new_instance(self):
        new_instance = getattr(self.__storage, "new_instance", None)
        base = new_instance() if new_instance is not None else self.__storage
        # Each MVCC instance gets its own temp dir to avoid file name collisions
        instance_temp = tempfile.mkdtemp(dir=self._temp_dir)
        return S3BlobStorage(base, self._s3_client, self._cache, instance_temp)

    def close(self):
        self.__storage.close()
        close_cache = getattr(self._cache, "close", None)
        if close_cache is not None:
            close_cache()
        with contextlib.suppress(OSError):
            shutil.rmtree(self._temp_dir)

    # -- Pack / GC --

    def pack(self, pack_time, referencesf):
        # Pack the base storage first
        self.__storage.pack(pack_time, referencesf)
        # GC: remove S3 keys for unreachable OIDs
        for key in self._s3_client.list_objects("blobs/"):
            oid = self._oid_from_key(key)
            if oid is None:
                continue
            try:
                self.__storage.load(oid)
            except ZODB.POSException.POSKeyError:
                logger.info("GC: removing orphaned S3 key %s", key)
                try:
                    self._s3_client.delete_object(key)
                except Exception:
                    logger.warning("GC: failed to delete S3 key %s", key, exc_info=True)

    @staticmethod
    def _oid_from_key(key):
        """Extract oid bytes from S3 key like 'blobs/{oid_hex}/{tid_hex}.blob'."""
        m = _BLOB_KEY_RE.match(key)
        if m is None:
            return None
        try:
            return ZODB.utils.p64(int(m.group(1), 16))
        except (ValueError, OverflowError):
            return None

    # -- Helpers --

    def _s3_key(self, oid, tid):
        return f"blobs/{_oid_hex(oid)}/{_tid_hex(tid)}.blob"


def _oid_hex(oid):
    """Convert oid bytes to hex string."""
    return ZODB.utils.oid_repr(oid).removeprefix("0x").lstrip("0") or "0"


def _tid_hex(tid):
    """Convert tid bytes to hex string."""
    return ZODB.utils.tid_repr(tid).removeprefix("0x").lstrip("0") or "0"
