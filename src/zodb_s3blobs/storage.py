import collections
import contextlib
import logging
import os
import re
import shutil
import sys
import tempfile
import ZODB.blob
import ZODB.interfaces
import ZODB.POSException
import ZODB.utils
import zope.interface


logger = logging.getLogger(__name__)

_relstorage_lock_early_applied = False


def _ensure_relstorage_lock_early():
    """Force RelStorage to allocate TID during tpc_vote (LOCK_EARLY).

    RelStorage 3.x defers TID allocation to tpc_finish for shorter lock
    hold times. S3BlobStorage needs the TID in tpc_vote to construct the
    S3 key before uploading. Without LOCK_EARLY, the TID is not available.

    This monkey-patches the module-level LOCK_EARLY flag in
    relstorage.storage.tpc.vote. The flag is read by AbstractVote._vote()
    to decide whether to call _lock_and_move(vote_only=True).

    Safe to call multiple times (idempotent).
    """
    global _relstorage_lock_early_applied
    if _relstorage_lock_early_applied:
        return
    try:
        import relstorage.storage.tpc.vote as vote_mod
    except ImportError:
        return
    if not vote_mod.LOCK_EARLY:
        vote_mod.LOCK_EARLY = True
        logger.info(
            "Forced RELSTORAGE_LOCK_EARLY=True for S3BlobStorage "
            "TID availability during tpc_vote"
        )
    _relstorage_lock_early_applied = True


_BLOB_KEY_RE = re.compile(r"^blobs/([0-9a-f]+)/([0-9a-f]+)\.blob$")
_OBJECT_STATE_BATCH = 500


@zope.interface.implementer(ZODB.interfaces.IBlobStorage)
class S3BlobStorage:
    """ZODB storage wrapper that redirects blob operations to S3.

    Wraps any base storage via __getattr__ proxy pattern.
    All blob methods are explicitly defined to shadow the base
    storage's methods (if any).

    Provides IMVCCStorage when the base storage does (e.g. RelStorage).
    """

    def __init__(self, base_storage, s3_client, cache, temp_dir=None):
        self.__storage = base_storage
        self._s3_client = s3_client
        self._cache = cache
        self._pending_blobs = {}  # {oid: staged_path}
        self._uploaded_keys = []  # [(oid, tid, s3_key)]
        self._temp_dir = temp_dir or tempfile.mkdtemp()
        os.makedirs(self._temp_dir, exist_ok=True, mode=0o700)

        # Otherwise ZODB.DB wraps us in an MVCCAdapter, which never polls
        # an MVCC base for invalidations: commits from other processes
        # stay invisible.
        if ZODB.interfaces.IMVCCStorage.providedBy(base_storage):
            zope.interface.alsoProvides(self, ZODB.interfaces.IMVCCStorage)

        # Force LOCK_EARLY if RelStorage is involved so TID is
        # available during tpc_vote for S3 key construction.
        if hasattr(base_storage, "_tpc_phase"):
            _ensure_relstorage_lock_early()

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
        if self._pending_blobs:
            tid = self._extract_base_tid()
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

    def release(self):
        # Called by ZODB on an IMVCCStorage instance when its connection
        # is discarded.
        self.__storage.release()
        with contextlib.suppress(OSError):
            shutil.rmtree(self._temp_dir)

    def close(self):
        self.__storage.close()
        close_cache = getattr(self._cache, "close", None)
        if close_cache is not None:
            close_cache()
        with contextlib.suppress(OSError):
            shutil.rmtree(self._temp_dir)

    # -- Pack / GC --

    def pack(self, pack_time, referencesf):
        """Pack the base storage, then delete S3 blobs it no longer references.

        The key ``blobs/<oid>/<tid>.blob`` is kept while the base storage still
        has revision ``tid`` of ``oid``. History-preserving storages keep the
        revisions they need for ``loadBefore`` after ``pack_time``, so the
        blobs of those revisions are kept too.
        """
        result = self.__storage.pack(pack_time, referencesf)
        with self._gc_view() as view:
            # Blobs are uploaded in tpc_vote, before the commit is visible:
            # leave keys newer than the last committed transaction alone.
            last_tid = ZODB.utils.u64(view.lastTransaction())
            keys_by_oid = self._blob_keys_by_oid(last_tid)
            live = self._live_revisions(view, keys_by_oid)
        deleted = 0
        for oid, keys in keys_by_oid.items():
            live_tids = live.get(oid, ())
            for tid, key in keys:
                if tid in live_tids:
                    continue
                logger.info("GC: removing S3 key %s", key)
                try:
                    self._s3_client.delete_object(key)
                    deleted += 1
                except Exception:
                    logger.warning("GC: failed to delete S3 key %s", key, exc_info=True)
        logger.info("GC: deleted %d S3 keys", deleted)
        return result

    @contextlib.contextmanager
    def _gc_view(self):
        """Yield the base storage, or a fresh instance of an MVCC base.

        RelStorage's root instance does not see commits made by other
        processes; a freshly polled instance does.
        """
        base = self.__storage
        if not ZODB.interfaces.IMVCCStorage.providedBy(base):
            yield base
            return
        instance = base.new_instance()
        try:
            instance.poll_invalidations()
            yield instance
        finally:
            instance.release()

    def _blob_keys_by_oid(self, max_tid):
        """Return ``{oid: [(tid, key), ...]}`` for keys with ``tid <= max_tid``."""
        keys_by_oid = collections.defaultdict(list)
        for key in self._s3_client.list_objects("blobs/"):
            revision = self._revision_from_key(key)
            if revision is None:
                continue
            oid, tid = revision
            if tid <= max_tid:
                keys_by_oid[oid].append((tid, key))
        return keys_by_oid

    def _live_revisions(self, view, oids):
        """Return ``{oid: {tid, ...}}`` of the revisions the base storage has."""
        if self._is_history_preserving_relstorage():
            return self._live_revisions_from_object_state(oids)
        live = {}
        for oid in oids:
            try:
                records = view.history(oid, size=sys.maxsize)
            except ZODB.POSException.POSKeyError:
                continue
            live[oid] = {ZODB.utils.u64(record["tid"]) for record in records}
        return live

    def _is_history_preserving_relstorage(self):
        options = getattr(self.__storage, "_options", None)
        return bool(getattr(options, "keep_history", False)) and hasattr(
            self.__storage, "_adapter"
        )

    def _live_revisions_from_object_state(self, oids):
        """History-preserving RelStorage: read revisions from ``object_state``.

        ``history()`` omits the revision that was current at pack time,
        although pack keeps it, so it can't be used here.
        """
        live = collections.defaultdict(set)
        zoids = [ZODB.utils.u64(oid) for oid in oids]
        connmanager = self.__storage._adapter.connmanager
        conn, cursor = connmanager.open_for_load()
        try:
            # Stay under SQLite's default limit of 999 query parameters.
            for i in range(0, len(zoids), _OBJECT_STATE_BATCH):
                batch = zoids[i : i + _OBJECT_STATE_BATCH]
                placeholders = ",".join(["%s"] * len(batch))
                cursor.execute(
                    f"SELECT zoid, tid FROM object_state WHERE zoid IN ({placeholders})",
                    batch,
                )
                for zoid, tid in cursor.fetchall():
                    live[ZODB.utils.p64(zoid)].add(tid)
        finally:
            connmanager.close(conn, cursor)
        return live

    @staticmethod
    def _revision_from_key(key):
        """Return ``(oid, tid)`` from a key like ``blobs/{oid_hex}/{tid_hex}.blob``.

        ``oid`` is bytes, ``tid`` an int. Returns None for any other key.
        """
        m = _BLOB_KEY_RE.match(key)
        if m is None:
            return None
        try:
            return ZODB.utils.p64(int(m.group(1), 16)), int(m.group(2), 16)
        except (ValueError, OverflowError):
            return None

    # -- Helpers --

    def _extract_base_tid(self):
        """Extract the current transaction TID from the base storage.

        Supports two storage backends:
        - BaseStorage subclasses (MappingStorage, FileStorage): ``_tid`` attribute
        - RelStorage: ``_tpc_phase.committing_tid_lock.tid``
          (requires LOCK_EARLY so TID is allocated during tpc_vote)
        """
        # BaseStorage path (MappingStorage, FileStorage)
        tid = getattr(self.__storage, "_tid", None)
        if tid is not None:
            return tid
        # RelStorage path: TID lives in the TPC phase's lock object
        phase = getattr(self.__storage, "_tpc_phase", None)
        if phase is not None:
            lock = getattr(phase, "committing_tid_lock", None)
            if lock is not None:
                tid = getattr(lock, "tid", None)
                if tid is not None:
                    return tid
        raise RuntimeError(
            "Cannot determine TID after tpc_vote. "
            "The base storage must expose _tid (BaseStorage) "
            "or _tpc_phase.committing_tid_lock.tid (RelStorage with LOCK_EARLY)."
        )

    def _s3_key(self, oid, tid):
        return f"blobs/{_oid_hex(oid)}/{_tid_hex(tid)}.blob"


def _oid_hex(oid):
    """Convert oid bytes to hex string."""
    return ZODB.utils.oid_repr(oid).removeprefix("0x").lstrip("0") or "0"


def _tid_hex(tid):
    """Convert tid bytes to hex string."""
    return ZODB.utils.tid_repr(tid).removeprefix("0x").lstrip("0") or "0"
