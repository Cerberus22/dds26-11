"""
Singleton Redis access for microservices.

- Holds the main Redis connection and an optional lock Redis connection.
- Exposes simple get/set/mset methods used by the services.
"""

import atexit
import os
import threading
import time
import uuid
import random
from typing import Dict, List, Optional, Set
import contextvars

import redis


class DDSdb_async:
    """Singleton wrapper around Redis with optional lock Redis."""

    _instance: "DDSdb_async | None" = None
    _initialized: bool = False

    _scripts = {}


    def __new__(cls) -> "DDSdb_async":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self.__class__._initialized:
            return

        self._db = redis.asyncio.Redis(
            host=os.environ["REDIS_HOST"],
            port=int(os.environ["REDIS_PORT"]),
            password=os.environ["REDIS_PASSWORD"],
            db=int(os.environ["REDIS_DB"]),
        )

        self._lock_db: "redis.Redis | None" = None
        if os.environ.get("LOCK_REDIS_HOST"):
            self._lock_db = redis.Redis(
                host=os.environ["LOCK_REDIS_HOST"],
                port=int(os.environ.get("LOCK_REDIS_PORT", "6379")),
                password=os.environ.get("LOCK_REDIS_PASSWORD", "redis"),
                db=int(os.environ.get("LOCK_REDIS_DB", "0")),
            )

        self.__class__._initialized = True
        self.initialize_scripts()

        self._txn_shared: Dict[str, Set[str]] = {}
        self._txn_exclusive: Dict[str, Set[str]] = {}

        # self._local = threading.local()
        self._current_txn: contextvars.ContextVar = contextvars.ContextVar('current_txn', default=None)

    
    def initialize_scripts(self) -> None:
        if self._scripts:
            return

        if self._lock_db is None:
            raise RuntimeError("LOCK_REDIS_* not configured, lock_db is None")

        # Acquire shared lock: only if exclusive lock key does not exist
        self._scripts["acquire_shared"] = self._lock_db.register_script("""
            -- KEYS[1] = shared lock key (SET)
            -- KEYS[2] = exclusive lock key (STRING)
            -- ARGV[1] = txn_id

            if redis.call("EXISTS", KEYS[2]) == 1 then
                return 0
            end

            redis.call("SADD", KEYS[1], ARGV[1])
            return 1
        """)

        # Acquire exclusive lock: only if not taken by another txn (re-entrant allowed)
        self._scripts["acquire_exclusive"] = self._lock_db.register_script("""
            -- KEYS[1] = exclusive lock key (STRING)
            -- ARGV[1] = txn_id
            -- ARGV[2] = ttl_ms (optional)

            local current = redis.call("GET", KEYS[1])

            if not current then
                if ARGV[2] then
                    redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
                else
                    redis.call("SET", KEYS[1], ARGV[1])
                end
                return 1
            end

            if current == ARGV[1] then
                return 1
            end

            return 0
        """)

        # Release shared lock: remove txn from set; delete key if empty
        self._scripts["release_shared"] = self._lock_db.register_script("""
            -- KEYS[1] = shared lock key (SET)
            -- ARGV[1] = txn_id

            redis.call("SREM", KEYS[1], ARGV[1])

            if redis.call("SCARD", KEYS[1]) == 0 then
                redis.call("DEL", KEYS[1])
            end

            return 1
        """)

        # Release exclusive lock: delete only if owned by txn
        self._scripts["release_exclusive"] = self._lock_db.register_script("""
            -- KEYS[1] = exclusive lock key (STRING)
            -- ARGV[1] = txn_id

            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            end

            return 0
        """)


    # -----------------------
    # Private lock key helpers
    # -----------------------
    def _shared_lock_key(self, key: str) -> str:
        return f"shared:{key}"

    def _exclusive_lock_key(self, key: str) -> str:
        return f"exclusive:{key}"

    # -----------------------
    # Private tracking helpers
    # -----------------------
    def _record_shared(self, txn_id: uuid.UUID, shared_lock_key: str) -> None:
        tid = str(txn_id)
        self._txn_shared.setdefault(tid, set()).add(shared_lock_key)

    def _record_exclusive(self, txn_id: uuid.UUID, exclusive_lock_key: str) -> None:
        tid = str(txn_id)
        self._txn_exclusive.setdefault(tid, set()).add(exclusive_lock_key)

    def _forget_txn(self, txn_id: uuid.UUID) -> None:
        tid = str(txn_id)
        self._txn_shared.pop(tid, None)
        self._txn_exclusive.pop(tid, None)

    # -----------------------
    # Private "does this txn hold lock?" helpers
    # -----------------------
    def _txn_has_shared_lock(self, shared_lock_key: str, txn_id: uuid.UUID) -> bool:
        if self._lock_db is None:
            return False
        # shared lock is a SET of txn_ids
        return bool(self._lock_db.sismember(shared_lock_key, str(txn_id)))

    def _txn_has_exclusive_lock(self, exclusive_lock_key: str, txn_id: uuid.UUID) -> bool:
        if self._lock_db is None:
            return False
        owner = self._lock_db.get(exclusive_lock_key)
        if owner is None:
            return False
        # redis-py returns bytes unless decode_responses=True
        return owner.decode() == str(txn_id)

    # -----------------------
    # Private blocking acquisition
    # -----------------------
    def _acquire_shared_lock(
        self,
        shared_lock_key: str,
        exclusive_lock_key: str,
        txn_id: uuid.UUID,
        timeout_s: float = 5.0,
        base_sleep_s: float = 0.01,
        max_sleep_s: float = 0.2,
    ) -> bool:
        self.initialize_scripts()
        deadline = time.monotonic() + timeout_s
        sleep_s = base_sleep_s

        while True:
            result = self._scripts["acquire_shared"](
                keys=[shared_lock_key, exclusive_lock_key],
                args=[str(txn_id)],
            )

            if result == 1:
                return True

            if time.monotonic() >= deadline:
                return False

            time.sleep(random.uniform(0, sleep_s))
            sleep_s = min(max_sleep_s, sleep_s * 2)

    def _acquire_exclusive_lock(
        self,
        exclusive_lock_key: str,
        txn_id: uuid.UUID,
        ttl_ms: Optional[int] = 10000,
        timeout_s: float = 5.0,
        base_sleep_s: float = 0.01,
        max_sleep_s: float = 0.2,
    ) -> bool:
        self.initialize_scripts()
        deadline = time.monotonic() + timeout_s
        sleep_s = base_sleep_s
        tid = str(txn_id)

        while True:
            args = [tid]
            if ttl_ms is not None:
                args.append(str(ttl_ms))

            result = self._scripts["acquire_exclusive"](
                keys=[exclusive_lock_key],
                args=args,
            )

            if result == 1:
                return True

            if time.monotonic() >= deadline:
                return False

            time.sleep(random.uniform(0, sleep_s))
            sleep_s = min(max_sleep_s, sleep_s * 2)

    # -----------------------
    # Private release helpers
    # -----------------------
    def _release_locks(
        self,
        shared_lock_keys: List[str],
        exclusive_lock_keys: List[str],
        txn_id: uuid.UUID,
    ) -> None:
        if self._lock_db is None:
            return
        self.initialize_scripts()

        tid = str(txn_id)

        for sk in shared_lock_keys:
            self._scripts["release_shared"](keys=[sk], args=[tid])

        for xk in exclusive_lock_keys:
            self._scripts["release_exclusive"](keys=[xk], args=[tid])

    # -----------------------
    # Public: release everything held by a txn (2PL end)
    # -----------------------
    def release_txn_locks(self, txn_id: uuid.UUID) -> None:
        """
        Call this at commit/abort. Releases all locks we recorded for txn_id.
        """
        tid = str(txn_id)
        shared_keys = list(self._txn_shared.get(tid, set()))
        exclusive_keys = list(self._txn_exclusive.get(tid, set()))

        self._release_locks(shared_keys, exclusive_keys, txn_id)
        self._forget_txn(txn_id)

    # def _set_current_txn(self, txn_id):
    #     self._local.txn_id = txn_id

    # def _get_current_txn(self):
    #     return getattr(self._local, "txn_id", None)

    # def _clear_current_txn(self):
    #     if hasattr(self._local, "txn_id"):
    #         del self._local.txn_id

    def _set_current_txn(self, txn_id):
        self._current_txn.set(txn_id)

    def _get_current_txn(self):
        return self._current_txn.get()

    def _clear_current_txn(self):
        self._current_txn.set(None)

    #########################################################
    # ---- main-db operations (used in services) ----
    #########################################################

    async def get(self, key: str):
        """Acquire S-lock and read. Lock is held until release_txn_locks()."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._db.get(key)

        shared_lock_key = self._shared_lock_key(key)
        exclusive_lock_key = self._exclusive_lock_key(key)

        if not self._acquire_shared_lock(shared_lock_key, exclusive_lock_key, txn_id):
            return None

        self._record_shared(txn_id, shared_lock_key)
        return await self._db.get(key)


    async def set(self, key: str, value, **kwargs):
        """
        If inside a transaction: acquire X-lock, record it, then write.
        If not inside a transaction: write directly.
        """
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._db.set(key, value, **kwargs)

        exclusive_lock_key = self._exclusive_lock_key(key)
        if not self._acquire_exclusive_lock(exclusive_lock_key, txn_id):
            return None

        self._record_exclusive(txn_id, exclusive_lock_key)
        return await self._db.set(key, value, **kwargs)


    async def mset(self, mapping: dict, **kwargs):
        """
        If inside a transaction: acquire X-locks for all keys (sorted),
        record them, then write. If not in a transaction: write directly.
        """
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._db.mset(mapping, **kwargs)

        keys_sorted = sorted(mapping.keys())
        acquired_exclusive: list[str] = []

        for k in keys_sorted:
            xk = self._exclusive_lock_key(k)

            # If txn already holds this lock, skip reacquiring
            if self._txn_has_exclusive_lock(xk, txn_id):
                continue

            if not self._acquire_exclusive_lock(xk, txn_id):
                # release only locks acquired in THIS mset call
                self._release_locks([], acquired_exclusive, txn_id)
                return None

            acquired_exclusive.append(xk)

        for xk in acquired_exclusive:
            self._record_exclusive(txn_id, xk)

        return await self._db.mset(mapping, **kwargs)

    async def get_for_update(self, key: str):
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._db.get(key)

        exclusive_lock_key = self._exclusive_lock_key(key)
        if not self._acquire_exclusive_lock(exclusive_lock_key, txn_id):
            return None

        self._record_exclusive(txn_id, exclusive_lock_key)
        return await self._db.get(key)

    # ---- accessors ----
    @property
    def db(self) -> "redis.Redis":
        """Underlying main Redis client (read-only)."""
        return self._db

    @property
    def lock_db(self) -> "redis.Redis | None":
        """Underlying lock Redis client (read-only)."""
        return self._lock_db
    
    @property
    def raw(self):
        """Direct async Redis client for ops that don't need locking."""
        return self._db

    def close(self) -> None:
        # self._db.close()
        if self._lock_db is not None:
            self._lock_db.close()
        


# Module-level singleton instance (keeps your `from dds_db import db` usage)
db = DDSdb_async()
lock_db = db.lock_db

atexit.register(db.close)

