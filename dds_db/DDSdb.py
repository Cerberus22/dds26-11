"""
Singleton Redis access for microservices.

- Holds the main Redis connection and an optional lock Redis connection.
- Both sync and async Redis clients are maintained.
- Exposes simple get/set/mset methods used by the services.
- Async methods use redis.asyncio so they never block the event loop.
"""

import asyncio
import atexit
import os
import threading
import time
import uuid
import random
from typing import Dict, List, Optional, Set

import redis
import redis.asyncio as aioredis
from contextvars import ContextVar


class DDSdb:
    """Singleton wrapper around Redis with optional lock Redis."""

    _instance: "DDSdb | None" = None
    _initialized: bool = False

    _sync_scripts = {}
    _async_scripts = {}

    _current_txn: ContextVar[uuid.UUID | None] = ContextVar('_current_txn', default=None)

    def __new__(cls) -> "DDSdb":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self.__class__._initialized:
            return

        # ---- Sync clients (used by sync get/set/mset etc.) ----
        self._db = redis.Redis(
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

        # ---- Async clients (used by async_get/async_set etc.) ----
        self._async_db = aioredis.Redis(
            host=os.environ["REDIS_HOST"],
            port=int(os.environ["REDIS_PORT"]),
            password=os.environ["REDIS_PASSWORD"],
            db=int(os.environ["REDIS_DB"]),
        )

        self._async_lock_db: "aioredis.Redis | None" = None
        if os.environ.get("LOCK_REDIS_HOST"):
            self._async_lock_db = aioredis.Redis(
                host=os.environ["LOCK_REDIS_HOST"],
                port=int(os.environ.get("LOCK_REDIS_PORT", "6379")),
                password=os.environ.get("LOCK_REDIS_PASSWORD", "redis"),
                db=int(os.environ.get("LOCK_REDIS_DB", "0")),
            )

        self.__class__._initialized = True

        # Lock tracking per transaction
        self._txn_shared: Dict[str, Set[str]] = {}
        self._txn_exclusive: Dict[str, Set[str]] = {}

        # Thread-local storage for current transaction id
        #self._local = threading.local()

        # Register Lua scripts on sync clients
        self._initialize_sync_scripts()

    # ---------------------------------------------------------------
    # Lua script registration
    # ---------------------------------------------------------------

    _LUA_ACQUIRE_SHARED = """
        -- KEYS[1] = shared lock key (SET)
        -- KEYS[2] = exclusive lock key (STRING)
        -- ARGV[1] = txn_id
        if redis.call("EXISTS", KEYS[2]) == 1 then
            return 0
        end
        redis.call("SADD", KEYS[1], ARGV[1])
        return 1
    """

    _LUA_ACQUIRE_EXCLUSIVE = """
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
    """

    _LUA_RELEASE_SHARED = """
        -- KEYS[1] = shared lock key (SET)
        -- ARGV[1] = txn_id
        redis.call("SREM", KEYS[1], ARGV[1])
        if redis.call("SCARD", KEYS[1]) == 0 then
            redis.call("DEL", KEYS[1])
        end
        return 1
    """

    _LUA_RELEASE_EXCLUSIVE = """
        -- KEYS[1] = exclusive lock key (STRING)
        -- ARGV[1] = txn_id
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end
        return 0
    """

    def _initialize_sync_scripts(self) -> None:
        if self._sync_scripts:
            return
        if self._lock_db is None:
            return
        self._sync_scripts["acquire_shared"] = self._lock_db.register_script(self._LUA_ACQUIRE_SHARED)
        self._sync_scripts["acquire_exclusive"] = self._lock_db.register_script(self._LUA_ACQUIRE_EXCLUSIVE)
        self._sync_scripts["release_shared"] = self._lock_db.register_script(self._LUA_RELEASE_SHARED)
        self._sync_scripts["release_exclusive"] = self._lock_db.register_script(self._LUA_RELEASE_EXCLUSIVE)

    async def _initialize_async_scripts(self) -> None:
        if self._async_scripts:
            return
        if self._async_lock_db is None:
            raise RuntimeError("LOCK_REDIS_* not configured, async_lock_db is None")
        self._async_scripts["acquire_shared"] = self._async_lock_db.register_script(self._LUA_ACQUIRE_SHARED)
        self._async_scripts["acquire_exclusive"] = self._async_lock_db.register_script(self._LUA_ACQUIRE_EXCLUSIVE)
        self._async_scripts["release_shared"] = self._async_lock_db.register_script(self._LUA_RELEASE_SHARED)
        self._async_scripts["release_exclusive"] = self._async_lock_db.register_script(self._LUA_RELEASE_EXCLUSIVE)

    # ---------------------------------------------------------------
    # Lock key helpers
    # ---------------------------------------------------------------

    def _shared_lock_key(self, key: str) -> str:
        return f"shared:{key}"

    def _exclusive_lock_key(self, key: str) -> str:
        return f"exclusive:{key}"

    # ---------------------------------------------------------------
    # Transaction tracking
    # ---------------------------------------------------------------

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

    # ---------------------------------------------------------------
    # Transaction context (thread-local)
    # ---------------------------------------------------------------

    def _set_current_txn(self, txn_id):
        self._current_txn.set(txn_id)

    def _get_current_txn(self):
        return self._current_txn.get()

    def _clear_current_txn(self):
        self._current_txn.set(None)

    def begin_txn(self, txn_id: uuid.UUID):
        self._set_current_txn(txn_id)

    async def async_begin_txn(self, txn_id: uuid.UUID):
        self._set_current_txn(txn_id)

    def end_txn(self, txn_id: uuid.UUID):
        self.release_txn_locks(txn_id)
        self._clear_current_txn()

    def detach_txn(self):
        self._clear_current_txn()

    async def async_detach_txn(self):
        self._clear_current_txn()

    # ---------------------------------------------------------------
    # SYNC lock acquisition (blocking — only use from sync code paths)
    # ---------------------------------------------------------------

    def _acquire_shared_lock(
        self,
        shared_lock_key: str,
        exclusive_lock_key: str,
        txn_id: uuid.UUID,
        timeout_s: float = 5.0,
        base_sleep_s: float = 0.01,
        max_sleep_s: float = 0.2,
    ) -> bool:
        self._initialize_sync_scripts()
        deadline = time.monotonic() + timeout_s
        sleep_s = base_sleep_s

        while True:
            result = self._sync_scripts["acquire_shared"](
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
        ttl_ms: Optional[int] = 5000,
        timeout_s: float = 5.0,
        base_sleep_s: float = 0.01,
        max_sleep_s: float = 0.2,
    ) -> bool:
        self._initialize_sync_scripts()
        deadline = time.monotonic() + timeout_s
        sleep_s = base_sleep_s
        tid = str(txn_id)

        while True:
            args = [tid]
            if ttl_ms is not None:
                args.append(str(ttl_ms))
            result = self._sync_scripts["acquire_exclusive"](
                keys=[exclusive_lock_key],
                args=args,
            )
            if result == 1:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(random.uniform(0, sleep_s))
            sleep_s = min(max_sleep_s, sleep_s * 2)

    # ---------------------------------------------------------------
    # ASYNC lock acquisition (non-blocking — safe for asyncio)
    # ---------------------------------------------------------------

    async def _acquire_shared_lock_async(
        self,
        shared_lock_key: str,
        exclusive_lock_key: str,
        txn_id: uuid.UUID,
        timeout_s: float = 5.0,
        base_sleep_s: float = 0.01,
        max_sleep_s: float = 0.2,
    ) -> bool:
        await self._initialize_async_scripts()
        deadline = time.monotonic() + timeout_s
        sleep_s = base_sleep_s

        while True:
            result = await self._async_scripts["acquire_shared"](
                keys=[shared_lock_key, exclusive_lock_key],
                args=[str(txn_id)],
            )
            if result == 1:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(random.uniform(0, sleep_s))
            sleep_s = min(max_sleep_s, sleep_s * 2)

    async def _acquire_exclusive_lock_async(
        self,
        exclusive_lock_key: str,
        txn_id: uuid.UUID,
        ttl_ms: Optional[int] = 10000,
        timeout_s: float = 5.0,
        base_sleep_s: float = 0.01,
        max_sleep_s: float = 0.2,
    ) -> bool:
        await self._initialize_async_scripts()
        deadline = time.monotonic() + timeout_s
        sleep_s = base_sleep_s
        tid = str(txn_id)

        while True:
            args = [tid]
            if ttl_ms is not None:
                args.append(str(ttl_ms))
            result = await self._async_scripts["acquire_exclusive"](
                keys=[exclusive_lock_key],
                args=args,
            )
            if result == 1:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(random.uniform(0, sleep_s))
            sleep_s = min(max_sleep_s, sleep_s * 2)

    # ---------------------------------------------------------------
    # SYNC lock release
    # ---------------------------------------------------------------

    def _release_locks(
        self,
        shared_lock_keys: List[str],
        exclusive_lock_keys: List[str],
        txn_id: uuid.UUID,
    ) -> None:
        if self._lock_db is None:
            return
        self._initialize_sync_scripts()
        tid = str(txn_id)
        for sk in shared_lock_keys:
            self._sync_scripts["release_shared"](keys=[sk], args=[tid])
        for xk in exclusive_lock_keys:
            self._sync_scripts["release_exclusive"](keys=[xk], args=[tid]) 


    # ---------------------------------------------------------------
    # ASYNC lock release
    # ---------------------------------------------------------------

    async def _release_locks_async(
        self,
        shared_lock_keys: List[str],
        exclusive_lock_keys: List[str],
        txn_id: uuid.UUID,
    ) -> None:
        if self._async_lock_db is None:
            return
        await self._initialize_async_scripts()
        tid = str(txn_id)
        for sk in shared_lock_keys:
            await self._async_scripts["release_shared"](keys=[sk], args=[tid])
        for xk in exclusive_lock_keys:
            await self._async_scripts["release_exclusive"](keys=[xk], args=[tid])

    # ---------------------------------------------------------------
    # Public: release all locks held by a txn
    # ---------------------------------------------------------------

    def release_txn_locks(self, txn_id: uuid.UUID) -> None:
        """Sync release — use from sync code only."""
        tid = str(txn_id)
        shared_keys = list(self._txn_shared.get(tid, set()))
        exclusive_keys = list(self._txn_exclusive.get(tid, set()))
        self._release_locks(shared_keys, exclusive_keys, txn_id)
        self._forget_txn(txn_id)

    async def release_txn_locks_async(self, txn_id: uuid.UUID) -> None:
        """Async release — use from async code."""
        tid = str(txn_id)
        shared_keys = list(self._txn_shared.get(tid, set()))
        exclusive_keys = list(self._txn_exclusive.get(tid, set()))
        await self._release_locks_async(shared_keys, exclusive_keys, txn_id)
        self._forget_txn(txn_id)

    async def async_end_txn(self, txn_id: uuid.UUID):
        """Async version of end_txn — releases locks without blocking."""
        await self.release_txn_locks_async(txn_id)
        self._clear_current_txn()

    # ###############################################################
    # SYNC data operations (for non-async code paths)
    # ###############################################################

    def get(self, key: str):
        """Acquire S-lock (sync) and read."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return self._db.get(key)

        shared_lock_key = self._shared_lock_key(key)
        exclusive_lock_key = self._exclusive_lock_key(key)

        if not self._acquire_shared_lock(shared_lock_key, exclusive_lock_key, txn_id):
            return None

        self._record_shared(txn_id, shared_lock_key)
        return self._db.get(key)

    def set(self, key: str, value, **kwargs):
        """Acquire X-lock (sync) and write."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return self._db.set(key, value, **kwargs)

        exclusive_lock_key = self._exclusive_lock_key(key)
        if not self._acquire_exclusive_lock(exclusive_lock_key, txn_id):
            return None

        self._record_exclusive(txn_id, exclusive_lock_key)
        return self._db.set(key, value, **kwargs)

    def mset(self, mapping: dict, **kwargs):
        """Acquire X-locks (sync, sorted) and write all."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return self._db.mset(mapping, **kwargs)

        keys_sorted = sorted(mapping.keys())
        acquired_exclusive: list[str] = []

        for k in keys_sorted:
            xk = self._exclusive_lock_key(k)
            if self._txn_has_exclusive_lock(xk, txn_id):
                continue
            if not self._acquire_exclusive_lock(xk, txn_id):
                self._release_locks([], acquired_exclusive, txn_id)
                return None
            acquired_exclusive.append(xk)

        for xk in acquired_exclusive:
            self._record_exclusive(txn_id, xk)

        return self._db.mset(mapping, **kwargs)

    def get_for_update(self, key: str):
        """Acquire X-lock (sync) and read. Use when you intend to write back."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return self._db.get(key)

        exclusive_lock_key = self._exclusive_lock_key(key)
        if not self._acquire_exclusive_lock(exclusive_lock_key, txn_id):
            return None

        self._record_exclusive(txn_id, exclusive_lock_key)
        return self._db.get(key)

    def delete(self, key: str):
        return self._db.delete(key)

    # ###############################################################
    # ASYNC data operations (for async service handlers)
    # All Redis I/O goes through self._async_db / self._async_lock_db
    # ###############################################################

    async def async_get(self, key: str):
        """Acquire S-lock (async) and read — never blocks the event loop."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._async_db.get(key)

        shared_lock_key = self._shared_lock_key(key)
        exclusive_lock_key = self._exclusive_lock_key(key)

        if not await self._acquire_shared_lock_async(shared_lock_key, exclusive_lock_key, txn_id):
            return None

        self._record_shared(txn_id, shared_lock_key)
        return await self._async_db.get(key)

    async def async_set(self, key: str, value, **kwargs):
        """Acquire X-lock (async) and write — never blocks the event loop."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._async_db.set(key, value, **kwargs)

        exclusive_lock_key = self._exclusive_lock_key(key)
        if not await self._acquire_exclusive_lock_async(exclusive_lock_key, txn_id):
            return None

        self._record_exclusive(txn_id, exclusive_lock_key)
        return await self._async_db.set(key, value, **kwargs)

    async def async_get_for_update(self, key: str):
        """Acquire X-lock (async) and read — never blocks the event loop."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._async_db.get(key)

        exclusive_lock_key = self._exclusive_lock_key(key)
        if not await self._acquire_exclusive_lock_async(exclusive_lock_key, txn_id):
            return None

        self._record_exclusive(txn_id, exclusive_lock_key)
        return await self._async_db.get(key)

    async def async_mset(self, mapping: dict, **kwargs):
        """Acquire X-locks (async, sorted) and write all."""
        txn_id = self._get_current_txn()
        if txn_id is None:
            return await self._async_db.mset(mapping, **kwargs)

        keys_sorted = sorted(mapping.keys())
        acquired_exclusive: list[str] = []

        for k in keys_sorted:
            xk = self._exclusive_lock_key(k)
            if await self._txn_has_exclusive_lock_async(xk, txn_id):
                continue
            if not await self._acquire_exclusive_lock_async(xk, txn_id):
                await self._release_locks_async([], acquired_exclusive, txn_id)
                return None
            acquired_exclusive.append(xk)

        for xk in acquired_exclusive:
            self._record_exclusive(txn_id, xk)

        return await self._async_db.mset(mapping, **kwargs)

    async def async_delete(self, key: str):
        return await self._async_db.delete(key)

    # ---------------------------------------------------------------
    # Sync "does this txn hold lock?" helpers
    # ---------------------------------------------------------------

    def _txn_has_shared_lock(self, shared_lock_key: str, txn_id: uuid.UUID) -> bool:
        if self._lock_db is None:
            return False
        return bool(self._lock_db.sismember(shared_lock_key, str(txn_id)))

    def _txn_has_exclusive_lock(self, exclusive_lock_key: str, txn_id: uuid.UUID) -> bool:
        if self._lock_db is None:
            return False
        owner = self._lock_db.get(exclusive_lock_key)
        if owner is None:
            return False
        return owner.decode() == str(txn_id)

    # ---------------------------------------------------------------
    # Async "does this txn hold lock?" helpers
    # ---------------------------------------------------------------

    async def _txn_has_shared_lock_async(self, shared_lock_key: str, txn_id: uuid.UUID) -> bool:
        if self._async_lock_db is None:
            return False
        return bool(await self._async_lock_db.sismember(shared_lock_key, str(txn_id)))

    async def _txn_has_exclusive_lock_async(self, exclusive_lock_key: str, txn_id: uuid.UUID) -> bool:
        if self._async_lock_db is None:
            return False
        owner = await self._async_lock_db.get(exclusive_lock_key)
        if owner is None:
            return False
        return owner.decode() == str(txn_id)

    # ---------------------------------------------------------------
    # Accessors
    # ---------------------------------------------------------------

    @property
    def db(self) -> "redis.Redis":
        """Underlying sync Redis client."""
        return self._db

    @property
    def lock_db(self) -> "redis.Redis | None":
        """Underlying sync lock Redis client."""
        return self._lock_db

    def close(self) -> None:
        self._db.close()
        if self._lock_db is not None:
            self._lock_db.close()

    async def async_close(self) -> None:
        await self._async_db.aclose()
        if self._async_lock_db is not None:
            await self._async_lock_db.aclose()
        self._db.close()
        if self._lock_db is not None:
            self._lock_db.close()


# Module-level singleton
db = DDSdb()
lock_db = db.lock_db

atexit.register(db.close)