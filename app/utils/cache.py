"""
utils/cache.py  –  Lightweight in-memory TTL cache.

Uses cachetools.TTLCache wrapped in an asyncio-safe helper.
For multi-worker deployments, swap this with Redis via aiocache.
"""
import time
import threading
from typing import Any, Optional
from cachetools import TTLCache


class SheetCache:
    """
    Thread-safe TTL cache for Google Sheets data.
    
    Single instance holds the last-fetched DataFrame.
    Auto-invalidates after `ttl` seconds so data stays fresh.
    """

    def __init__(self, ttl: int = 30):
        self._ttl = ttl
        self._cache: dict[str, Any] = {}
        self._timestamps: dict[str, float] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            if time.monotonic() - self._timestamps[key] > self._ttl:
                del self._cache[key]
                del self._timestamps[key]
                return None
            return self._cache[key]

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._cache[key] = value
            self._timestamps[key] = time.monotonic()

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)

    def last_refreshed(self, key: str) -> Optional[float]:
        with self._lock:
            return self._timestamps.get(key)

    @property
    def ttl(self) -> int:
        return self._ttl


# Module-level singleton
_sheet_cache: Optional[SheetCache] = None


def get_sheet_cache(ttl: int = 30) -> SheetCache:
    global _sheet_cache
    if _sheet_cache is None:
        _sheet_cache = SheetCache(ttl=ttl)
    return _sheet_cache