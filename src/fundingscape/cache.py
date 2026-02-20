"""HTTP caching layer for idempotent data fetching."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from fundingscape import CACHE_DIR

logger = logging.getLogger(__name__)

DEFAULT_DELAY = 1.0  # seconds between requests (be a good citizen)


@dataclass
class CacheEntry:
    url: str
    status_code: int
    headers: dict[str, str]
    body: bytes
    fetched_at: float
    etag: str | None = None
    last_modified: str | None = None
    was_cached: bool = False


@dataclass
class CachedHttpClient:
    """HTTP client with filesystem caching and rate limiting."""

    cache_dir: str = CACHE_DIR
    delay: float = DEFAULT_DELAY
    timeout: float = 60.0
    _last_request_time: float = field(default=0.0, repr=False)

    def _cache_path(self, url: str, suffix: str = "") -> Path:
        """Generate cache file path from URL hash."""
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        source_dir = Path(self.cache_dir)
        source_dir.mkdir(parents=True, exist_ok=True)
        return source_dir / f"{url_hash}{suffix}"

    def _read_metadata(self, url: str) -> dict[str, Any] | None:
        """Read cached metadata for a URL."""
        meta_path = self._cache_path(url, ".meta.json")
        if meta_path.exists():
            return json.loads(meta_path.read_text())
        return None

    def _write_cache(self, url: str, entry: CacheEntry) -> None:
        """Write response to cache."""
        data_path = self._cache_path(url, ".data")
        meta_path = self._cache_path(url, ".meta.json")
        data_path.write_bytes(entry.body)
        meta = {
            "url": url,
            "status_code": entry.status_code,
            "headers": entry.headers,
            "fetched_at": entry.fetched_at,
            "etag": entry.etag,
            "last_modified": entry.last_modified,
        }
        meta_path.write_text(json.dumps(meta, indent=2))

    def _read_cache(self, url: str) -> CacheEntry | None:
        """Read response from cache."""
        data_path = self._cache_path(url, ".data")
        meta = self._read_metadata(url)
        if meta and data_path.exists():
            return CacheEntry(
                url=url,
                status_code=meta["status_code"],
                headers=meta["headers"],
                body=data_path.read_bytes(),
                fetched_at=meta["fetched_at"],
                etag=meta.get("etag"),
                last_modified=meta.get("last_modified"),
                was_cached=True,
            )
        return None

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def fetch(
        self,
        url: str,
        force: bool = False,
        headers: dict[str, str] | None = None,
    ) -> CacheEntry:
        """Fetch URL with caching. Returns CacheEntry.

        Uses ETags and Last-Modified for conditional requests.
        If cached and not modified (304), returns cached data.
        """
        if not force:
            cached = self._read_cache(url)
            if cached:
                # Try conditional request
                req_headers = dict(headers or {})
                if cached.etag:
                    req_headers["If-None-Match"] = cached.etag
                if cached.last_modified:
                    req_headers["If-Modified-Since"] = cached.last_modified

                self._rate_limit()
                try:
                    resp = httpx.get(
                        url, headers=req_headers, timeout=self.timeout,
                        follow_redirects=True,
                    )
                    if resp.status_code == 304:
                        logger.info("Cache hit (304): %s", url)
                        return cached
                except httpx.HTTPError:
                    logger.warning("Conditional request failed, using cache: %s", url)
                    return cached
            else:
                # No cache — fresh fetch
                self._rate_limit()
                resp = httpx.get(
                    url, headers=headers or {}, timeout=self.timeout,
                    follow_redirects=True,
                )
        else:
            self._rate_limit()
            resp = httpx.get(
                url, headers=headers or {}, timeout=self.timeout,
                follow_redirects=True,
            )

        resp.raise_for_status()

        entry = CacheEntry(
            url=url,
            status_code=resp.status_code,
            headers=dict(resp.headers),
            body=resp.content,
            fetched_at=time.time(),
            etag=resp.headers.get("etag"),
            last_modified=resp.headers.get("last-modified"),
        )
        self._write_cache(url, entry)
        logger.info("Fetched and cached: %s (%d bytes)", url, len(entry.body))
        return entry

    def fetch_json(self, url: str, **kwargs: Any) -> Any:
        """Fetch URL and parse as JSON."""
        entry = self.fetch(url, **kwargs)
        return json.loads(entry.body)

    def fetch_text(self, url: str, **kwargs: Any) -> str:
        """Fetch URL and return as text."""
        entry = self.fetch(url, **kwargs)
        return entry.body.decode("utf-8")
