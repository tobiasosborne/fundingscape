"""Tests for the HTTP caching layer."""

import json
import os
import tempfile
import time

import pytest

from fundingscape.cache import CacheEntry, CachedHttpClient


@pytest.fixture
def tmp_cache(tmp_path):
    return CachedHttpClient(cache_dir=str(tmp_path), delay=0.0)


class TestCacheEntry:
    def test_create_entry(self):
        entry = CacheEntry(
            url="https://example.com",
            status_code=200,
            headers={"content-type": "application/json"},
            body=b'{"key": "value"}',
            fetched_at=time.time(),
        )
        assert entry.status_code == 200
        assert not entry.was_cached


class TestCachedHttpClient:
    def test_cache_path_deterministic(self, tmp_cache):
        p1 = tmp_cache._cache_path("https://example.com/test")
        p2 = tmp_cache._cache_path("https://example.com/test")
        assert p1 == p2

    def test_cache_path_different_urls(self, tmp_cache):
        p1 = tmp_cache._cache_path("https://example.com/a")
        p2 = tmp_cache._cache_path("https://example.com/b")
        assert p1 != p2

    def test_write_and_read_cache(self, tmp_cache):
        url = "https://example.com/data"
        entry = CacheEntry(
            url=url,
            status_code=200,
            headers={"content-type": "text/plain"},
            body=b"hello world",
            fetched_at=time.time(),
            etag='"abc123"',
        )
        tmp_cache._write_cache(url, entry)
        cached = tmp_cache._read_cache(url)

        assert cached is not None
        assert cached.body == b"hello world"
        assert cached.etag == '"abc123"'
        assert cached.was_cached

    def test_read_cache_miss(self, tmp_cache):
        cached = tmp_cache._read_cache("https://nonexistent.com")
        assert cached is None

    def test_metadata_written(self, tmp_cache):
        url = "https://example.com/meta"
        entry = CacheEntry(
            url=url,
            status_code=200,
            headers={"x-test": "value"},
            body=b"content",
            fetched_at=1234567890.0,
            etag='"etag1"',
            last_modified="Wed, 21 Oct 2025 07:28:00 GMT",
        )
        tmp_cache._write_cache(url, entry)

        meta = tmp_cache._read_metadata(url)
        assert meta is not None
        assert meta["url"] == url
        assert meta["status_code"] == 200
        assert meta["etag"] == '"etag1"'
        assert meta["last_modified"] == "Wed, 21 Oct 2025 07:28:00 GMT"
