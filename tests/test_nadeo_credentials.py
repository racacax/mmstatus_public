"""Tests for src/nadeo_credentials.py — the centralized credential store."""

import json
import threading
import time
from datetime import datetime

import pytest

import settings
import src.nadeo_credentials as creds_module
from src.nadeo_credentials import read_credentials, write_credentials

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def reset_cache():
    """Wipe the in-process TTL cache before and after every test."""
    creds_module._cache = {}
    creds_module._cache_loaded_at = 0.0
    yield
    creds_module._cache = {}
    creds_module._cache_loaded_at = 0.0


@pytest.fixture
def creds_file(tmp_path, monkeypatch):
    """Point the module at a fresh temp file for each test."""
    path = str(tmp_path / "nadeo_credentials.json")
    monkeypatch.setattr(settings, "NADEO_CREDENTIALS_FILE", path)
    return path


def _write_raw(path: str, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f)


# ── read_credentials ──────────────────────────────────────────────────────────


class TestReadCredentials:
    def test_missing_file_returns_empty_dict(self, creds_file):
        assert read_credentials() == {}

    def test_corrupt_json_returns_empty_dict(self, creds_file):
        with open(creds_file, "w") as f:
            f.write("not json{{{")
        assert read_credentials() == {}

    def test_empty_json_object_returns_empty_dict(self, creds_file):
        _write_raw(creds_file, {})
        assert read_credentials() == {}

    def test_valid_file_returns_data(self, creds_file):
        payload = {"NadeoCore": {"access_token": "at", "refresh_token": "rt", "expire_time": "2026-01-01T00:00:00"}}
        _write_raw(creds_file, payload)
        assert read_credentials() == payload

    def test_multiple_keys_all_returned(self, creds_file):
        payload = {
            "NadeoCore": {"access_token": "at1", "refresh_token": "rt1", "expire_time": "2026-01-01T00:00:00"},
            "NadeoOauth": {"access_token": "at2", "refresh_token": "rt2", "expire_time": "2026-06-01T00:00:00"},
        }
        _write_raw(creds_file, payload)
        result = read_credentials()
        assert set(result.keys()) == {"NadeoCore", "NadeoOauth"}

    def test_returns_copy_not_reference(self, creds_file):
        _write_raw(creds_file, {"K": {"access_token": "at"}})
        result = read_credentials()
        result["K"]["access_token"] = "mutated"
        assert read_credentials()["K"]["access_token"] == "at"

    # ── TTL cache ─────────────────────────────────────────────────────────────

    def test_cache_used_within_ttl(self, creds_file):
        _write_raw(creds_file, {"K": {"access_token": "original"}})
        read_credentials()  # populate cache
        # Overwrite file without touching cache
        _write_raw(creds_file, {"K": {"access_token": "updated"}})
        assert read_credentials()["K"]["access_token"] == "original"

    def test_cache_bypassed_after_ttl(self, creds_file):
        _write_raw(creds_file, {"K": {"access_token": "original"}})
        read_credentials()  # populate cache
        creds_module._cache_loaded_at = 0.0  # expire the cache
        _write_raw(creds_file, {"K": {"access_token": "updated"}})
        assert read_credentials()["K"]["access_token"] == "updated"

    def test_empty_cache_always_hits_disk(self, creds_file):
        _write_raw(creds_file, {"K": {"access_token": "from_disk"}})
        # Cache is empty (autouse fixture wiped it), even if loaded_at is recent
        creds_module._cache_loaded_at = time.monotonic()
        assert read_credentials()["K"]["access_token"] == "from_disk"

    def test_write_immediately_refreshes_cache(self, creds_file):
        write_credentials("K", "at1", "rt1", datetime(2026, 1, 1))
        # Overwrite file behind the module's back
        _write_raw(creds_file, {"K": {"access_token": "stale"}})
        # Cache was just updated by write_credentials — should still see at1
        assert read_credentials()["K"]["access_token"] == "at1"


# ── write_credentials ─────────────────────────────────────────────────────────


class TestWriteCredentials:
    def test_creates_file_when_absent(self, creds_file):
        write_credentials("NadeoCore", "at", "rt", datetime(2026, 1, 1))
        assert json.load(open(creds_file))["NadeoCore"]["access_token"] == "at"

    def test_writes_all_three_fields(self, creds_file):
        dt = datetime(2026, 6, 15, 12, 0, 0)
        write_credentials("K", "my_access", "my_refresh", dt)
        entry = json.load(open(creds_file))["K"]
        assert entry["access_token"] == "my_access"
        assert entry["refresh_token"] == "my_refresh"
        assert entry["expire_time"] == dt.isoformat()

    def test_updates_existing_key(self, creds_file):
        write_credentials("K", "at1", "rt1", datetime(2026, 1, 1))
        write_credentials("K", "at2", "rt2", datetime(2026, 6, 1))
        entry = json.load(open(creds_file))["K"]
        assert entry["access_token"] == "at2"
        assert entry["refresh_token"] == "rt2"

    def test_preserves_other_keys(self, creds_file):
        write_credentials("A", "at_a", "rt_a", datetime(2026, 1, 1))
        write_credentials("B", "at_b", "rt_b", datetime(2026, 1, 1))
        data = json.load(open(creds_file))
        assert "A" in data and "B" in data
        assert data["A"]["access_token"] == "at_a"

    def test_no_tmp_file_left_behind(self, tmp_path, creds_file):
        write_credentials("K", "at", "rt", datetime(2026, 1, 1))
        assert not (tmp_path / "nadeo_credentials.json.tmp").exists()

    def test_three_independent_keys_coexist(self, creds_file):
        for key in ("NadeoCore", "NadeoLive", "NadeoOauth"):
            write_credentials(key, f"at_{key}", f"rt_{key}", datetime(2026, 1, 1))
        data = json.load(open(creds_file))
        assert set(data.keys()) == {"NadeoCore", "NadeoLive", "NadeoOauth"}

    # ── concurrency ───────────────────────────────────────────────────────────

    def test_concurrent_writes_no_data_loss(self, creds_file):
        """All threads write distinct keys; every key must survive."""
        errors = []

        def writer(key):
            try:
                write_credentials(key, f"at_{key}", f"rt_{key}", datetime(2026, 1, 1))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(f"Key{i}",)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        data = json.load(open(creds_file))
        assert len(data) == 20
        for i in range(20):
            assert f"Key{i}" in data

    def test_concurrent_same_key_last_write_wins(self, creds_file):
        """Concurrent writes to the same key must not corrupt the file."""
        errors = []

        def writer(token):
            try:
                write_credentials("K", token, token, datetime(2026, 1, 1))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(f"token_{i}",)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        data = json.load(open(creds_file))
        # File must be valid JSON with a single "K" entry
        assert "K" in data
        assert data["K"]["access_token"].startswith("token_")
