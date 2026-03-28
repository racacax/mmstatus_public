"""
Tests for the GET /api/thread_health endpoint (APIViews.get_thread_health).

The endpoint reads cache/thread_health.json and returns derived fields.
Tests write a temp JSON file and patch HEALTH_FILE so nothing touches the
real cache directory.
"""

import json
import os
from datetime import datetime, timedelta
from unittest.mock import patch

from src.views import APIViews


# ── Helpers ───────────────────────────────────────────────────────────────────


def write_health(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def call_endpoint(tmp_path, data=None):
    """Write data to a temp file, patch HEALTH_FILE, call the endpoint."""
    health_file = str(tmp_path / "thread_health.json")
    if data is not None:
        write_health(health_file, data)
    with patch("src.views.HEALTH_FILE", health_file):
        return APIViews.get_thread_health()


def make_thread_entry(is_alive=True, start_offset_seconds=-3600, last_error_offset_seconds=None, error_count=0):
    """
    Build a single thread entry dict (as stored in the health file).
    start_offset_seconds: seconds relative to now (negative = in the past).
    last_error_offset_seconds: None means no error ever recorded.
    """
    now = datetime.now()
    start_time = now + timedelta(seconds=start_offset_seconds)
    last_error_time = (
        (now + timedelta(seconds=last_error_offset_seconds)) if last_error_offset_seconds is not None else None
    )
    return {
        "is_alive": is_alive,
        "start_time": start_time.isoformat(),
        "last_error_time": last_error_time.isoformat() if last_error_time else None,
        "error_count": error_count,
    }


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestGetThreadHealth:

    # ── file-absent cases ────────────────────────────────────────────────────

    def test_returns_503_when_file_missing(self, tmp_path):
        status, data = call_endpoint(tmp_path, data=None)  # file never written
        assert status == 503

    def test_503_body_contains_message(self, tmp_path):
        _, data = call_endpoint(tmp_path, data=None)
        assert "message" in data

    # ── file-present cases ───────────────────────────────────────────────────

    def test_returns_200_when_file_exists(self, tmp_path):
        status, _ = call_endpoint(tmp_path, {"MyThread": make_thread_entry()})
        assert status == 200

    def test_all_threads_appear_in_results(self, tmp_path):
        payload = {
            "ThreadA": make_thread_entry(),
            "ThreadB": make_thread_entry(is_alive=False),
        }
        _, data = call_endpoint(tmp_path, payload)
        assert "ThreadA" in data
        assert "ThreadB" in data

    def test_result_has_expected_keys(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry()})
        entry = data["T"]
        assert set(entry.keys()) == {"is_alive", "uptime_seconds", "seconds_since_last_error", "error_count"}

    # ── is_alive ────────────────────────────────────────────────────────────

    def test_is_alive_true_preserved(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(is_alive=True)})
        assert data["T"]["is_alive"] is True

    def test_is_alive_false_preserved(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(is_alive=False)})
        assert data["T"]["is_alive"] is False

    # ── uptime_seconds ───────────────────────────────────────────────────────

    def test_uptime_seconds_approximately_correct(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(start_offset_seconds=-3600)})
        # started 1 hour ago → uptime ≈ 3600s (allow ±2s for test execution time)
        assert abs(data["T"]["uptime_seconds"] - 3600) < 2

    def test_uptime_seconds_longer_for_older_start(self, tmp_path):
        payload = {
            "Old": make_thread_entry(start_offset_seconds=-7200),
            "New": make_thread_entry(start_offset_seconds=-600),
        }
        _, data = call_endpoint(tmp_path, payload)
        assert data["Old"]["uptime_seconds"] > data["New"]["uptime_seconds"]

    def test_uptime_seconds_is_positive(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(start_offset_seconds=-1)})
        assert data["T"]["uptime_seconds"] > 0

    def test_uptime_seconds_is_float(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry()})
        assert isinstance(data["T"]["uptime_seconds"], float)

    # ── seconds_since_last_error ─────────────────────────────────────────────

    def test_seconds_since_last_error_none_when_no_error(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(last_error_offset_seconds=None)})
        assert data["T"]["seconds_since_last_error"] is None

    def test_seconds_since_last_error_approximately_correct(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(last_error_offset_seconds=-1800)})
        # last error 30 minutes ago → ≈ 1800s
        assert abs(data["T"]["seconds_since_last_error"] - 1800) < 2

    def test_seconds_since_last_error_is_positive(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(last_error_offset_seconds=-10)})
        assert data["T"]["seconds_since_last_error"] > 0

    def test_seconds_since_last_error_is_float(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(last_error_offset_seconds=-60)})
        assert isinstance(data["T"]["seconds_since_last_error"], float)

    def test_recent_error_has_smaller_seconds_than_old_error(self, tmp_path):
        payload = {
            "Recent": make_thread_entry(last_error_offset_seconds=-60),
            "Old": make_thread_entry(last_error_offset_seconds=-3600),
        }
        _, data = call_endpoint(tmp_path, payload)
        assert data["Recent"]["seconds_since_last_error"] < data["Old"]["seconds_since_last_error"]

    # ── error_count ──────────────────────────────────────────────────────────

    def test_error_count_zero_by_default(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(error_count=0)})
        assert data["T"]["error_count"] == 0

    def test_error_count_positive_value(self, tmp_path):
        _, data = call_endpoint(tmp_path, {"T": make_thread_entry(error_count=5)})
        assert data["T"]["error_count"] == 5

    # ── empty file ───────────────────────────────────────────────────────────

    def test_empty_health_file_returns_empty_results(self, tmp_path):
        status, data = call_endpoint(tmp_path, {})
        assert status == 200
        assert data == {}
