"""
Tests for src.thread_health.write_health_file.

Uses MagicMock to stand in for threading.Thread and AbstractThread instances so
the test has no dependency on manager.py (which starts threads on import).
"""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.thread_health import write_health_file

# ── Helpers ───────────────────────────────────────────────────────────────────


def make_cls(name):
    cls = MagicMock()
    cls.__name__ = name
    return cls


def make_entry(is_alive, start_time, last_error_time=None, error_count=0):
    t = MagicMock()
    t.is_alive.return_value = is_alive
    instance = MagicMock()
    instance.start_time = start_time
    instance.last_error_time = last_error_time
    instance.error_count = error_count
    return t, instance


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestWriteHealthFile:
    def _read(self, path):
        with open(path) as f:
            return json.load(f)

    def test_creates_file(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("MyThread")
        active = {cls: make_entry(True, datetime(2026, 1, 1))}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        assert out.exists()

    def test_thread_name_is_key(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("FooThread")
        active = {cls: make_entry(True, datetime(2026, 1, 1))}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        data = self._read(out)
        assert "FooThread" in data

    def test_is_alive_true_written(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("T")
        active = {cls: make_entry(True, datetime(2026, 1, 1))}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        assert self._read(out)["T"]["is_alive"] is True

    def test_is_alive_false_written(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("T")
        active = {cls: make_entry(False, datetime(2026, 1, 1))}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        assert self._read(out)["T"]["is_alive"] is False

    def test_start_time_written_as_iso(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("T")
        start = datetime(2026, 3, 27, 10, 30, 0)
        active = {cls: make_entry(True, start)}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        assert self._read(out)["T"]["start_time"] == start.isoformat()

    def test_last_error_time_null_when_none(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("T")
        active = {cls: make_entry(True, datetime(2026, 1, 1), last_error_time=None)}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        assert self._read(out)["T"]["last_error_time"] is None

    def test_last_error_time_written_as_iso_when_set(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("T")
        err_time = datetime(2026, 3, 27, 15, 0, 0)
        active = {cls: make_entry(True, datetime(2026, 1, 1), last_error_time=err_time)}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        assert self._read(out)["T"]["last_error_time"] == err_time.isoformat()

    def test_multiple_threads_all_present(self, tmp_path):
        out = tmp_path / "health.json"
        cls_a = make_cls("ThreadA")
        cls_b = make_cls("ThreadB")
        active = {
            cls_a: make_entry(True, datetime(2026, 1, 1)),
            cls_b: make_entry(False, datetime(2026, 2, 1)),
        }
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        data = self._read(out)
        assert "ThreadA" in data
        assert "ThreadB" in data

    def test_multiple_threads_independent_is_alive(self, tmp_path):
        out = tmp_path / "health.json"
        cls_a = make_cls("ThreadA")
        cls_b = make_cls("ThreadB")
        active = {
            cls_a: make_entry(True, datetime(2026, 1, 1)),
            cls_b: make_entry(False, datetime(2026, 1, 1)),
        }
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        data = self._read(out)
        assert data["ThreadA"]["is_alive"] is True
        assert data["ThreadB"]["is_alive"] is False

    def test_calls_is_alive_on_thread(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("T")
        t, instance = make_entry(True, datetime(2026, 1, 1))
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file({cls: (t, instance)})
        t.is_alive.assert_called_once()

    def test_error_count_written(self, tmp_path):
        out = tmp_path / "health.json"
        cls = make_cls("T")
        active = {cls: make_entry(True, datetime(2026, 1, 1), error_count=3)}
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file(active)
        assert self._read(out)["T"]["error_count"] == 3

    def test_empty_active_threads_writes_empty_dict(self, tmp_path):
        out = tmp_path / "health.json"
        with patch("src.thread_health.HEALTH_FILE", str(out)):
            write_health_file({})
        assert self._read(out) == {}
