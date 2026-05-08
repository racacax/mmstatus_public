import sys
import time as time_module
from datetime import datetime, timedelta

import pytest

from models import Game, Map
from src.threads.watchdog import WatchdogThread, STALE_THRESHOLD_MINUTES


def _make_thread(start_offset_minutes=0):
    th = WatchdogThread()
    th.start_time = datetime.now() - timedelta(minutes=start_offset_minutes)
    return th


def _insert_match(minutes_ago):
    map_o, _ = Map.get_or_create(uid="FAKE_UID")
    Game.create(
        time=datetime.now() - timedelta(minutes=minutes_ago),
        is_finished=True,
        map=map_o,
        trackmaster_limit=0,
    )


def _exit_after(th, n_calls):
    """Replace time.sleep so the handle() loop stops after n_calls iterations."""
    count = [0]

    def fake_sleep(_):
        count[0] += 1
        if count[0] >= n_calls:
            raise StopIteration

    th._fake_sleep = fake_sleep


class TestWatchdogNoExit:
    def test_no_exit_when_uptime_under_threshold(self, monkeypatch):
        """Thread must not exit while it has been alive less than STALE_THRESHOLD_MINUTES."""
        monkeypatch.setattr(sys, "exit", lambda code: (_ for _ in ()).throw(SystemExit(code)))
        th = _make_thread(start_offset_minutes=STALE_THRESHOLD_MINUTES - 1)
        _insert_match(minutes_ago=STALE_THRESHOLD_MINUTES + 5)

        _exit_after(th, 1)
        monkeypatch.setattr(time_module, "sleep", th._fake_sleep)

        with pytest.raises(StopIteration):
            th.handle()

    def test_no_exit_when_last_match_is_recent(self, monkeypatch):
        """No exit when last match is within the staleness window."""
        exited = [False]
        monkeypatch.setattr(sys, "exit", lambda _: exited.__setitem__(0, True))
        th = _make_thread(start_offset_minutes=STALE_THRESHOLD_MINUTES + 1)
        _insert_match(minutes_ago=STALE_THRESHOLD_MINUTES - 1)

        _exit_after(th, 1)
        monkeypatch.setattr(time_module, "sleep", th._fake_sleep)

        with pytest.raises(StopIteration):
            th.handle()

        assert exited[0] is False

    def test_no_exit_when_no_matches_in_db(self, monkeypatch):
        """No exit when the Game table is empty (startup condition)."""
        exited = [False]
        monkeypatch.setattr(sys, "exit", lambda _: exited.__setitem__(0, True))
        th = _make_thread(start_offset_minutes=STALE_THRESHOLD_MINUTES + 1)

        _exit_after(th, 1)
        monkeypatch.setattr(time_module, "sleep", th._fake_sleep)

        with pytest.raises(StopIteration):
            th.handle()

        assert exited[0] is False


class TestWatchdogExit:
    def test_exits_when_stale_and_uptime_sufficient(self, monkeypatch):
        """sys.exit(1) must be called when uptime >= threshold and last match is too old."""
        exit_codes = []
        monkeypatch.setattr(sys, "exit", lambda code: exit_codes.append(code))
        th = _make_thread(start_offset_minutes=STALE_THRESHOLD_MINUTES + 1)
        _insert_match(minutes_ago=STALE_THRESHOLD_MINUTES + 1)

        _exit_after(th, 2)
        monkeypatch.setattr(time_module, "sleep", th._fake_sleep)

        # handle() calls sys.exit then keeps looping; StopIteration cuts it off
        with pytest.raises(StopIteration):
            th.handle()

        assert exit_codes == [1]

    def test_exit_code_is_one(self, monkeypatch):
        exit_codes = []
        monkeypatch.setattr(sys, "exit", lambda code: exit_codes.append(code))
        th = _make_thread(start_offset_minutes=STALE_THRESHOLD_MINUTES + 1)
        _insert_match(minutes_ago=STALE_THRESHOLD_MINUTES + 1)

        _exit_after(th, 2)
        monkeypatch.setattr(time_module, "sleep", th._fake_sleep)

        with pytest.raises(StopIteration):
            th.handle()

        assert exit_codes[0] == 1

    def test_no_exit_when_exactly_at_threshold(self, monkeypatch):
        """age must be strictly greater than the threshold to trigger exit."""
        exited = [False]
        monkeypatch.setattr(sys, "exit", lambda _: exited.__setitem__(0, True))
        th = _make_thread(start_offset_minutes=STALE_THRESHOLD_MINUTES + 1)
        # last match exactly at threshold — should NOT trigger
        _insert_match(minutes_ago=STALE_THRESHOLD_MINUTES)

        _exit_after(th, 1)
        monkeypatch.setattr(time_module, "sleep", th._fake_sleep)

        with pytest.raises(StopIteration):
            th.handle()

        assert exited[0] is False

    def test_uptime_guard_uses_most_recent_match(self, monkeypatch):
        """With multiple matches, only the most recent one is checked."""
        exited = [False]
        monkeypatch.setattr(sys, "exit", lambda _: exited.__setitem__(0, True))
        th = _make_thread(start_offset_minutes=STALE_THRESHOLD_MINUTES + 1)

        _insert_match(minutes_ago=STALE_THRESHOLD_MINUTES + 30)  # old match
        _insert_match(minutes_ago=STALE_THRESHOLD_MINUTES - 1)  # recent match

        _exit_after(th, 1)
        monkeypatch.setattr(time_module, "sleep", th._fake_sleep)

        with pytest.raises(StopIteration):
            th.handle()

        assert exited[0] is False
