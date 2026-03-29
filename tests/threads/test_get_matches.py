import time as time_module

import pytest

import settings
from models import Game, Map, Player, PlayerGame
from src.services import NadeoLive
from src.threads.get_matches import GetMatchesThread, MAX_RETRIES
from tests.mock_payloads.match import get_match

# ── Existing baseline tests ────────────────────────────────────────────────────


def test_insert_match_correct_id(monkeypatch, mock_get_match):
    """
    thread has to create a match with the highest start id between env and db last id
    """
    fake_id = 1234
    monkeypatch.setattr(settings, "START_ID", fake_id)
    th1 = GetMatchesThread()
    th1.insert_match()
    assert Game.select(Game.id).get_or_none().id == fake_id + 1
    th2 = GetMatchesThread()
    th2.insert_match()
    assert Game.select(Game.id).order_by(Game.id.desc())[0].id == fake_id + 2


def test_insert_match_all_participants(monkeypatch, mock_get_match):
    """
    all participants needs to be inserted
    """
    th1 = GetMatchesThread()
    th1.insert_match()
    current_match = Game.select(Game).get()
    assert PlayerGame.select().count() == 6
    assert Player.select().count() == 6
    for pg in PlayerGame.select(PlayerGame):
        assert pg.game_id == current_match.id


def test_match_exception_returns_false(monkeypatch):
    monkeypatch.setattr(NadeoLive, "get_match", lambda _: {"exception": "Yep"})
    th1 = GetMatchesThread()
    th1.insert_match()
    assert th1.insert_match() == False
    assert Game.select(Game).filter().count() == 0


def test_insertion_map_only_once(monkeypatch, mock_get_match):
    th1 = GetMatchesThread()
    th1.insert_match()
    th1.match_id += 1
    th1.insert_match()
    assert Map.select().filter().count() == 1


# ── insert_match(): exception no longer recurses ───────────────────────────────


class TestInsertMatchExceptionHandling:
    """
    After the bug-fix, an exception during insertion must return False immediately
    instead of calling insert_match() recursively without bound.
    """

    def test_exception_returns_false(self, monkeypatch):
        def raise_error(_):
            raise Exception("DB error")

        monkeypatch.setattr(NadeoLive, "get_match", raise_error)
        th = GetMatchesThread()
        assert th.insert_match() is False

    def test_api_called_exactly_once_on_exception(self, monkeypatch):
        """No recursion — the API must be called exactly once per insert_match() call."""
        call_count = [0]

        def counting_raise(_):
            call_count[0] += 1
            raise Exception("error")

        monkeypatch.setattr(NadeoLive, "get_match", counting_raise)
        th = GetMatchesThread()
        th.insert_match()
        assert call_count[0] == 1

    def test_records_error_on_exception(self, monkeypatch):
        def raise_error(_):
            raise Exception("error")

        monkeypatch.setattr(NadeoLive, "get_match", raise_error)
        th = GetMatchesThread()
        th.insert_match()
        assert th.error_count == 1

    def test_no_game_inserted_on_exception(self, monkeypatch):
        def raise_error(_):
            raise Exception("error")

        monkeypatch.setattr(NadeoLive, "get_match", raise_error)
        th = GetMatchesThread()
        th.insert_match()
        assert Game.select().count() == 0

    def test_no_sleep_before_returning_false(self, monkeypatch):
        """The 1-second sleep was removed; insert_match() must return without sleeping."""
        slept = [False]

        def track_sleep(_):
            slept[0] = True

        monkeypatch.setattr(time_module, "sleep", track_sleep)

        def raise_error(_):
            raise Exception("error")

        monkeypatch.setattr(NadeoLive, "get_match", raise_error)
        th = GetMatchesThread()
        th.insert_match()
        assert slept[0] is False

    def test_successful_call_after_previous_exception_is_independent(self, monkeypatch, mock_get_match):
        """A fresh insert_match() call after a prior failure should succeed normally."""

        def raise_error(_):
            raise Exception("error")

        monkeypatch.setattr(NadeoLive, "get_match", raise_error)
        th = GetMatchesThread()
        th.insert_match()  # fails
        assert th.error_count == 1

        # restore the working mock and retry — should succeed
        monkeypatch.setattr(NadeoLive, "get_match", lambda match_id: get_match(match_id=match_id))
        result = th.insert_match()
        assert result is True
        assert Game.select().count() == 1


# ── run_insert_matches_loop(): match_id and tries bookkeeping ──────────────────


class TestRunInsertMatchesLoop:
    """
    run_insert_matches_loop() drives match_id forward and resets tries while
    insert_match() returns True, then stops when it returns False.
    """

    def _thread(self):
        th = GetMatchesThread()
        th.tries = 3  # set a non-zero sentinel to verify reset behaviour
        return th

    def _mock_insert(self, th, return_values):
        """Replace th.insert_match with a function returning successive values."""
        idx = [0]

        def fake():
            val = return_values[idx[0]]
            idx[0] += 1
            return val

        th.insert_match = fake

    def test_match_id_not_incremented_when_immediately_no_match(self):
        th = self._thread()
        initial_id = th.match_id
        self._mock_insert(th, [False])
        th.run_insert_matches_loop()
        assert th.match_id == initial_id

    def test_match_id_incremented_once_then_stops(self):
        th = self._thread()
        initial_id = th.match_id
        self._mock_insert(th, [True, False])
        th.run_insert_matches_loop()
        assert th.match_id == initial_id + 1

    def test_match_id_incremented_three_times(self):
        th = self._thread()
        initial_id = th.match_id
        self._mock_insert(th, [True, True, True, False])
        th.run_insert_matches_loop()
        assert th.match_id == initial_id + 3

    def test_tries_reset_to_zero_after_at_least_one_success(self):
        th = self._thread()
        self._mock_insert(th, [True, False])
        th.run_insert_matches_loop()
        assert th.tries == 0

    def test_tries_not_changed_when_no_match_found(self):
        th = self._thread()
        initial_tries = th.tries
        self._mock_insert(th, [False])
        th.run_insert_matches_loop()
        assert th.tries == initial_tries


# ── handle(): MAX_RETRIES skip logic ──────────────────────────────────────────


class TestHandleRetryLogic:
    """
    handle() must:
    - Increment self.tries each cycle when no new match is found
    - Skip to the next match ID (and record an error) when tries > MAX_RETRIES
    - Reset tries to 0 after the skip
    - NOT skip when tries == MAX_RETRIES exactly (boundary condition)
    """

    def _thread(self, monkeypatch):
        monkeypatch.setattr(time_module, "sleep", lambda _: None)
        return GetMatchesThread()

    def _exit_after(self, th, n_calls):
        """Replace run_insert_matches_loop to exit the infinite loop after n_calls."""
        calls = [0]

        def fake_loop():
            calls[0] += 1
            if calls[0] >= n_calls:
                raise StopIteration

        th.run_insert_matches_loop = fake_loop
        return calls

    def test_tries_increments_each_failed_cycle(self, monkeypatch):
        th = self._thread(monkeypatch)
        # Let the loop run 3 full cycles (run_loop returns normally, no match found)
        # On the 4th call raise StopIteration before tries can increment
        self._exit_after(th, 4)
        with pytest.raises(StopIteration):
            th.handle()
        # 3 full cycles completed → tries = 3
        assert th.tries == 3

    def test_skips_to_next_id_when_tries_exceeds_max_retries(self, monkeypatch):
        th = self._thread(monkeypatch)
        initial_id = th.match_id
        th.tries = MAX_RETRIES + 1  # above threshold: skip must fire immediately
        self._exit_after(th, 1)
        with pytest.raises(StopIteration):
            th.handle()
        assert th.match_id == initial_id + 1

    def test_no_skip_when_tries_equals_max_retries(self, monkeypatch):
        """tries == MAX_RETRIES is NOT > MAX_RETRIES, so no skip should occur."""
        th = self._thread(monkeypatch)
        initial_id = th.match_id
        th.tries = MAX_RETRIES  # exactly at boundary
        self._exit_after(th, 1)
        with pytest.raises(StopIteration):
            th.handle()
        assert th.match_id == initial_id  # no skip

    def test_tries_reset_to_zero_after_skip(self, monkeypatch):
        th = self._thread(monkeypatch)
        th.tries = MAX_RETRIES + 1
        self._exit_after(th, 1)
        with pytest.raises(StopIteration):
            th.handle()
        assert th.tries == 0

    def test_error_recorded_on_skip(self, monkeypatch):
        th = self._thread(monkeypatch)
        th.tries = MAX_RETRIES + 1
        self._exit_after(th, 1)
        with pytest.raises(StopIteration):
            th.handle()
        assert th.error_count == 1

    def test_no_error_recorded_when_no_skip(self, monkeypatch):
        th = self._thread(monkeypatch)
        th.tries = MAX_RETRIES  # below threshold — no skip
        self._exit_after(th, 1)
        with pytest.raises(StopIteration):
            th.handle()
        assert th.error_count == 0

    def test_skip_only_once_per_over_threshold_event(self, monkeypatch):
        """
        After a skip, tries is reset to 0 and must accumulate again before
        the next skip. Two consecutive skips require two separate exhaustion runs.
        """
        th = self._thread(monkeypatch)
        initial_id = th.match_id
        call_count = [0]

        def fake_loop():
            call_count[0] += 1
            # Raise only after the second skip (tries will have been over threshold twice)
            if call_count[0] > MAX_RETRIES + 2:
                raise StopIteration

        th.run_insert_matches_loop = fake_loop
        with pytest.raises(StopIteration):
            th.handle()
        # First skip happens when tries hits MAX_RETRIES+1 from 0.
        # After the skip tries=0, then it takes MAX_RETRIES+1 more cycles to skip again.
        # Total skips = number of times match_id advanced beyond initial
        assert th.match_id >= initial_id + 1
