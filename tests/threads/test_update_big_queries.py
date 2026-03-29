import time as time_module
from datetime import datetime, timedelta


from models import Season
from scripts.recompute_big_queries import recompute
from src.threads.update_big_queries import (
    UpdateBigQueriesThread,
    get_rank_distribution_func,
    get_seasons_to_update,
)

# ── helpers ───────────────────────────────────────────────────────────────────


def make_season(name, end_delta, start_delta=None):
    now = datetime.now().replace(microsecond=0)
    start = now + start_delta if start_delta else now - timedelta(days=30)
    return Season.create(name=name, start_time=start, end_time=now + end_delta)


def season_ids(seasons):
    return {s.id for s in seasons}


# ── get_seasons_to_update ─────────────────────────────────────────────────────


class TestGetSeasonsToUpdate:

    # ── basic inclusion / exclusion ───────────────────────────────────────────

    def test_empty_when_no_seasons(self):
        assert get_seasons_to_update() == []

    def test_empty_when_only_old_seasons(self):
        make_season("old", end_delta=-timedelta(hours=25))
        make_season("older", end_delta=-timedelta(days=10))
        assert get_seasons_to_update() == []

    def test_includes_active_season(self):
        s = make_season("active", end_delta=timedelta(days=30))
        assert s.id in season_ids(get_seasons_to_update())

    def test_includes_season_ended_less_than_24h_ago(self):
        s = make_season("recent", end_delta=-timedelta(hours=12))
        assert s.id in season_ids(get_seasons_to_update())

    def test_excludes_season_ended_more_than_24h_ago(self):
        s = make_season("old", end_delta=-timedelta(hours=25))
        assert s.id not in season_ids(get_seasons_to_update())

    def test_includes_both_active_and_recently_ended(self):
        active = make_season("active", end_delta=timedelta(days=30))
        recent = make_season("recent", end_delta=-timedelta(hours=6))
        ids = season_ids(get_seasons_to_update())
        assert active.id in ids
        assert recent.id in ids

    def test_old_season_excluded_when_mixed_with_active(self):
        active = make_season("active", end_delta=timedelta(days=30))
        old = make_season("old", end_delta=-timedelta(hours=30))
        ids = season_ids(get_seasons_to_update())
        assert active.id in ids
        assert old.id not in ids

    # ── boundary conditions ───────────────────────────────────────────────────

    def test_season_ended_just_inside_24h_window_is_included(self):
        # 23h 59m ago — comfortably inside
        s = make_season("borderline_in", end_delta=-timedelta(hours=23, minutes=59))
        assert s.id in season_ids(get_seasons_to_update())

    def test_season_ended_just_outside_24h_window_is_excluded(self):
        # 24h 1m ago — just outside
        s = make_season("borderline_out", end_delta=-timedelta(hours=24, minutes=1))
        assert s.id not in season_ids(get_seasons_to_update())

    # ── multiple seasons ──────────────────────────────────────────────────────

    def test_returns_all_recently_ended_seasons(self):
        s1 = make_season("r1", end_delta=-timedelta(hours=2))
        s2 = make_season("r2", end_delta=-timedelta(hours=10))
        s3 = make_season("r3", end_delta=-timedelta(hours=20))
        ids = season_ids(get_seasons_to_update())
        assert {s1.id, s2.id, s3.id}.issubset(ids)

    def test_returns_exactly_relevant_seasons_no_extras(self):
        active = make_season("active", end_delta=timedelta(days=30))
        recent = make_season("recent", end_delta=-timedelta(hours=5))
        make_season("old1", end_delta=-timedelta(hours=26))
        make_season("old2", end_delta=-timedelta(days=5))
        ids = season_ids(get_seasons_to_update())
        assert ids == {active.id, recent.id}

    # ── ordering ──────────────────────────────────────────────────────────────

    def test_ordered_by_end_time_descending(self):
        s_earlier = make_season("earlier", end_delta=-timedelta(hours=20))
        s_later = make_season("later", end_delta=-timedelta(hours=3))
        seasons = get_seasons_to_update()
        ids = [s.id for s in seasons]
        assert ids.index(s_later.id) < ids.index(s_earlier.id)

    def test_active_season_before_recently_ended(self):
        # active (end in future) should sort after recently ended when ordered desc
        # end_time of active > end_time of recently_ended
        active = make_season("active", end_delta=timedelta(days=30))
        recent = make_season("recent", end_delta=-timedelta(hours=5))
        seasons = get_seasons_to_update()
        ids = [s.id for s in seasons]
        assert ids.index(active.id) < ids.index(recent.id)


# ── UpdateBigQueriesThread.run_iteration ──────────────────────────────────────


class TestRunIteration:
    def _thread(self, monkeypatch):
        monkeypatch.setattr(time_module, "sleep", lambda _: None)
        return UpdateBigQueriesThread()

    def _capture_calls(self, thread):
        """Replace run_query with a recorder; return the calls list."""
        calls = []
        thread.run_query = lambda q, season: calls.append((q.__name__, season.id))
        return calls

    def test_no_queries_when_no_seasons(self, monkeypatch):
        thread = self._thread(monkeypatch)
        calls = self._capture_calls(thread)
        thread.run_iteration()
        assert calls == []

    def test_queries_run_for_active_season(self, monkeypatch):
        s = make_season("active", end_delta=timedelta(days=30))
        thread = self._thread(monkeypatch)
        calls = self._capture_calls(thread)
        thread.run_iteration()
        assert s.id in {sid for _, sid in calls}

    def test_queries_run_for_recently_ended_season(self, monkeypatch):
        s = make_season("recent", end_delta=-timedelta(hours=6))
        thread = self._thread(monkeypatch)
        calls = self._capture_calls(thread)
        thread.run_iteration()
        assert s.id in {sid for _, sid in calls}

    def test_queries_run_for_both_active_and_recently_ended(self, monkeypatch):
        active = make_season("active", end_delta=timedelta(days=30))
        recent = make_season("recent", end_delta=-timedelta(hours=6))
        thread = self._thread(monkeypatch)
        calls = self._capture_calls(thread)
        thread.run_iteration()
        ids_called = {sid for _, sid in calls}
        assert active.id in ids_called
        assert recent.id in ids_called

    def test_old_season_not_processed(self, monkeypatch):
        old = make_season("old", end_delta=-timedelta(hours=30))
        thread = self._thread(monkeypatch)
        calls = self._capture_calls(thread)
        thread.run_iteration()
        assert old.id not in {sid for _, sid in calls}

    def test_all_queries_executed_per_season(self, monkeypatch):
        s = make_season("active", end_delta=timedelta(days=30))
        thread = self._thread(monkeypatch)
        calls = self._capture_calls(thread)
        thread.run_iteration()
        names_called = [name for name, sid in calls if sid == s.id]
        expected_names = [q.__name__ for q in thread.get_queries(s)]
        assert names_called == expected_names

    def test_sleep_called_once_per_query_per_season(self, monkeypatch):
        s = make_season("active", end_delta=timedelta(days=30))
        sleep_calls = [0]
        monkeypatch.setattr(time_module, "sleep", lambda _: sleep_calls.__setitem__(0, sleep_calls[0] + 1))
        thread = UpdateBigQueriesThread()
        thread.run_query = lambda q, season: None
        thread.run_iteration()
        expected = len(thread.get_queries(s))
        assert sleep_calls[0] == expected

    def test_sleep_scales_with_number_of_seasons(self, monkeypatch):
        make_season("active", end_delta=timedelta(days=30))
        make_season("recent", end_delta=-timedelta(hours=6))
        sleep_calls = [0]
        monkeypatch.setattr(time_module, "sleep", lambda _: sleep_calls.__setitem__(0, sleep_calls[0] + 1))
        thread = UpdateBigQueriesThread()
        # get_queries returns the same count regardless of the specific season
        queries_per_season = len(thread.get_queries(Season.select().first()))
        thread.run_query = lambda q, season: None
        thread.run_iteration()
        assert sleep_calls[0] == queries_per_season * 2  # 2 seasons


# ── get_rank_distribution_func ────────────────────────────────────────────────


class TestGetRankDistributionFunc:

    def test_returned_function_has_correct_name(self):
        s = make_season("s", end_delta=timedelta(days=30))
        func = get_rank_distribution_func(s)
        assert func.__name__ == "get_activity_per_rank_distribution"

    def test_returns_callable(self):
        s = make_season("s", end_delta=timedelta(days=30))
        assert callable(get_rank_distribution_func(s))

    def test_different_seasons_produce_different_functions(self):
        s1 = make_season("s1", end_delta=timedelta(days=30))
        s2 = make_season("s2", end_delta=-timedelta(hours=6))
        f1 = get_rank_distribution_func(s1)
        f2 = get_rank_distribution_func(s2)
        assert f1 is not f2

    def test_get_queries_includes_rank_distribution(self):
        s = make_season("s", end_delta=timedelta(days=30))
        thread = UpdateBigQueriesThread()
        names = [q.__name__ for q in thread.get_queries(s)]
        assert "get_activity_per_rank_distribution" in names

    def test_rank_distribution_uses_correct_season_id_in_path(self, monkeypatch, tmp_path):
        """The function must read/write using the season passed to the factory,
        not Season.get_current_season()."""
        active = make_season("active", end_delta=timedelta(days=30))
        ended = make_season("ended", end_delta=-timedelta(hours=6))

        # Capture which path is opened for writing
        opened_paths = []
        real_open = open

        def tracking_open(path, mode="r", *args, **kwargs):
            if "get_activity_per_rank_distribution" in str(path):
                opened_paths.append((str(path), mode))
                if mode == "r":
                    raise FileNotFoundError  # simulate missing cache
            return real_open(path, mode, *args, **kwargs)

        monkeypatch.setattr("builtins.open", tracking_open)

        # Patch Player.select so the function doesn't need real data
        import src.threads.update_big_queries as ubq_module

        class FakeQuery:
            def join(self, *a, **kw):
                return self

            def where(self, *a, **kw):
                return self

            def dicts(self):
                return self

            def __getitem__(self, idx):
                return {"date": 0}

        monkeypatch.setattr(ubq_module.Player, "select", lambda *a, **kw: FakeQuery())

        func_for_ended = get_rank_distribution_func(ended)
        func_for_ended(ended.start_time, ended.end_time)

        # The path opened for reading/writing must contain ended.id, not active.id
        for path, _ in opened_paths:
            assert str(ended.id) in path
            assert str(active.id) not in path


# ── recompute script ──────────────────────────────────────────────────────────


class TestRecomputeScript:
    def _mock_run_query(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            UpdateBigQueriesThread,
            "run_query",
            lambda self, q, season: calls.append((q.__name__, season.id)),
        )
        return calls

    def test_runs_all_queries_for_valid_season(self, monkeypatch):
        s = make_season("s", end_delta=timedelta(days=30))
        calls = self._mock_run_query(monkeypatch)
        recompute([s.id])
        assert len(calls) > 0
        assert all(sid == s.id for _, sid in calls)

    def test_skips_nonexistent_season_id(self, monkeypatch, capsys):
        calls = self._mock_run_query(monkeypatch)
        recompute([99999])
        assert calls == []
        assert "not found" in capsys.readouterr().out

    def test_handles_multiple_valid_seasons(self, monkeypatch):
        s1 = make_season("s1", end_delta=timedelta(days=30))
        s2 = make_season("s2", end_delta=-timedelta(hours=6))
        calls = self._mock_run_query(monkeypatch)
        recompute([s1.id, s2.id])
        ids_called = {sid for _, sid in calls}
        assert s1.id in ids_called
        assert s2.id in ids_called

    def test_valid_season_processed_even_when_mixed_with_invalid(self, monkeypatch, capsys):
        s = make_season("s", end_delta=timedelta(days=30))
        calls = self._mock_run_query(monkeypatch)
        recompute([99999, s.id])
        assert s.id in {sid for _, sid in calls}
        assert "not found" in capsys.readouterr().out

    def test_all_queries_run_for_each_season(self, monkeypatch):
        s = make_season("s", end_delta=timedelta(days=30))
        calls = self._mock_run_query(monkeypatch)
        recompute([s.id])
        thread = UpdateBigQueriesThread()
        expected_names = [q.__name__ for q in thread.get_queries(s)]
        actual_names = [name for name, _ in calls]
        assert actual_names == expected_names

    def test_empty_season_ids_runs_nothing(self, monkeypatch):
        calls = self._mock_run_query(monkeypatch)
        recompute([])
        assert calls == []

    def test_no_sleep_between_queries(self, monkeypatch):
        """The script runs without the 60s inter-query sleep."""
        s = make_season("s", end_delta=timedelta(days=30))
        slept = [False]
        monkeypatch.setattr(time_module, "sleep", lambda _: slept.__setitem__(0, True))
        self._mock_run_query(monkeypatch)
        recompute([s.id])
        assert slept[0] is False
