import json
import time as time_module
from datetime import datetime, timedelta

import pytest

from models import Game, Map, Player, PlayerGame, PlayerSeason, Season, Zone
from scripts.recompute_big_queries import recompute
from src.threads.update_big_queries import (
    H2H_ELOS,
    UpdateBigQueriesThread,
    get_activity_day_of_the_week_funcs,
    get_activity_heatmap,
    get_activity_heatmap_funcs,
    get_activity_per_day_of_the_week,
    get_clubs_leaderboard,
    get_country_h2h_func,
    get_country_h2h_funcs,
    get_cross_rank_frequency,
    get_cross_rank_frequency_funcs,
    get_hot_this_week,
    get_hot_this_week_by_points_delta,
    get_hot_this_week_by_points_delta_funcs,
    get_hot_this_week_funcs,
    get_new_players_per_week,
    get_new_players_per_week_funcs,
    get_player_retention,
    get_player_retention_funcs,
    get_players_statistics,
    get_rank_distribution_func,
    get_seasons_to_update,
    get_top_100_per_country_func,
    get_maps_rank_distribution_func,
)
from src.utils import RANKS

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


# ── get_activity_per_day_of_the_week ──────────────────────────────────────────

# Known dates — all in the same week for easy day-of-week assertions:
#   2024-01-07 = Sunday    → key "0"  (DAYOFWEEK 1 - 1)
#   2024-01-08 = Monday    → key "1"
#   2024-01-09 = Tuesday   → key "2"
#   2024-01-10 = Wednesday → key "3"
#   2024-01-11 = Thursday  → key "4"
#   2024-01-12 = Friday    → key "5"
#   2024-01-13 = Saturday  → key "6"

DOW_DATES = {
    "0": datetime(2024, 1, 7, 12, 0, 0),  # Sunday
    "1": datetime(2024, 1, 8, 12, 0, 0),  # Monday
    "2": datetime(2024, 1, 9, 12, 0, 0),  # Tuesday
    "3": datetime(2024, 1, 10, 12, 0, 0),  # Wednesday
    "4": datetime(2024, 1, 11, 12, 0, 0),  # Thursday
    "5": datetime(2024, 1, 12, 12, 0, 0),  # Friday
    "6": datetime(2024, 1, 13, 12, 0, 0),  # Saturday
}

WINDOW_START = datetime(2024, 1, 1)
WINDOW_END = datetime(2024, 12, 31)


class TestGetActivityPerDayOfTheWeek:
    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="DOW_TEST_MAP", name="DoW Test Map")

    def _game(self, map_obj, time, min_elo=1000, average_elo=1000):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=time,
            min_elo=min_elo,
            average_elo=average_elo,
        )

    def _call(self, min_elo=0, min_date=WINDOW_START, max_date=WINDOW_END):
        return json.loads(get_activity_per_day_of_the_week({"key": "other", "min_elo": min_elo}, min_date, max_date))

    # ── shape ─────────────────────────────────────────────────────────────────

    def test_results_has_exactly_seven_keys(self, map_obj):
        self._game(map_obj, DOW_DATES["0"])
        data = self._call()
        assert set(data["results"].keys()) == {"0", "1", "2", "3", "4", "5", "6"}

    def test_returns_seven_keys_even_with_no_games(self):
        data = self._call()
        assert set(data["results"].keys()) == {"0", "1", "2", "3", "4", "5", "6"}

    def test_has_last_updated_timestamp(self, map_obj):
        data = self._call()
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    # ── day encoding (DAYOFWEEK() - 1) ───────────────────────────────────────

    def test_sunday_counted_in_key_zero(self, map_obj):
        self._game(map_obj, DOW_DATES["0"])
        data = self._call()
        assert data["results"]["0"] == 1

    def test_monday_counted_in_key_one(self, map_obj):
        self._game(map_obj, DOW_DATES["1"])
        data = self._call()
        assert data["results"]["1"] == 1

    def test_saturday_counted_in_key_six(self, map_obj):
        self._game(map_obj, DOW_DATES["6"])
        data = self._call()
        assert data["results"]["6"] == 1

    def test_all_seven_days_counted_correctly(self, map_obj):
        for dt in DOW_DATES.values():
            self._game(map_obj, dt)
        data = self._call()
        for key in "0123456":
            assert data["results"][key] == 1

    # ── aggregation ───────────────────────────────────────────────────────────

    def test_multiple_games_same_day_are_summed(self, map_obj):
        for _ in range(4):
            self._game(map_obj, DOW_DATES["0"])  # 4 Sunday games
        data = self._call()
        assert data["results"]["0"] == 4

    def test_games_on_different_days_counted_independently(self, map_obj):
        self._game(map_obj, DOW_DATES["0"])
        self._game(map_obj, DOW_DATES["0"])
        self._game(map_obj, DOW_DATES["3"])
        data = self._call()
        assert data["results"]["0"] == 2
        assert data["results"]["3"] == 1

    def test_days_with_no_games_return_zero(self, map_obj):
        self._game(map_obj, DOW_DATES["1"])  # only Monday
        data = self._call()
        for key in ["0", "2", "3", "4", "5", "6"]:
            assert data["results"][key] == 0

    def test_same_day_across_two_weeks_summed(self, map_obj):
        # Two Sundays two weeks apart → both land on key "0"
        self._game(map_obj, datetime(2024, 1, 7, 12, 0))
        self._game(map_obj, datetime(2024, 1, 14, 12, 0))
        data = self._call()
        assert data["results"]["0"] == 2

    # ── min_elo filter ────────────────────────────────────────────────────────

    def test_game_below_min_elo_excluded(self, map_obj):
        self._game(map_obj, DOW_DATES["0"], min_elo=500)  # below threshold
        self._game(map_obj, DOW_DATES["1"], min_elo=1000)  # above threshold
        data = self._call(min_elo=1000)
        assert data["results"]["0"] == 0
        assert data["results"]["1"] == 1

    def test_game_at_exact_min_elo_threshold_included(self, map_obj):
        self._game(map_obj, DOW_DATES["2"], min_elo=1000)
        data = self._call(min_elo=1000)
        assert data["results"]["2"] == 1

    def test_game_one_below_min_elo_excluded(self, map_obj):
        self._game(map_obj, DOW_DATES["2"], min_elo=999)
        data = self._call(min_elo=1000)
        assert data["results"]["2"] == 0

    def test_min_elo_zero_includes_all_games(self, map_obj):
        for dt in DOW_DATES.values():
            self._game(map_obj, dt, min_elo=0)
        data = self._call(min_elo=0)
        for key in "0123456":
            assert data["results"][key] == 1

    # ── average_elo filter ────────────────────────────────────────────────────

    def test_game_with_average_elo_minus_one_excluded(self, map_obj):
        self._game(map_obj, DOW_DATES["0"], average_elo=-1)  # unprocessed, must be excluded
        self._game(map_obj, DOW_DATES["1"], average_elo=1000)
        data = self._call()
        assert data["results"]["0"] == 0
        assert data["results"]["1"] == 1

    # ── date filtering ────────────────────────────────────────────────────────

    def test_game_before_min_date_excluded(self, map_obj):
        self._game(map_obj, DOW_DATES["0"])  # 2024-01-07
        min_date = datetime(2024, 1, 8)
        data = self._call(min_date=min_date)
        assert data["results"]["0"] == 0

    def test_game_after_max_date_excluded(self, map_obj):
        self._game(map_obj, DOW_DATES["6"])  # 2024-01-13
        max_date = datetime(2024, 1, 12)
        data = self._call(max_date=max_date)
        assert data["results"]["6"] == 0

    def test_only_games_inside_date_window_counted(self, map_obj):
        self._game(map_obj, datetime(2024, 1, 6, 12))  # Saturday, before window
        self._game(map_obj, datetime(2024, 1, 8, 12))  # Monday, inside window
        self._game(map_obj, datetime(2024, 1, 14, 12))  # Sunday, after window
        min_date = datetime(2024, 1, 7)
        max_date = datetime(2024, 1, 13, 23, 59)
        data = self._call(min_date=min_date, max_date=max_date)
        assert data["results"]["1"] == 1  # Monday inside
        assert data["results"]["0"] == 0  # Sunday (Jan 14) outside
        assert data["results"]["6"] == 0  # Saturday (Jan 6) outside


# ── get_activity_day_of_the_week_funcs ────────────────────────────────────────


class TestGetActivityDayOfTheWeekFuncs:

    def test_returns_one_function_per_rank(self):
        funcs = get_activity_day_of_the_week_funcs()
        assert len(funcs) == len(RANKS)

    def test_each_function_is_callable(self):
        for func in get_activity_day_of_the_week_funcs():
            assert callable(func)

    def test_each_function_has_correct_name(self):
        funcs = get_activity_day_of_the_week_funcs()
        for func, rank in zip(funcs, RANKS):
            assert func.__name__ == f"get_activity_per_day_of_the_week_{rank['min_elo']}"

    def test_function_names_are_unique(self):
        funcs = get_activity_day_of_the_week_funcs()
        names = [f.__name__ for f in funcs]
        assert len(names) == len(set(names))

    def test_each_function_captures_correct_min_elo(self, monkeypatch):
        """Verify closure binding: each function passes its own min_elo to the query."""
        captured = []

        def fake_query(rank, min_date, max_date):
            captured.append(rank["min_elo"])
            return json.dumps({"results": {str(i): 0 for i in range(7)}, "last_updated": 0})

        import src.threads.update_big_queries as ubq

        monkeypatch.setattr(ubq, "get_activity_per_day_of_the_week", fake_query)

        # Re-build the funcs after the monkeypatch so closures reference the patched name
        funcs = get_activity_day_of_the_week_funcs()
        for func in funcs:
            func(datetime(2024, 1, 1), datetime(2024, 12, 31))

        expected_elos = [rank["min_elo"] for rank in RANKS]
        assert captured == expected_elos


# ── registration in get_queries ───────────────────────────────────────────────


class TestActivityDayOfTheWeekRegistration:

    def test_all_rank_functions_registered_in_get_queries(self):
        s = make_season("s", end_delta=timedelta(days=30))
        thread = UpdateBigQueriesThread()
        names = {q.__name__ for q in thread.get_queries(s)}
        for rank in RANKS:
            assert f"get_activity_per_day_of_the_week_{rank['min_elo']}" in names

    def test_registered_count_matches_number_of_ranks(self):
        s = make_season("s", end_delta=timedelta(days=30))
        thread = UpdateBigQueriesThread()
        names = [q.__name__ for q in thread.get_queries(s)]
        dow_names = [n for n in names if n.startswith("get_activity_per_day_of_the_week_")]
        assert len(dow_names) == len(RANKS)


# ── get_player_retention ──────────────────────────────────────────────────────
#
# Week numbering: FLOOR(DATEDIFF(game.time, min_date) / 7)
#   week 0 = days 0-6 from min_date
#   week 1 = days 7-13, etc.
#
# The first result entry appears once both week 0 and week 1 have data.
# The last week in the season is always skipped (no following week to compare).


class TestGetPlayerRetention:
    # Anchor date — all test games are placed relative to this
    BASE = datetime(2024, 1, 1, 12, 0, 0)
    WINDOW_END = datetime(2024, 12, 31)

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="RETENTION_TEST_MAP", name="Retention Map")

    @pytest.fixture
    def player_a(self):
        from uuid import UUID

        return Player.create(uuid=UUID("aaaaaaaa-0000-0000-0000-000000000001"), name="PlayerA")

    @pytest.fixture
    def player_b(self):
        from uuid import UUID

        return Player.create(uuid=UUID("bbbbbbbb-0000-0000-0000-000000000002"), name="PlayerB")

    @pytest.fixture
    def player_c(self):
        from uuid import UUID

        return Player.create(uuid=UUID("cccccccc-0000-0000-0000-000000000003"), name="PlayerC")

    def _game(self, map_obj, days_offset, min_elo=1000, average_elo=1000):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=self.BASE + timedelta(days=days_offset),
            min_elo=min_elo,
            average_elo=average_elo,
        )

    def _pg(self, player, game):
        return PlayerGame.create(player=player, game=game)

    def _call(self, min_elo=0, min_date=None, max_date=None):
        return json.loads(
            get_player_retention(
                {"key": "other", "min_elo": min_elo},
                min_date or self.BASE,
                max_date or self.WINDOW_END,
            )
        )

    def _week(self, data, n):
        return next((r for r in data["results"] if r["week"] == n), None)

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_empty_results_with_no_games(self):
        data = self._call()
        assert data["results"] == []

    def test_has_last_updated(self):
        data = self._call()
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    def test_single_week_produces_no_results(self, map_obj, player_a):
        # All games in week 0 only — no week 1 to compare to
        self._pg(player_a, self._game(map_obj, days_offset=0))
        self._pg(player_a, self._game(map_obj, days_offset=3))
        data = self._call()
        assert data["results"] == []

    def test_result_row_has_expected_keys(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, days_offset=0))  # week 0
        self._pg(player_a, self._game(map_obj, days_offset=7))  # week 1
        data = self._call()
        row = data["results"][0]
        assert set(row.keys()) == {"week", "week_start", "total_players", "retained_players", "retention_rate"}

    # ── week numbering ────────────────────────────────────────────────────────

    def test_days_0_to_6_are_week_zero(self, map_obj, player_a, player_b):
        # player_a plays on day 0 and day 6 (both week 0)
        # player_b plays on day 7 (week 1) to give a comparison target
        self._pg(player_a, self._game(map_obj, days_offset=0))
        self._pg(player_a, self._game(map_obj, days_offset=6))
        self._pg(player_b, self._game(map_obj, days_offset=7))
        data = self._call()
        w0 = self._week(data, 0)
        assert w0 is not None
        assert w0["total_players"] == 1  # only player_a in week 0

    def test_day_7_is_week_one(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj, days_offset=0))  # week 0
        self._pg(player_b, self._game(map_obj, days_offset=7))  # week 1
        self._pg(player_b, self._game(map_obj, days_offset=14))  # week 2 (to make week 1 show up)
        data = self._call()
        w1 = self._week(data, 1)
        assert w1 is not None
        assert w1["total_players"] == 1  # only player_b in week 1

    # ── retention counting ────────────────────────────────────────────────────

    def test_full_retention_when_all_players_return(self, map_obj, player_a, player_b):
        # Both players in week 0 and week 1
        for p in [player_a, player_b]:
            self._pg(p, self._game(map_obj, days_offset=0))
            self._pg(p, self._game(map_obj, days_offset=7))
        data = self._call()
        w0 = self._week(data, 0)
        assert w0["total_players"] == 2
        assert w0["retained_players"] == 2
        assert w0["retention_rate"] == 100.0

    def test_zero_retention_when_no_players_return(self, map_obj, player_a, player_b):
        # player_a in week 0, player_b in week 1 (no overlap)
        self._pg(player_a, self._game(map_obj, days_offset=0))
        self._pg(player_b, self._game(map_obj, days_offset=7))
        data = self._call()
        w0 = self._week(data, 0)
        assert w0["total_players"] == 1
        assert w0["retained_players"] == 0
        assert w0["retention_rate"] == 0.0

    def test_partial_retention(self, map_obj, player_a, player_b, player_c):
        # All 3 play week 0; only player_a and player_b return for week 1
        for p in [player_a, player_b, player_c]:
            self._pg(p, self._game(map_obj, days_offset=0))
        for p in [player_a, player_b]:
            self._pg(p, self._game(map_obj, days_offset=7))
        data = self._call()
        w0 = self._week(data, 0)
        assert w0["total_players"] == 3
        assert w0["retained_players"] == 2
        assert abs(w0["retention_rate"] - 66.67) < 0.01

    def test_multiple_games_same_week_count_player_once(self, map_obj, player_a, player_b):
        # player_a plays 5 times in week 0 — should still count as 1 player
        for day in [0, 1, 2, 3, 4]:
            self._pg(player_a, self._game(map_obj, days_offset=day))
        self._pg(player_b, self._game(map_obj, days_offset=7))  # week 1
        data = self._call()
        w0 = self._week(data, 0)
        assert w0["total_players"] == 1

    # ── multi-week series ─────────────────────────────────────────────────────

    def test_each_week_pair_produces_independent_result(self, map_obj, player_a, player_b, player_c):
        # week 0: A, B — week 1: A, C — week 2: B, C
        self._pg(player_a, self._game(map_obj, days_offset=0))
        self._pg(player_b, self._game(map_obj, days_offset=0))
        self._pg(player_a, self._game(map_obj, days_offset=7))
        self._pg(player_c, self._game(map_obj, days_offset=7))
        self._pg(player_b, self._game(map_obj, days_offset=14))
        self._pg(player_c, self._game(map_obj, days_offset=14))
        data = self._call()
        w0 = self._week(data, 0)
        w1 = self._week(data, 1)
        # week 0→1: A and B in w0; only A in w1 → 1/2 retained
        assert w0["total_players"] == 2
        assert w0["retained_players"] == 1
        # week 1→2: A and C in w1; only C in w2 → 1/2 retained
        assert w1["total_players"] == 2
        assert w1["retained_players"] == 1

    def test_last_week_not_included_when_no_following_week(self, map_obj, player_a):
        # Only week 0 and week 1 data → week 0 shows up, week 1 does not
        self._pg(player_a, self._game(map_obj, days_offset=0))
        self._pg(player_a, self._game(map_obj, days_offset=7))
        data = self._call()
        assert self._week(data, 0) is not None
        assert self._week(data, 1) is None

    def test_week_start_timestamp_is_correct(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj, days_offset=0))
        self._pg(player_b, self._game(map_obj, days_offset=7))
        data = self._call()
        w0 = self._week(data, 0)
        expected_ts = self.BASE.timestamp()
        assert abs(w0["week_start"] - expected_ts) < 1

    # ── min_elo filter ────────────────────────────────────────────────────────

    def test_game_below_min_elo_excluded_from_both_weeks(self, map_obj, player_a, player_b):
        # player_a plays low-elo game in week 0; player_b plays qualifying game in both weeks
        self._pg(player_a, self._game(map_obj, days_offset=0, min_elo=500))  # excluded
        self._pg(player_b, self._game(map_obj, days_offset=0, min_elo=1000))  # included
        self._pg(player_b, self._game(map_obj, days_offset=7, min_elo=1000))  # included
        data = self._call(min_elo=1000)
        w0 = self._week(data, 0)
        assert w0["total_players"] == 1  # player_a not counted
        assert w0["retained_players"] == 1

    def test_player_counts_if_any_qualifying_game_in_week(self, map_obj, player_a, player_b):
        # player_a has one low-elo and one qualifying game in week 0 → still counted
        self._pg(player_a, self._game(map_obj, days_offset=0, min_elo=500))  # excluded
        self._pg(player_a, self._game(map_obj, days_offset=2, min_elo=1000))  # qualifies
        self._pg(player_b, self._game(map_obj, days_offset=7, min_elo=1000))  # week 1
        data = self._call(min_elo=1000)
        w0 = self._week(data, 0)
        assert w0["total_players"] == 1  # player_a counted via qualifying game

    # ── average_elo filter ────────────────────────────────────────────────────

    def test_games_with_average_elo_minus_one_excluded(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj, days_offset=0, average_elo=-1))  # unprocessed
        self._pg(player_b, self._game(map_obj, days_offset=0, average_elo=1000))
        self._pg(player_b, self._game(map_obj, days_offset=7, average_elo=1000))
        data = self._call()
        w0 = self._week(data, 0)
        assert w0["total_players"] == 1  # player_a excluded


# ── get_player_retention_funcs ────────────────────────────────────────────────


class TestGetPlayerRetentionFuncs:

    def test_returns_one_function_per_rank(self):
        assert len(get_player_retention_funcs()) == len(RANKS)

    def test_each_function_has_correct_name(self):
        for func, rank in zip(get_player_retention_funcs(), RANKS):
            assert func.__name__ == f"get_player_retention_{rank['min_elo']}"

    def test_function_names_are_unique(self):
        names = [f.__name__ for f in get_player_retention_funcs()]
        assert len(names) == len(set(names))

    def test_each_function_captures_correct_min_elo(self, monkeypatch):
        captured = []

        def fake_retention(rank, min_date, max_date):
            captured.append(rank["min_elo"])
            return json.dumps({"results": [], "last_updated": 0})

        import src.threads.update_big_queries as ubq

        monkeypatch.setattr(ubq, "get_player_retention", fake_retention)

        funcs = get_player_retention_funcs()
        for func in funcs:
            func(datetime(2024, 1, 1), datetime(2024, 12, 31))

        assert captured == [rank["min_elo"] for rank in RANKS]


# ── registration in get_queries ───────────────────────────────────────────────


class TestPlayerRetentionRegistration:

    def test_all_rank_functions_registered_in_get_queries(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = {q.__name__ for q in UpdateBigQueriesThread().get_queries(s)}
        for rank in RANKS:
            assert f"get_player_retention_{rank['min_elo']}" in names

    def test_registered_count_matches_number_of_ranks(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        retention_names = [n for n in names if n.startswith("get_player_retention_")]
        assert len(retention_names) == len(RANKS)


# ── get_hot_this_week ─────────────────────────────────────────────────────────
#
# Returns top 20 players by wins in the last 7 days (relative to datetime.now()).
# Filterable by min_elo (Player.points >= min_elo — current player points, not game min_elo).
# All games in the 7-day window count regardless of the game's own min_elo.
# _min_date and _max_date are accepted but ignored.


class TestGetHotThisWeek:
    """Tests for get_hot_this_week()."""

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="HOT_WEEK_MAP", name="Hot Week Map")

    @pytest.fixture
    def player_a(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("aaaaaaaa-0000-0000-0000-000000000011"), name="PlayerA", club_tag="TAG_A", points=2000
        )

    @pytest.fixture
    def player_b(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("bbbbbbbb-0000-0000-0000-000000000012"), name="PlayerB", club_tag="TAG_B", points=2000
        )

    @pytest.fixture
    def player_c(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("cccccccc-0000-0000-0000-000000000013"), name="PlayerC", club_tag="TAG_C", points=2000
        )

    def _game(self, map_obj, days_ago=1, min_elo=0, average_elo=1000):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=datetime.now() - timedelta(days=days_ago),
            min_elo=min_elo,
            average_elo=average_elo,
        )

    def _pg(self, player, game, is_win=False):
        return PlayerGame.create(player=player, game=game, is_win=is_win)

    def _call(self, min_elo=0, min_rank=None):
        return json.loads(get_hot_this_week(min_elo, min_rank, None, None))

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_empty_when_no_games(self):
        data = self._call()
        assert data["results"] == []

    def test_has_last_updated(self):
        data = self._call()
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    def test_result_row_has_expected_keys(self, map_obj, player_a):
        g = self._game(map_obj)
        self._pg(player_a, g, is_win=True)
        data = self._call()
        row = data["results"][0]
        assert set(row.keys()) == {"name", "uuid", "club_tag", "country", "current_points", "wins", "played"}

    def test_uuid_is_string(self, map_obj, player_a):
        g = self._game(map_obj)
        self._pg(player_a, g, is_win=True)
        data = self._call()
        assert isinstance(data["results"][0]["uuid"], str)

    def test_current_points_is_player_points(self, map_obj, player_a):
        player_a.points = 2500
        player_a.save()
        self._pg(player_a, self._game(map_obj), is_win=True)
        data = self._call()
        assert data["results"][0]["current_points"] == 2500

    def test_country_is_none_when_no_country(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj), is_win=True)
        data = self._call()
        assert data["results"][0]["country"] is None

    # ── ordering ──────────────────────────────────────────────────────────────

    def test_ordered_by_wins_descending(self, map_obj, player_a, player_b, player_c):
        # player_c: 3 wins, player_a: 2 wins, player_b: 1 win
        for _ in range(3):
            self._pg(player_c, self._game(map_obj), is_win=True)
        for _ in range(2):
            self._pg(player_a, self._game(map_obj), is_win=True)
        self._pg(player_b, self._game(map_obj), is_win=True)
        data = self._call()
        wins = [r["wins"] for r in data["results"]]
        assert wins == sorted(wins, reverse=True)
        assert data["results"][0]["name"] == "PlayerC"

    def test_max_20_results(self, map_obj):
        from uuid import UUID

        for i in range(25):
            p = Player.create(uuid=UUID(f"dddddddd-0000-0000-0000-{i:012d}"), name=f"P{i}")
            self._pg(p, self._game(map_obj), is_win=True)
        data = self._call()
        assert len(data["results"]) <= 20

    # ── time window ───────────────────────────────────────────────────────────

    def test_game_within_7_days_included(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, days_ago=6), is_win=True)
        data = self._call()
        assert len(data["results"]) == 1

    def test_game_older_than_7_days_excluded(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, days_ago=8), is_win=True)
        data = self._call()
        assert data["results"] == []

    def test_min_date_max_date_args_are_ignored(self, map_obj, player_a):
        """_min_date and _max_date params have no effect on the 7-day window."""
        g = self._game(map_obj, days_ago=1)
        self._pg(player_a, g, is_win=True)
        # pass far-past dates that would normally exclude this game
        far_past = datetime(2000, 1, 1)
        data = json.loads(get_hot_this_week(0, None, far_past, far_past))
        assert len(data["results"]) == 1

    # ── min_elo filter (based on Player.points, not Game.min_elo) ────────────

    def test_player_above_threshold_included(self, map_obj):
        from uuid import UUID

        p = Player.create(uuid=UUID("eeeeeeee-0000-0000-0000-000000000011"), name="HighPts", points=2000)
        self._pg(p, self._game(map_obj), is_win=True)
        data = self._call(min_elo=1000)
        assert len(data["results"]) == 1

    def test_player_below_threshold_excluded(self, map_obj):
        from uuid import UUID

        p = Player.create(uuid=UUID("eeeeeeee-0000-0000-0000-000000000012"), name="LowPts", points=500)
        self._pg(p, self._game(map_obj), is_win=True)
        data = self._call(min_elo=1000)
        assert data["results"] == []

    def test_game_with_low_min_elo_counts_if_player_qualifies(self, map_obj):
        from uuid import UUID

        # Player has high points but game was played at low min_elo — should still appear
        p = Player.create(uuid=UUID("eeeeeeee-0000-0000-0000-000000000013"), name="HighPtsLowGame", points=2000)
        self._pg(p, self._game(map_obj, min_elo=0), is_win=True)
        data = self._call(min_elo=1000)
        assert len(data["results"]) == 1

    def test_min_elo_zero_includes_all(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj), is_win=True)
        self._pg(player_b, self._game(map_obj), is_win=True)
        data = self._call(min_elo=0)
        names = {r["name"] for r in data["results"]}
        assert "PlayerA" in names and "PlayerB" in names

    # ── average_elo filter (invalid games excluded) ───────────────────────────

    def test_game_with_average_elo_minus_1_excluded(self, map_obj, player_a):
        g = self._game(map_obj, average_elo=-1)
        self._pg(player_a, g, is_win=True)
        data = self._call()
        assert data["results"] == []

    # ── wins vs played counts ─────────────────────────────────────────────────

    def test_wins_and_played_counts_are_correct(self, map_obj, player_a):
        for _ in range(3):
            self._pg(player_a, self._game(map_obj), is_win=True)
        for _ in range(2):
            self._pg(player_a, self._game(map_obj), is_win=False)
        data = self._call()
        row = data["results"][0]
        assert row["wins"] == 3
        assert row["played"] == 5

    def test_player_with_zero_wins_included_in_played_count(self, map_obj, player_a):
        for _ in range(3):
            self._pg(player_a, self._game(map_obj), is_win=False)
        data = self._call()
        # player_a should appear since they played
        assert len(data["results"]) == 1
        assert data["results"][0]["wins"] == 0
        assert data["results"][0]["played"] == 3

    # ── player isolation ──────────────────────────────────────────────────────

    def test_player_isolation(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj), is_win=True)
        self._pg(player_a, self._game(map_obj), is_win=True)
        self._pg(player_b, self._game(map_obj), is_win=True)
        data = self._call()
        by_name = {r["name"]: r for r in data["results"]}
        assert by_name["PlayerA"]["wins"] == 2
        assert by_name["PlayerB"]["wins"] == 1

    # ── club_tag ──────────────────────────────────────────────────────────────

    def test_club_tag_is_returned(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj), is_win=True)
        data = self._call()
        assert data["results"][0]["club_tag"] == "TAG_A"


# ── get_hot_this_week_funcs ───────────────────────────────────────────────────

HOT_RANKS = [r for r in RANKS if r["min_elo"] >= 3000]


class TestGetHotThisWeekFuncs:

    def test_returns_one_func_per_rank(self):
        assert len(get_hot_this_week_funcs()) == len(HOT_RANKS)

    def test_func_names_match_rank_min_elo(self):
        for func, rank in zip(get_hot_this_week_funcs(), HOT_RANKS):
            assert func.__name__ == f"get_hot_this_week_{rank['min_elo']}"

    def test_no_late_binding_all_names_distinct(self):
        names = [f.__name__ for f in get_hot_this_week_funcs()]
        assert len(names) == len(set(names))

    def test_func_delegates_to_get_hot_this_week(self, monkeypatch):
        import src.threads.update_big_queries as ubq

        calls = []

        def fake_query(min_elo, min_rank, min_date, max_date):
            calls.append((min_elo, min_rank))
            return json.dumps({"last_updated": 0.0, "results": []})

        monkeypatch.setattr(ubq, "get_hot_this_week", fake_query)
        funcs = get_hot_this_week_funcs()
        funcs[0](datetime.now() - timedelta(days=30), datetime.now())
        assert calls == [(RANKS[0]["min_elo"], RANKS[0]["min_rank"])]


# ── get_hot_this_week registration ───────────────────────────────────────────


class TestHotThisWeekRegistration:

    def test_all_rank_functions_registered_in_get_queries(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = {q.__name__ for q in UpdateBigQueriesThread().get_queries(s)}
        for rank in HOT_RANKS:
            assert f"get_hot_this_week_{rank['min_elo']}" in names

    def test_registered_count_matches_number_of_ranks(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        hot_names = [n for n in names if n.startswith("get_hot_this_week_") and "points_delta" not in n]
        assert len(hot_names) == len(HOT_RANKS)

    def test_points_delta_registered_for_all_ranks(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        delta_names = [n for n in names if n.startswith("get_hot_this_week_by_points_delta_")]
        assert len(delta_names) == len(HOT_RANKS)


# ── get_hot_this_week_by_points_delta ─────────────────────────────────────────
# Top 20 players by net points gained in the last 7 days.
# delta = Player.points (current) - points_after_match of first game of the week.
# Filter: Player.points >= min_elo (current player points, not game min_elo).
# All games in the window count regardless of the game's own min_elo.
# Requires points_after_match to be set; games with NULL are ignored.
# _min_date and _max_date are accepted but ignored.


class TestGetHotThisWeekByPointsDelta:
    """Tests for get_hot_this_week_by_points_delta()."""

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="DELTA_MAP", name="Delta Map")

    @pytest.fixture
    def player_a(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("aaaaaaaa-0000-0000-0000-000000000021"), name="PlayerA", club_tag="TAG_A", points=3000
        )

    @pytest.fixture
    def player_b(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("bbbbbbbb-0000-0000-0000-000000000022"), name="PlayerB", club_tag="TAG_B", points=3000
        )

    @pytest.fixture
    def player_c(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("cccccccc-0000-0000-0000-000000000023"), name="PlayerC", club_tag="TAG_C", points=3000
        )

    def _game(self, map_obj, days_ago=1, min_elo=0, average_elo=1000):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=datetime.now() - timedelta(days=days_ago, seconds=days_ago),
            min_elo=min_elo,
            average_elo=average_elo,
        )

    def _pg(self, player, game, points_after=None):
        return PlayerGame.create(player=player, game=game, points_after_match=points_after)

    def _call(self, min_elo=0, min_rank=None):
        return json.loads(get_hot_this_week_by_points_delta(min_elo, min_rank, None, None))

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_empty_when_no_games(self):
        data = self._call()
        assert data["results"] == []

    def test_has_last_updated(self):
        data = self._call()
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    def test_result_row_has_expected_keys(self, map_obj, player_a):
        g = self._game(map_obj)
        self._pg(player_a, g, points_after=3100)
        data = self._call()
        row = data["results"][0]
        assert set(row.keys()) == {"name", "uuid", "club_tag", "country", "current_points", "delta", "played"}

    def test_uuid_is_string(self, map_obj, player_a):
        g = self._game(map_obj)
        self._pg(player_a, g, points_after=3100)
        data = self._call()
        assert isinstance(data["results"][0]["uuid"], str)

    def test_current_points_is_player_points(self, map_obj, player_a):
        player_a.points = 3200
        player_a.save()
        g = self._game(map_obj)
        self._pg(player_a, g, points_after=3200)
        data = self._call()
        assert data["results"][0]["current_points"] == 3200

    def test_country_is_none_when_no_country(self, map_obj, player_a):
        g = self._game(map_obj)
        self._pg(player_a, g, points_after=3000)
        data = self._call()
        assert data["results"][0]["country"] is None

    def test_played_counts_all_games_including_without_points_after(self, map_obj, player_a):
        # 2 games with points_after, 1 without — played should be 3
        player_a.points = 3100
        player_a.save()
        self._pg(player_a, self._game(map_obj, days_ago=3), points_after=3000)
        self._pg(player_a, self._game(map_obj, days_ago=2), points_after=None)
        self._pg(player_a, self._game(map_obj, days_ago=1), points_after=3100)
        data = self._call()
        assert data["results"][0]["played"] == 3

    # ── delta computation ─────────────────────────────────────────────────────
    # delta = Player.points (current) - points_after_match of first game this week

    def test_single_game_has_delta_zero(self, map_obj, player_a):
        # player.points = 3000, first (and only) game has points_after=3000 → delta=0
        player_a.points = 3000
        player_a.save()
        g = self._game(map_obj)
        self._pg(player_a, g, points_after=3000)
        data = self._call()
        assert data["results"][0]["delta"] == 0

    def test_delta_is_current_points_minus_first_game(self, map_obj, player_a):
        # First game: points_after=3000; player currently at 3200 → delta=200
        player_a.points = 3200
        player_a.save()
        g1 = self._game(map_obj, days_ago=3)
        g2 = self._game(map_obj, days_ago=1)
        self._pg(player_a, g1, points_after=3000)
        self._pg(player_a, g2, points_after=3150)
        data = self._call()
        assert data["results"][0]["delta"] == 200

    def test_negative_delta_when_points_dropped(self, map_obj, player_a):
        # First game: points_after=3200; player now at 3000 → delta=-200
        player_a.points = 3000
        player_a.save()
        g1 = self._game(map_obj, days_ago=3)
        g2 = self._game(map_obj, days_ago=1)
        self._pg(player_a, g1, points_after=3200)
        self._pg(player_a, g2, points_after=3050)
        data = self._call()
        assert data["results"][0]["delta"] == -200

    def test_played_count_is_correct(self, map_obj, player_a):
        for i in range(4):
            g = self._game(map_obj, days_ago=i + 1)
            self._pg(player_a, g, points_after=3000 + i * 50)
        data = self._call()
        assert data["results"][0]["played"] == 4

    # ── ordering ──────────────────────────────────────────────────────────────

    def test_ordered_by_delta_descending(self, map_obj, player_a, player_b, player_c):
        # player_c: current=3300 first=3000 → +300; player_a: 3200-3000=+200; player_b: 3100-3000=+100
        for player, current in [(player_c, 3300), (player_a, 3200), (player_b, 3100)]:
            player.points = current
            player.save()
            g1 = self._game(map_obj, days_ago=3)
            g2 = self._game(map_obj, days_ago=1)
            self._pg(player, g1, points_after=3000)
            self._pg(player, g2, points_after=current - 50)
        data = self._call()
        deltas = [r["delta"] for r in data["results"]]
        assert deltas == sorted(deltas, reverse=True)
        assert data["results"][0]["name"] == "PlayerC"

    def test_max_20_results(self, map_obj):
        from uuid import UUID

        for i in range(25):
            p = Player.create(uuid=UUID(f"dddddddd-0000-0000-0000-{i:012d}"), name=f"P{i}", points=3000 + i)
            g = self._game(map_obj, days_ago=1)
            self._pg(p, g, points_after=3000)
        data = self._call()
        assert len(data["results"]) <= 20

    # ── time window ───────────────────────────────────────────────────────────

    def test_game_within_7_days_included(self, map_obj, player_a):
        g = self._game(map_obj, days_ago=6)
        self._pg(player_a, g, points_after=3000)
        data = self._call()
        assert len(data["results"]) == 1

    def test_game_older_than_7_days_excluded(self, map_obj, player_a):
        g = self._game(map_obj, days_ago=8)
        self._pg(player_a, g, points_after=3000)
        data = self._call()
        assert data["results"] == []

    def test_min_date_max_date_args_are_ignored(self, map_obj, player_a):
        g = self._game(map_obj, days_ago=1)
        self._pg(player_a, g, points_after=3000)
        far_past = datetime(2000, 1, 1)
        data = json.loads(get_hot_this_week_by_points_delta(0, None, far_past, far_past))
        assert len(data["results"]) == 1

    # ── null points_after_match excluded ──────────────────────────────────────

    def test_game_with_null_points_after_excluded(self, map_obj, player_a):
        g = self._game(map_obj, days_ago=1)
        self._pg(player_a, g, points_after=None)
        data = self._call()
        assert data["results"] == []

    def test_only_null_points_games_produces_empty(self, map_obj, player_a):
        for i in range(3):
            g = self._game(map_obj, days_ago=i + 1)
            self._pg(player_a, g, points_after=None)
        data = self._call()
        assert data["results"] == []

    # ── min_elo filter (based on Player.points, not Game.min_elo) ────────────

    def test_player_above_threshold_included(self, map_obj, player_a):
        player_a.points = 3000
        player_a.save()
        g = self._game(map_obj)
        self._pg(player_a, g, points_after=3000)
        data = self._call(min_elo=1000)
        assert len(data["results"]) == 1

    def test_player_below_threshold_excluded(self, map_obj):
        from uuid import UUID

        p = Player.create(uuid=UUID("eeeeeeee-0000-0000-0000-000000000021"), name="LowPts", points=500)
        g = self._game(map_obj)
        self._pg(p, g, points_after=500)
        data = self._call(min_elo=1000)
        assert data["results"] == []

    def test_game_with_low_min_elo_counts_if_player_qualifies(self, map_obj, player_a):
        # Game's min_elo is 0 but player.points=3000 >= threshold=1000 → included
        player_a.points = 3000
        player_a.save()
        g = self._game(map_obj, min_elo=0)
        self._pg(player_a, g, points_after=3000)
        data = self._call(min_elo=1000)
        assert len(data["results"]) == 1

    # ── average_elo filter ────────────────────────────────────────────────────

    def test_game_with_average_elo_minus_1_excluded(self, map_obj, player_a):
        g = self._game(map_obj, average_elo=-1)
        self._pg(player_a, g, points_after=3000)
        data = self._call()
        assert data["results"] == []

    # ── player isolation ──────────────────────────────────────────────────────

    def test_player_isolation(self, map_obj, player_a, player_b):
        # player_a: current=3200, first_game=3000 → delta=200
        # player_b: current=3050, first_game=3000 → delta=50
        player_a.points = 3200
        player_a.save()
        player_b.points = 3050
        player_b.save()
        for i, (player, first) in enumerate([(player_a, 3000), (player_b, 3000)]):
            g1 = self._game(map_obj, days_ago=3 + i)
            g2 = self._game(map_obj, days_ago=1)
            self._pg(player, g1, points_after=first)
            self._pg(player, g2, points_after=player.points - 50)
        data = self._call()
        by_name = {r["name"]: r for r in data["results"]}
        assert by_name["PlayerA"]["delta"] == 200
        assert by_name["PlayerB"]["delta"] == 50

    # ── club_tag ──────────────────────────────────────────────────────────────

    def test_club_tag_is_returned(self, map_obj, player_a):
        g = self._game(map_obj)
        self._pg(player_a, g, points_after=3000)
        data = self._call()
        assert data["results"][0]["club_tag"] == "TAG_A"


# ── get_hot_this_week_by_points_delta_funcs ───────────────────────────────────


class TestGetHotThisWeekByPointsDeltaFuncs:

    def test_returns_one_func_per_rank(self):
        assert len(get_hot_this_week_by_points_delta_funcs()) == len(HOT_RANKS)

    def test_func_names_include_min_elo(self):
        funcs = get_hot_this_week_by_points_delta_funcs()
        names = [f.__name__ for f in funcs]
        for rank in HOT_RANKS:
            assert f"get_hot_this_week_by_points_delta_{rank['min_elo']}" in names

    def test_names_are_unique(self):
        names = [f.__name__ for f in get_hot_this_week_by_points_delta_funcs()]
        assert len(names) == len(set(names))

    def test_func_delegates_to_get_hot_this_week_by_points_delta(self, monkeypatch):
        import src.threads.update_big_queries as ubq

        calls = []

        def fake_query(min_elo, min_rank, min_date, max_date):
            calls.append((min_elo, min_rank))
            return json.dumps({"last_updated": 0.0, "results": []})

        monkeypatch.setattr(ubq, "get_hot_this_week_by_points_delta", fake_query)
        funcs = get_hot_this_week_by_points_delta_funcs()
        funcs[0](datetime.now() - timedelta(days=30), datetime.now())
        assert calls == [(RANKS[0]["min_elo"], RANKS[0]["min_rank"])]


# ── get_new_players_per_week ──────────────────────────────────────────────────
# For each week of the season, count players whose first qualifying game (at the
# given elo tier) fell in that week. Week numbers are 0-indexed from season
# start. A player is counted only once (the week of their first game).


class TestGetNewPlayersPerWeek:

    SEASON_START = datetime(2024, 1, 1)
    SEASON_END = datetime(2024, 12, 31)

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="NPW_MAP", name="NPW Map")

    @pytest.fixture
    def player_a(self):
        from uuid import UUID

        return Player.create(uuid=UUID("aaaaaaaa-0000-0000-0000-000000000031"), name="PlayerA")

    @pytest.fixture
    def player_b(self):
        from uuid import UUID

        return Player.create(uuid=UUID("bbbbbbbb-0000-0000-0000-000000000032"), name="PlayerB")

    @pytest.fixture
    def player_c(self):
        from uuid import UUID

        return Player.create(uuid=UUID("cccccccc-0000-0000-0000-000000000033"), name="PlayerC")

    def _game(self, map_obj, time, min_elo=0, average_elo=1000):
        return Game.create(map=map_obj, is_finished=True, time=time, min_elo=min_elo, average_elo=average_elo)

    def _pg(self, player, game):
        return PlayerGame.create(player=player, game=game)

    def _call(self, min_elo=0):
        return json.loads(
            get_new_players_per_week({"key": "other", "min_elo": min_elo}, self.SEASON_START, self.SEASON_END)
        )

    def _week(self, data, week_num):
        results = {r["week"]: r for r in data["results"]}
        return results.get(week_num)

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_empty_when_no_games(self):
        data = self._call()
        assert data["results"] == []

    def test_has_last_updated(self):
        data = self._call()
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    def test_result_row_has_expected_keys(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert set(data["results"][0].keys()) == {"week", "week_start", "new_players"}

    def test_week_is_integer(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert isinstance(data["results"][0]["week"], int)

    def test_new_players_is_integer(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert isinstance(data["results"][0]["new_players"], int)

    def test_week_start_is_float(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert isinstance(data["results"][0]["week_start"], float)

    # ── week numbering ────────────────────────────────────────────────────────

    def test_game_on_day_0_is_week_0(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert data["results"][0]["week"] == 0

    def test_game_on_day_6_is_week_0(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=6)))
        data = self._call()
        assert data["results"][0]["week"] == 0

    def test_game_on_day_7_is_week_1(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=7)))
        data = self._call()
        assert data["results"][0]["week"] == 1

    def test_game_on_day_13_is_week_1(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=13)))
        data = self._call()
        assert data["results"][0]["week"] == 1

    def test_game_on_day_14_is_week_2(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=14)))
        data = self._call()
        assert data["results"][0]["week"] == 2

    def test_week_start_matches_season_start_plus_offset(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=7)))
        data = self._call()
        week_1 = data["results"][0]
        expected_start = self.SEASON_START.timestamp() + 7 * 86400
        assert week_1["week_start"] == expected_start

    # ── counting ──────────────────────────────────────────────────────────────

    def test_single_player_one_game_counts_as_1(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert data["results"][0]["new_players"] == 1

    def test_two_players_same_week_counted_together(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        self._pg(player_b, self._game(map_obj, self.SEASON_START + timedelta(days=3)))
        data = self._call()
        assert len(data["results"]) == 1
        assert data["results"][0]["new_players"] == 2

    def test_players_in_different_weeks_split_into_separate_rows(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        self._pg(player_b, self._game(map_obj, self.SEASON_START + timedelta(days=7)))
        data = self._call()
        assert len(data["results"]) == 2
        assert self._week(data, 0)["new_players"] == 1
        assert self._week(data, 1)["new_players"] == 1

    def test_player_counted_only_in_first_week_even_with_later_games(self, map_obj, player_a):
        # First game in week 0; second game in week 1 — should only appear in week 0
        g1 = self._game(map_obj, self.SEASON_START)
        g2 = self._game(map_obj, self.SEASON_START + timedelta(days=7))
        self._pg(player_a, g1)
        self._pg(player_a, g2)
        data = self._call()
        assert len(data["results"]) == 1
        assert data["results"][0]["week"] == 0
        assert data["results"][0]["new_players"] == 1

    def test_results_ordered_by_week(self, map_obj, player_a, player_b, player_c):
        self._pg(player_c, self._game(map_obj, self.SEASON_START + timedelta(days=14)))
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        self._pg(player_b, self._game(map_obj, self.SEASON_START + timedelta(days=7)))
        data = self._call()
        weeks = [r["week"] for r in data["results"]]
        assert weeks == sorted(weeks)

    # ── elo filter ────────────────────────────────────────────────────────────

    def test_game_below_min_elo_excluded(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START, min_elo=500))
        data = self._call(min_elo=1000)
        assert data["results"] == []

    def test_game_at_min_elo_included(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START, min_elo=1000))
        data = self._call(min_elo=1000)
        assert data["results"][0]["new_players"] == 1

    def test_first_game_below_elo_not_counted_second_game_above_is(self, map_obj, player_a):
        # Player's first game ever is below the elo threshold → not in week 0.
        # Their first qualifying game is in week 1 → counted there.
        g_low = self._game(map_obj, self.SEASON_START, min_elo=500)
        g_high = self._game(map_obj, self.SEASON_START + timedelta(days=7), min_elo=3000)
        self._pg(player_a, g_low)
        self._pg(player_a, g_high)
        data = self._call(min_elo=3000)
        assert len(data["results"]) == 1
        assert data["results"][0]["week"] == 1

    def test_average_elo_minus_1_excluded(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START, average_elo=-1))
        data = self._call()
        assert data["results"] == []

    # ── season boundary ───────────────────────────────────────────────────────

    def test_game_before_season_start_excluded(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START - timedelta(days=1)))
        data = self._call()
        assert data["results"] == []

    def test_game_after_season_end_excluded(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_END + timedelta(days=1)))
        data = self._call()
        assert data["results"] == []

    def test_game_on_season_start_included(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert data["results"][0]["new_players"] == 1

    def test_game_on_season_end_included(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_END))
        data = self._call()
        assert len(data["results"]) == 1
        assert data["results"][0]["new_players"] == 1

    # ── sparse output (only weeks with new players appear) ────────────────────

    def test_empty_week_produces_no_row(self, map_obj, player_a, player_b):
        # Players appear in week 0 and week 2 but nobody debuts in week 1
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        self._pg(player_b, self._game(map_obj, self.SEASON_START + timedelta(days=14)))
        data = self._call()
        weeks = [r["week"] for r in data["results"]]
        assert 1 not in weeks
        assert weeks == [0, 2]

    def test_only_weeks_with_debuts_appear(self, map_obj, player_a, player_b, player_c):
        # Week 0 and week 4 only
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        self._pg(player_b, self._game(map_obj, self.SEASON_START + timedelta(days=28)))
        # player_c plays in week 0 again (already covered — no additional row)
        self._pg(player_c, self._game(map_obj, self.SEASON_START + timedelta(days=3)))
        data = self._call()
        weeks = [r["week"] for r in data["results"]]
        assert weeks == [0, 4]

    # ── multiple games for same player in same week ───────────────────────────

    def test_player_with_three_games_in_week_0_counts_as_1(self, map_obj, player_a):
        for day in range(3):
            self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=day)))
        data = self._call()
        assert data["results"][0]["new_players"] == 1

    def test_player_with_five_games_across_two_weeks_counted_once_in_first(self, map_obj, player_a):
        for day in range(5):
            self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=day * 3)))
        data = self._call()
        total = sum(r["new_players"] for r in data["results"])
        assert total == 1

    # ── week_start values ─────────────────────────────────────────────────────

    def test_week_0_start_equals_season_start_timestamp(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        data = self._call()
        assert data["results"][0]["week_start"] == self.SEASON_START.timestamp()

    def test_week_start_formula_week_3(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=21)))
        data = self._call()
        assert data["results"][0]["week_start"] == self.SEASON_START.timestamp() + 3 * 7 * 86400

    def test_week_start_formula_large_week(self, map_obj, player_a):
        # Day 70 → week 10
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=70)))
        data = self._call()
        row = data["results"][0]
        assert row["week"] == 10
        assert row["week_start"] == self.SEASON_START.timestamp() + 10 * 7 * 86400

    # ── min_elo edge cases ────────────────────────────────────────────────────

    def test_min_elo_zero_includes_all_games(self, map_obj, player_a, player_b):
        self._pg(player_a, self._game(map_obj, self.SEASON_START, min_elo=0))
        self._pg(player_b, self._game(map_obj, self.SEASON_START, min_elo=5000))
        data = self._call(min_elo=0)
        assert data["results"][0]["new_players"] == 2

    def test_min_elo_above_all_games_returns_empty(self, map_obj, player_a):
        self._pg(player_a, self._game(map_obj, self.SEASON_START, min_elo=1000))
        data = self._call(min_elo=9999)
        assert data["results"] == []

    def test_player_with_all_games_below_elo_never_appears(self, map_obj, player_a, player_b):
        # player_a only has low-elo games; player_b has a qualifying game
        for day in range(5):
            self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=day), min_elo=500))
        self._pg(player_b, self._game(map_obj, self.SEASON_START, min_elo=3000))
        data = self._call(min_elo=3000)
        assert len(data["results"]) == 1
        assert data["results"][0]["new_players"] == 1

    # ── sum invariant ─────────────────────────────────────────────────────────

    def test_sum_of_new_players_equals_distinct_qualifying_players(self, map_obj, player_a, player_b, player_c):
        # 3 players, each debuts in a different week → total new_players across all weeks = 3
        self._pg(player_a, self._game(map_obj, self.SEASON_START))
        self._pg(player_b, self._game(map_obj, self.SEASON_START + timedelta(days=7)))
        self._pg(player_c, self._game(map_obj, self.SEASON_START + timedelta(days=21)))
        data = self._call()
        assert sum(r["new_players"] for r in data["results"]) == 3

    def test_player_with_pre_season_and_in_season_game_counted_in_correct_week(self, map_obj, player_a):
        # Game before season → excluded; game in week 1 of season → counted there
        self._pg(player_a, self._game(map_obj, self.SEASON_START - timedelta(days=1)))
        self._pg(player_a, self._game(map_obj, self.SEASON_START + timedelta(days=7)))
        data = self._call()
        assert len(data["results"]) == 1
        assert data["results"][0]["week"] == 1

    # ── three or more players in same week ────────────────────────────────────

    def test_three_players_debut_in_same_week(self, map_obj, player_a, player_b, player_c):
        for player in [player_a, player_b, player_c]:
            self._pg(player, self._game(map_obj, self.SEASON_START + timedelta(days=1)))
        data = self._call()
        assert len(data["results"]) == 1
        assert data["results"][0]["new_players"] == 3

    # ── two players on same game ──────────────────────────────────────────────

    def test_two_players_in_same_game_both_counted(self, map_obj, player_a, player_b):
        g = self._game(map_obj, self.SEASON_START)
        self._pg(player_a, g)
        self._pg(player_b, g)
        data = self._call()
        assert data["results"][0]["new_players"] == 2


class TestGetNewPlayersPerWeekFuncs:

    def test_returns_one_func_per_rank(self):
        assert len(get_new_players_per_week_funcs()) == len(RANKS)

    def test_func_names_include_min_elo(self):
        funcs = get_new_players_per_week_funcs()
        names = [f.__name__ for f in funcs]
        for rank in RANKS:
            assert f"get_new_players_per_week_{rank['min_elo']}" in names

    def test_names_are_unique(self):
        names = [f.__name__ for f in get_new_players_per_week_funcs()]
        assert len(names) == len(set(names))

    def test_func_delegates_to_get_new_players_per_week(self, monkeypatch):
        import src.threads.update_big_queries as ubq

        calls = []

        def fake_query(rank, min_date, max_date):
            calls.append(rank["min_elo"])
            return json.dumps({"last_updated": 0.0, "results": []})

        monkeypatch.setattr(ubq, "get_new_players_per_week", fake_query)
        funcs = get_new_players_per_week_funcs()
        funcs[0](datetime(2024, 1, 1), datetime(2024, 12, 31))
        assert calls == [RANKS[0]["min_elo"]]


class TestNewPlayersPerWeekRegistration:

    def test_all_rank_functions_registered_in_get_queries(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = {q.__name__ for q in UpdateBigQueriesThread().get_queries(s)}
        for rank in RANKS:
            assert f"get_new_players_per_week_{rank['min_elo']}" in names

    def test_registered_count_matches_number_of_ranks(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        npw_names = [n for n in names if n.startswith("get_new_players_per_week_")]
        assert len(npw_names) == len(RANKS)


# ── get_activity_heatmap ──────────────────────────────────────────────────────
#
# Returns sparse (day, hour, count) rows for games in the season window,
# filtered by min_elo. 0-indexed day: 0=Sunday…6=Saturday, hour 0-23.
# Only cells with at least one game are returned.


class TestGetActivityHeatmap:
    BASE = datetime(2024, 6, 3, 10, 0, 0)  # Monday 10:00
    WINDOW_END = datetime(2024, 12, 31)

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="HEATMAP_MAP", name="Heatmap Map")

    def _game(self, map_obj, time, min_elo=0, average_elo=1000):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=time,
            min_elo=min_elo,
            average_elo=average_elo,
        )

    def _call(self, min_elo=0, min_date=None, max_date=None):
        return json.loads(
            get_activity_heatmap(
                {"key": "other", "min_elo": min_elo},
                min_date or self.BASE,
                max_date or self.WINDOW_END,
            )
        )

    def _cell(self, data, day, hour):
        return next((r for r in data["results"] if r["day"] == day and r["hour"] == hour), None)

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_empty_when_no_games(self):
        data = self._call()
        assert data["results"] == []

    def test_has_last_updated(self):
        data = self._call()
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    def test_result_row_has_expected_keys(self, map_obj):
        self._game(map_obj, self.BASE)
        data = self._call()
        assert set(data["results"][0].keys()) == {"day", "hour", "count"}

    def test_sparse_only_non_zero_cells_returned(self, map_obj):
        # Only one game → only one cell
        self._game(map_obj, self.BASE)
        data = self._call()
        assert len(data["results"]) == 1

    # ── day encoding ──────────────────────────────────────────────────────────

    def test_sunday_encoded_as_0(self, map_obj):
        sunday = datetime(2024, 6, 2, 12, 0)  # known Sunday
        self._game(map_obj, sunday)
        data = self._call(min_date=datetime(2024, 1, 1))
        cell = self._cell(data, 0, 12)
        assert cell is not None

    def test_saturday_encoded_as_6(self, map_obj):
        saturday = datetime(2024, 6, 8, 15, 0)  # known Saturday
        self._game(map_obj, saturday)
        data = self._call(min_date=datetime(2024, 1, 1))
        cell = self._cell(data, 6, 15)
        assert cell is not None

    def test_monday_encoded_as_1(self, map_obj):
        # BASE is Monday 2024-06-03; DAYOFWEEK=2 → 2-1=1
        self._game(map_obj, self.BASE)
        data = self._call()
        cell = self._cell(data, 1, 10)
        assert cell is not None

    # ── hour encoding ─────────────────────────────────────────────────────────

    def test_hour_matches_game_time_hour(self, map_obj):
        t = datetime(2024, 6, 3, 17, 30)
        self._game(map_obj, t)
        data = self._call()
        cell = self._cell(data, 1, 17)  # Monday = day 1
        assert cell is not None and cell["count"] == 1

    # ── aggregation ───────────────────────────────────────────────────────────

    def test_multiple_games_same_cell_aggregated(self, map_obj):
        for _ in range(4):
            self._game(map_obj, self.BASE)
        data = self._call()
        assert data["results"][0]["count"] == 4

    def test_games_in_different_cells_produce_separate_rows(self, map_obj):
        self._game(map_obj, datetime(2024, 6, 3, 10, 0))  # Monday 10h
        self._game(map_obj, datetime(2024, 6, 3, 14, 0))  # Monday 14h
        self._game(map_obj, datetime(2024, 6, 4, 10, 0))  # Tuesday 10h
        data = self._call()
        assert len(data["results"]) == 3

    # ── date filtering ────────────────────────────────────────────────────────

    def test_game_before_min_date_excluded(self, map_obj):
        self._game(map_obj, self.BASE - timedelta(days=1))
        data = self._call()
        assert data["results"] == []

    def test_game_after_max_date_excluded(self, map_obj):
        self._game(map_obj, self.WINDOW_END + timedelta(days=1))
        data = self._call()
        assert data["results"] == []

    def test_game_on_min_date_included(self, map_obj):
        self._game(map_obj, self.BASE)
        data = self._call()
        assert len(data["results"]) == 1

    # ── min_elo filtering ─────────────────────────────────────────────────────

    def test_game_meets_min_elo_included(self, map_obj):
        self._game(map_obj, self.BASE, min_elo=2000)
        data = self._call(min_elo=2000)
        assert len(data["results"]) == 1

    def test_game_below_min_elo_excluded(self, map_obj):
        self._game(map_obj, self.BASE, min_elo=500)
        data = self._call(min_elo=1000)
        assert data["results"] == []

    def test_min_elo_zero_includes_all(self, map_obj):
        self._game(map_obj, self.BASE, min_elo=0)
        self._game(map_obj, self.BASE, min_elo=5000)
        data = self._call(min_elo=0)
        assert data["results"][0]["count"] == 2

    # ── average_elo filter ────────────────────────────────────────────────────

    def test_game_with_average_elo_minus_1_excluded(self, map_obj):
        self._game(map_obj, self.BASE, average_elo=-1)
        data = self._call()
        assert data["results"] == []

    # ── ordering ──────────────────────────────────────────────────────────────

    def test_results_ordered_by_day_then_hour(self, map_obj):
        self._game(map_obj, datetime(2024, 6, 8, 15, 0))  # Saturday
        self._game(map_obj, datetime(2024, 6, 3, 10, 0))  # Monday
        self._game(map_obj, datetime(2024, 6, 3, 7, 0))  # Monday earlier
        data = self._call(min_date=datetime(2024, 1, 1))
        days_hours = [(r["day"], r["hour"]) for r in data["results"]]
        assert days_hours == sorted(days_hours)


# ── get_activity_heatmap_funcs ────────────────────────────────────────────────


class TestGetActivityHeatmapFuncs:

    def test_returns_one_func_per_rank(self):
        assert len(get_activity_heatmap_funcs()) == len(RANKS)

    def test_func_names_match_rank_min_elo(self):
        for func, rank in zip(get_activity_heatmap_funcs(), RANKS):
            assert func.__name__ == f"get_activity_heatmap_{rank['min_elo']}"

    def test_no_late_binding_all_names_distinct(self):
        names = [f.__name__ for f in get_activity_heatmap_funcs()]
        assert len(names) == len(set(names))

    def test_func_delegates_to_get_activity_heatmap(self, monkeypatch):
        import src.threads.update_big_queries as ubq

        calls = []

        def fake_query(rank, min_date, max_date):
            calls.append(rank["min_elo"])
            return json.dumps({"last_updated": 0.0, "results": []})

        monkeypatch.setattr(ubq, "get_activity_heatmap", fake_query)
        funcs = get_activity_heatmap_funcs()
        funcs[0](datetime.now() - timedelta(days=30), datetime.now())
        assert calls == [RANKS[0]["min_elo"]]


# ── get_activity_heatmap registration ────────────────────────────────────────


class TestActivityHeatmapRegistration:

    def test_all_rank_functions_registered_in_get_queries(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = {q.__name__ for q in UpdateBigQueriesThread().get_queries(s)}
        for rank in RANKS:
            assert f"get_activity_heatmap_{rank['min_elo']}" in names

    def test_registered_count_matches_number_of_ranks(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        heatmap_names = [n for n in names if n.startswith("get_activity_heatmap_")]
        assert len(heatmap_names) == len(RANKS)


# ── get_country_h2h ───────────────────────────────────────────────────────────
#
# For each (country_a, country_b) pair found in qualifying games:
#   wins   = distinct games where at least one country_a player won
#   losses = games - wins
#   games  = distinct games where both country_a and country_b had a player
#
# Only H2H_ELOS tiers are computed (3000, 3300, 3600, 4000).


class TestGetCountryH2H:
    BASE = datetime(2024, 6, 1, 12, 0)
    WINDOW_END = datetime(2024, 12, 31)

    # ── fixtures ──────────────────────────────────────────────────────────────

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="H2H_MAP", name="H2H Map")

    @pytest.fixture
    def zone_fra(self):
        return Zone.create(uuid="zfra-h2h-001", name="France", country_alpha3="FRA", file_name="FRA")

    @pytest.fixture
    def zone_ger(self):
        return Zone.create(uuid="zger-h2h-001", name="Germany", country_alpha3="GER", file_name="GER")

    @pytest.fixture
    def zone_esp(self):
        return Zone.create(uuid="zesp-h2h-001", name="Spain", country_alpha3="ESP", file_name="ESP")

    @pytest.fixture
    def player_fra(self, zone_fra):
        from uuid import UUID

        return Player.create(uuid=UUID("aaaaaaaa-0000-0000-0000-000000000031"), name="PlayerFRA", country=zone_fra)

    @pytest.fixture
    def player_ger(self, zone_ger):
        from uuid import UUID

        return Player.create(uuid=UUID("bbbbbbbb-0000-0000-0000-000000000032"), name="PlayerGER", country=zone_ger)

    @pytest.fixture
    def player_esp(self, zone_esp):
        from uuid import UUID

        return Player.create(uuid=UUID("cccccccc-0000-0000-0000-000000000033"), name="PlayerESP", country=zone_esp)

    def _game(self, map_obj, min_elo=3000, average_elo=3500, days_offset=1):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=self.BASE + timedelta(days=days_offset),
            min_elo=min_elo,
            average_elo=average_elo,
        )

    def _pg(self, player, game, is_win=False):
        return PlayerGame.create(player=player, game=game, is_win=is_win)

    def _call(self, tmp_path, min_elo=3000, min_date=None, max_date=None):
        s = make_season("h2h", end_delta=timedelta(days=365))
        func = get_country_h2h_func(str(tmp_path) + "/", s, min_elo)
        func(min_date or self.BASE, max_date or self.WINDOW_END)
        return str(tmp_path) + "/", s.id

    def _read(self, path, season_id, country, min_elo=3000):
        with open(f"{path}country_h2h_{min_elo}/{season_id}/{country}.txt") as f:
            return json.loads(f.read())

    def _record(self, data, opponent):
        return next((r for r in data["results"] if r["opponent"]["alpha3"] == opponent), None)

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_no_games_writes_no_files(self, tmp_path):
        path, sid = self._call(tmp_path)
        assert (
            not (tmp_path / "country_h2h_3000" / str(sid)).exists()
            or len(list((tmp_path / "country_h2h_3000" / str(sid)).iterdir())) == 0
        )

    def test_result_row_has_expected_keys(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        data = self._read(path, sid, "FRA")
        row = data["results"][0]
        assert set(row.keys()) == {"opponent", "wins", "draws", "losses", "games"}
        assert set(row["opponent"].keys()) == {"name", "file_name", "alpha3"}

    def test_has_last_updated(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        data = self._read(path, sid, "FRA")
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    def test_separate_file_per_country(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        # Both FRA and GER should have files
        self._read(path, sid, "FRA")
        self._read(path, sid, "GER")

    # ── wins / losses / games semantics ──────────────────────────────────────

    def test_fra_wins_when_fra_player_wins(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec["wins"] == 1
        assert rec["losses"] == 0
        assert rec["games"] == 1

    def test_fra_loses_when_fra_player_does_not_win(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=False)
        self._pg(player_ger, g, is_win=True)
        path, sid = self._call(tmp_path)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec["wins"] == 0
        assert rec["losses"] == 1
        assert rec["games"] == 1

    def test_wins_and_losses_are_symmetric(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        fra_rec = self._record(self._read(path, sid, "FRA"), "GER")
        ger_rec = self._record(self._read(path, sid, "GER"), "FRA")
        assert fra_rec["wins"] == ger_rec["losses"]
        assert fra_rec["losses"] == ger_rec["wins"]
        assert fra_rec["games"] == ger_rec["games"]

    def test_multiple_games_accumulate(self, tmp_path, map_obj, player_fra, player_ger):
        for is_win in [True, True, False]:
            g = self._game(map_obj, days_offset=is_win + 1)
            self._pg(player_fra, g, is_win=is_win)
            self._pg(player_ger, g, is_win=not is_win)
        path, sid = self._call(tmp_path)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec["wins"] == 2
        assert rec["losses"] == 1
        assert rec["games"] == 3

    def test_same_team_winners_not_counted_in_h2h(self, tmp_path, map_obj, player_fra, player_ger):
        # FRA and GER are on the same winning team — should not appear in each other's h2h
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=True)
        path, sid = self._call(tmp_path)
        # No cross-team opponent → no files written at all
        import pytest as _pytest

        with _pytest.raises(FileNotFoundError):
            self._read(path, sid, "FRA")

    def test_same_team_losers_not_counted_in_h2h(self, tmp_path, map_obj, player_fra, player_ger, player_esp):
        # FRA and GER are both losers against ESP — should not appear in each other's h2h
        g = self._game(map_obj)
        self._pg(player_esp, g, is_win=True)
        self._pg(player_fra, g, is_win=False)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        ger_rec = self._record(self._read(path, sid, "GER"), "FRA")
        fra_rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert ger_rec is None
        assert fra_rec is None

    def test_same_team_game_does_not_inflate_h2h(self, tmp_path, map_obj, player_fra, player_ger, player_esp):
        # Game 1: FRA beats GER (cross-team, counts)
        g1 = self._game(map_obj, days_offset=1)
        self._pg(player_fra, g1, is_win=True)
        self._pg(player_ger, g1, is_win=False)
        # Game 2: FRA and GER are both winners against ESP (same team, must not count)
        g2 = self._game(map_obj, days_offset=2)
        self._pg(player_fra, g2, is_win=True)
        self._pg(player_ger, g2, is_win=True)
        self._pg(player_esp, g2, is_win=False)
        path, sid = self._call(tmp_path)
        fra_rec = self._record(self._read(path, sid, "FRA"), "GER")
        ger_rec = self._record(self._read(path, sid, "GER"), "FRA")
        # Only game 1 counts for FRA vs GER
        assert fra_rec["wins"] == 1
        assert fra_rec["losses"] == 0
        assert fra_rec["games"] == 1
        # Symmetry must hold
        assert ger_rec["wins"] == 0
        assert ger_rec["losses"] == 1

    def test_wins_losses_symmetric_with_mixed_games(self, tmp_path, map_obj, player_fra, player_ger, player_esp):
        # FRA beats GER twice
        for i in range(2):
            g = self._game(map_obj, days_offset=i + 1)
            self._pg(player_fra, g, is_win=True)
            self._pg(player_ger, g, is_win=False)
        # GER beats FRA once
        g = self._game(map_obj, days_offset=3)
        self._pg(player_ger, g, is_win=True)
        self._pg(player_fra, g, is_win=False)
        # FRA and GER on same winning team (should be ignored)
        g = self._game(map_obj, days_offset=4)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=True)
        self._pg(player_esp, g, is_win=False)
        path, sid = self._call(tmp_path)
        fra_rec = self._record(self._read(path, sid, "FRA"), "GER")
        ger_rec = self._record(self._read(path, sid, "GER"), "FRA")
        assert fra_rec["wins"] == ger_rec["losses"] == 2
        assert fra_rec["losses"] == ger_rec["wins"] == 1

    # ── three-country game ────────────────────────────────────────────────────

    def test_three_countries_all_pairs_recorded(self, tmp_path, map_obj, player_fra, player_ger, player_esp):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        self._pg(player_esp, g, is_win=False)
        path, sid = self._call(tmp_path)
        fra_data = self._read(path, sid, "FRA")
        opponents = {r["opponent"]["alpha3"] for r in fra_data["results"]}
        assert "GER" in opponents
        assert "ESP" in opponents

    def test_three_countries_same_team_losers_not_paired(self, tmp_path, map_obj, player_fra, player_ger, player_esp):
        # FRA wins; GER and ESP both lose — GER vs ESP should not appear in h2h
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        self._pg(player_esp, g, is_win=False)
        path, sid = self._call(tmp_path)
        ger_data = self._read(path, sid, "GER")
        esp_data = self._read(path, sid, "ESP")
        ger_vs_esp = self._record(ger_data, "ESP")
        esp_vs_ger = self._record(esp_data, "GER")
        assert ger_vs_esp is None
        assert esp_vs_ger is None

    # ── min_elo filtering ─────────────────────────────────────────────────────

    def test_game_meets_min_elo_included(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj, min_elo=3000)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path, min_elo=3000)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec is not None

    def test_game_below_min_elo_excluded(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj, min_elo=2000)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path, min_elo=3000)
        import pytest as _pytest

        with _pytest.raises(FileNotFoundError):
            self._read(path, sid, "FRA")

    # ── average_elo filter ────────────────────────────────────────────────────

    def test_game_with_average_elo_minus_1_excluded(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj, average_elo=-1)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        import pytest as _pytest

        with _pytest.raises(FileNotFoundError):
            self._read(path, sid, "FRA")

    # ── date filtering ────────────────────────────────────────────────────────

    def test_game_before_min_date_excluded(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj, days_offset=-1)  # before BASE
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        import pytest as _pytest

        with _pytest.raises(FileNotFoundError):
            self._read(path, sid, "FRA")

    def test_game_after_max_date_excluded(self, tmp_path, map_obj, player_fra, player_ger):
        g = self._game(map_obj, days_offset=400)  # after WINDOW_END
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        import pytest as _pytest

        with _pytest.raises(FileNotFoundError):
            self._read(path, sid, "FRA")

    # ── player with no country is ignored ────────────────────────────────────

    def test_player_without_country_not_counted(self, tmp_path, map_obj, player_fra):
        from uuid import UUID

        no_country = Player.create(uuid=UUID("dddddddd-0000-0000-0000-000000000034"), name="NoCountry")
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(no_country, g, is_win=False)
        path, sid = self._call(tmp_path)
        # FRA has no opponent with a country → no file
        import pytest as _pytest

        with _pytest.raises(FileNotFoundError):
            self._read(path, sid, "FRA")

    # ── draw detection ───────────────────────────────────────────────────────

    @pytest.fixture
    def player_fra2(self, zone_fra):
        from uuid import UUID

        return Player.create(uuid=UUID("aaaaaaaa-0000-0000-0000-000000000035"), name="PlayerFRA2", country=zone_fra)

    @pytest.fixture
    def player_ger2(self, zone_ger):
        from uuid import UUID

        return Player.create(uuid=UUID("bbbbbbbb-0000-0000-0000-000000000036"), name="PlayerGER2", country=zone_ger)

    def test_fra_in_both_teams_is_draw(self, tmp_path, map_obj, player_fra, player_fra2, player_ger):
        # FRA has one player on each team → draw
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_fra2, g, is_win=False)
        self._pg(player_ger, g, is_win=True)
        path, sid = self._call(tmp_path)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec["draws"] == 1
        assert rec["wins"] == 0
        assert rec["losses"] == 0
        assert rec["games"] == 1

    def test_ger_in_both_teams_is_draw(self, tmp_path, map_obj, player_fra, player_ger, player_ger2):
        # GER has one player on each team → draw for both FRA and GER
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=True)
        self._pg(player_ger2, g, is_win=False)
        path, sid = self._call(tmp_path)
        fra_rec = self._record(self._read(path, sid, "FRA"), "GER")
        ger_rec = self._record(self._read(path, sid, "GER"), "FRA")
        assert fra_rec["draws"] == 1
        assert fra_rec["wins"] == 0
        assert ger_rec["draws"] == 1

    def test_both_countries_in_both_teams_is_draw(
        self, tmp_path, map_obj, player_fra, player_fra2, player_ger, player_ger2
    ):
        # Both FRA and GER span both teams
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_fra2, g, is_win=False)
        self._pg(player_ger, g, is_win=True)
        self._pg(player_ger2, g, is_win=False)
        path, sid = self._call(tmp_path)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec["draws"] == 1
        assert rec["wins"] == 0
        assert rec["losses"] == 0

    def test_draw_is_symmetric(self, tmp_path, map_obj, player_fra, player_fra2, player_ger):
        # If FRA is in both teams, GER should also see it as a draw
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_fra2, g, is_win=False)
        self._pg(player_ger, g, is_win=True)
        path, sid = self._call(tmp_path)
        fra_rec = self._record(self._read(path, sid, "FRA"), "GER")
        ger_rec = self._record(self._read(path, sid, "GER"), "FRA")
        assert fra_rec["draws"] == ger_rec["draws"] == 1

    def test_draw_does_not_count_as_win_or_loss(self, tmp_path, map_obj, player_fra, player_fra2, player_ger):
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_fra2, g, is_win=False)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec["wins"] + rec["draws"] + rec["losses"] == rec["games"]
        assert rec["draws"] == 1
        assert rec["wins"] == 0
        assert rec["losses"] == 0

    def test_no_draw_when_countries_on_separate_teams(self, tmp_path, map_obj, player_fra, player_ger):
        # Normal game: FRA wins, GER loses — no draw
        g = self._game(map_obj)
        self._pg(player_fra, g, is_win=True)
        self._pg(player_ger, g, is_win=False)
        path, sid = self._call(tmp_path)
        rec = self._record(self._read(path, sid, "FRA"), "GER")
        assert rec["draws"] == 0

    # ── ordering ──────────────────────────────────────────────────────────────

    def test_results_ordered_by_games_descending(self, tmp_path, map_obj, player_fra, player_ger, player_esp):
        # FRA vs GER: 3 games; FRA vs ESP: 1 game
        for i in range(3):
            g = self._game(map_obj, days_offset=i + 1)
            self._pg(player_fra, g, is_win=True)
            self._pg(player_ger, g, is_win=False)
        g2 = self._game(map_obj, days_offset=10)
        self._pg(player_fra, g2, is_win=True)
        self._pg(player_esp, g2, is_win=False)
        path, sid = self._call(tmp_path)
        data = self._read(path, sid, "FRA")
        games_list = [r["games"] for r in data["results"]]
        assert games_list == sorted(games_list, reverse=True)
        assert data["results"][0]["opponent"]["alpha3"] == "GER"


# ── get_country_h2h_funcs ─────────────────────────────────────────────────────


class TestGetCountryH2HFuncs:

    def test_returns_one_func_per_h2h_elo(self):
        s = make_season("s", end_delta=timedelta(days=30))
        assert len(get_country_h2h_funcs("cache/", s)) == len(H2H_ELOS)

    def test_func_names_match_h2h_elos(self):
        s = make_season("s", end_delta=timedelta(days=30))
        for func, elo in zip(get_country_h2h_funcs("cache/", s), H2H_ELOS):
            assert func.__name__ == f"get_country_h2h_{elo}"

    def test_all_names_distinct(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [f.__name__ for f in get_country_h2h_funcs("cache/", s)]
        assert len(names) == len(set(names))

    def test_h2h_elos_constant_matches_expected_tiers(self):
        assert H2H_ELOS == [3000, 3300, 3600, 4000]


# ── get_country_h2h registration ──────────────────────────────────────────────


class TestCountryH2HRegistration:

    def test_all_h2h_elo_functions_registered(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = {q.__name__ for q in UpdateBigQueriesThread().get_queries(s)}
        for elo in H2H_ELOS:
            assert f"get_country_h2h_{elo}" in names

    def test_registered_count_matches_h2h_elos(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        h2h_names = [n for n in names if n.startswith("get_country_h2h_")]
        assert len(h2h_names) == len(H2H_ELOS)

    def test_no_non_h2h_elos_registered(self):
        """Verifies the stat is NOT computed for elos below 3000."""
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        h2h_names = [n for n in names if n.startswith("get_country_h2h_")]
        for name in h2h_names:
            elo = int(name.split("_")[-1])
            assert elo in H2H_ELOS
            assert elo >= 3000


# ── get_cross_rank_frequency ──────────────────────────────────────────────────
#
# Bins games by (effective_max_elo - min_elo) spread into 13 buckets matching
# RANKS thresholds: [0, 300, 600, 1000, 1300, 1600, 2000, 2300, 2600, 3000, 3300, 3600, 4000].
# For TM games (trackmaster_limit < 999999), effective max = trackmaster_limit.
# Filters: average_elo > -1, min_elo >= min_elo, time in [min_date, max_date].


class TestGetCrossRankFrequency:

    BASE = datetime(2024, 3, 1)
    MAX_DATE = datetime(2024, 4, 1)

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="CRF_BQ_MAP", name="CRF BQ Map")

    def _game(self, map_obj, min_elo, max_elo, average_elo=2000, trackmaster_limit=999999):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=self.BASE + timedelta(days=5),
            min_elo=min_elo,
            max_elo=max_elo,
            average_elo=average_elo,
            trackmaster_limit=trackmaster_limit,
        )

    def _call(self, min_elo=0):
        return json.loads(get_cross_rank_frequency({"key": "other", "min_elo": min_elo}, self.BASE, self.MAX_DATE))

    def _bucket(self, data, spread_min):
        return next(r for r in data["results"] if r["spread_min"] == spread_min)

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_json_with_results(self):
        data = self._call()
        assert "results" in data

    def test_has_last_updated(self):
        data = self._call()
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)

    def test_returns_13_bins(self):
        data = self._call()
        assert len(data["results"]) == 13

    def test_bin_keys_are_correct(self):
        data = self._call()
        thresholds = sorted(r["min_elo"] for r in RANKS)
        for i, row in enumerate(data["results"]):
            assert row["spread_min"] == thresholds[i]
            expected_max = thresholds[i + 1] - 1 if i + 1 < len(thresholds) else None
            assert row["spread_max"] == expected_max

    def test_all_counts_zero_when_no_games(self):
        data = self._call()
        assert all(r["count"] == 0 for r in data["results"])

    def test_bins_ordered_ascending(self):
        data = self._call()
        mins = [r["spread_min"] for r in data["results"]]
        assert mins == sorted(mins)

    def test_count_is_integer(self, map_obj):
        self._game(map_obj, min_elo=1000, max_elo=1200)
        data = self._call()
        assert all(isinstance(r["count"], int) for r in data["results"])

    def test_last_bin_has_none_spread_max(self):
        data = self._call()
        assert data["results"][-1]["spread_max"] is None

    def test_first_bin_spread_min_is_zero(self):
        data = self._call()
        assert data["results"][0]["spread_min"] == 0

    # ── spread bucketing ──────────────────────────────────────────────────────

    def test_spread_0_falls_in_bucket_0(self, map_obj):
        self._game(map_obj, min_elo=2000, max_elo=2000)
        data = self._call()
        assert self._bucket(data, 0)["count"] == 1

    def test_spread_299_falls_in_bucket_0(self, map_obj):
        self._game(map_obj, min_elo=2000, max_elo=2299)
        data = self._call()
        assert self._bucket(data, 0)["count"] == 1

    def test_spread_300_falls_in_bucket_300(self, map_obj):
        self._game(map_obj, min_elo=2000, max_elo=2300)
        data = self._call()
        assert self._bucket(data, 300)["count"] == 1
        assert self._bucket(data, 0)["count"] == 0

    def test_spread_599_falls_in_bucket_300(self, map_obj):
        self._game(map_obj, min_elo=2000, max_elo=2599)
        data = self._call()
        assert self._bucket(data, 300)["count"] == 1

    def test_spread_600_falls_in_bucket_600(self, map_obj):
        self._game(map_obj, min_elo=1000, max_elo=1600)
        data = self._call()
        assert self._bucket(data, 600)["count"] == 1

    def test_spread_999_falls_in_bucket_600(self, map_obj):
        self._game(map_obj, min_elo=1000, max_elo=1999)
        data = self._call()
        assert self._bucket(data, 600)["count"] == 1

    def test_spread_1000_falls_in_bucket_1000(self, map_obj):
        self._game(map_obj, min_elo=1000, max_elo=2000)
        data = self._call()
        assert self._bucket(data, 1000)["count"] == 1

    def test_spread_2299_falls_in_bucket_2000(self, map_obj):
        self._game(map_obj, min_elo=0, max_elo=2299)
        data = self._call()
        assert self._bucket(data, 2000)["count"] == 1

    def test_spread_2300_falls_in_bucket_2300(self, map_obj):
        # new bin not in old 8-bin set
        self._game(map_obj, min_elo=0, max_elo=2300)
        data = self._call()
        assert self._bucket(data, 2300)["count"] == 1
        assert self._bucket(data, 2000)["count"] == 0

    def test_spread_2600_falls_in_bucket_2600(self, map_obj):
        # new bin — old system lumped 2000-2599 together
        self._game(map_obj, min_elo=0, max_elo=2600)
        data = self._call()
        assert self._bucket(data, 2600)["count"] == 1

    def test_spread_2999_falls_in_bucket_2600(self, map_obj):
        self._game(map_obj, min_elo=0, max_elo=2999)
        data = self._call()
        assert self._bucket(data, 2600)["count"] == 1

    def test_spread_3000_falls_in_bucket_3000(self, map_obj):
        self._game(map_obj, min_elo=0, max_elo=3000)
        data = self._call()
        assert self._bucket(data, 3000)["count"] == 1

    def test_spread_3600_falls_in_bucket_3600(self, map_obj):
        self._game(map_obj, min_elo=0, max_elo=3600)
        data = self._call()
        assert self._bucket(data, 3600)["count"] == 1

    def test_spread_4000_falls_in_last_bucket(self, map_obj):
        self._game(map_obj, min_elo=0, max_elo=4000)
        data = self._call()
        assert self._bucket(data, 4000)["count"] == 1

    def test_multiple_games_accumulate_in_same_bucket(self, map_obj):
        self._game(map_obj, min_elo=2000, max_elo=2100)  # spread 100 → bucket 0
        self._game(map_obj, min_elo=2000, max_elo=2200)  # spread 200 → bucket 0
        data = self._call()
        assert self._bucket(data, 0)["count"] == 2

    def test_games_split_across_buckets(self, map_obj):
        self._game(map_obj, min_elo=2000, max_elo=2100)  # spread 100 → bucket 0
        self._game(map_obj, min_elo=2000, max_elo=2400)  # spread 400 → bucket 300
        data = self._call()
        assert self._bucket(data, 0)["count"] == 1
        assert self._bucket(data, 300)["count"] == 1

    def test_tm_game_spread_uses_max_elo_not_trackmaster_limit(self, map_obj):
        # min_elo=3600, max_elo=5000, trackmaster_limit=4000
        # spread = max_elo - min_elo = 5000 - 3600 = 1400 → bucket 1300
        self._game(map_obj, min_elo=3600, max_elo=5000, trackmaster_limit=4000)
        data = self._call()
        assert self._bucket(data, 1300)["count"] == 1
        assert self._bucket(data, 300)["count"] == 0

    def test_spread_uses_max_elo(self, map_obj):
        # spread = 3900 - 3600 = 300 → bucket 300
        self._game(map_obj, min_elo=3600, max_elo=3900, trackmaster_limit=999999)
        data = self._call()
        assert self._bucket(data, 300)["count"] == 1

    def test_player_above_4000_spread_uses_max_elo(self, map_obj):
        # spread = 4100 - 3700 = 400 → bucket 300
        self._game(map_obj, min_elo=3700, max_elo=4100, trackmaster_limit=999999)
        data = self._call()
        assert self._bucket(data, 300)["count"] == 1

    # ── min_elo filter ────────────────────────────────────────────────────────

    def test_game_below_min_elo_excluded(self, map_obj):
        self._game(map_obj, min_elo=500, max_elo=800)
        data = self._call(min_elo=1000)
        assert all(r["count"] == 0 for r in data["results"])

    def test_game_at_min_elo_included(self, map_obj):
        self._game(map_obj, min_elo=1000, max_elo=1200)
        data = self._call(min_elo=1000)
        assert self._bucket(data, 0)["count"] == 1

    def test_min_elo_zero_includes_all(self, map_obj):
        self._game(map_obj, min_elo=0, max_elo=200)
        data = self._call(min_elo=0)
        assert self._bucket(data, 0)["count"] == 1

    def test_min_elo_filters_mixed_games(self, map_obj):
        self._game(map_obj, min_elo=500, max_elo=700)  # excluded
        self._game(map_obj, min_elo=2000, max_elo=2200)  # included
        data = self._call(min_elo=1000)
        total = sum(r["count"] for r in data["results"])
        assert total == 1

    # ── average_elo filter ────────────────────────────────────────────────────

    def test_game_with_average_elo_minus_one_excluded(self, map_obj):
        self._game(map_obj, min_elo=2000, max_elo=2200, average_elo=-1)
        data = self._call()
        assert all(r["count"] == 0 for r in data["results"])

    # ── time range filter ─────────────────────────────────────────────────────

    def test_game_before_min_date_excluded(self, map_obj):
        # Create a game before BASE
        Game.create(
            map=map_obj,
            is_finished=True,
            time=self.BASE - timedelta(days=1),
            min_elo=2000,
            max_elo=2200,
            average_elo=2000,
        )
        data = self._call()
        assert all(r["count"] == 0 for r in data["results"])

    def test_game_after_max_date_excluded(self, map_obj):
        Game.create(
            map=map_obj,
            is_finished=True,
            time=self.MAX_DATE + timedelta(days=1),
            min_elo=2000,
            max_elo=2200,
            average_elo=2000,
        )
        data = self._call()
        assert all(r["count"] == 0 for r in data["results"])

    def test_game_at_boundaries_included(self, map_obj):
        Game.create(
            map=map_obj,
            is_finished=True,
            time=self.BASE,
            min_elo=2000,
            max_elo=2100,
            average_elo=2000,
        )
        Game.create(
            map=map_obj,
            is_finished=True,
            time=self.MAX_DATE,
            min_elo=2000,
            max_elo=2100,
            average_elo=2000,
        )
        data = self._call()
        assert self._bucket(data, 0)["count"] == 2


# ── get_cross_rank_frequency_funcs ────────────────────────────────────────────


class TestGetCrossRankFrequencyFuncs:

    def test_returns_one_function_per_rank(self):
        assert len(get_cross_rank_frequency_funcs()) == len(RANKS)

    def test_each_function_has_correct_name(self):
        for func, rank in zip(get_cross_rank_frequency_funcs(), RANKS):
            assert func.__name__ == f"get_cross_rank_frequency_{rank['min_elo']}"

    def test_function_names_are_unique(self):
        names = [f.__name__ for f in get_cross_rank_frequency_funcs()]
        assert len(names) == len(set(names))

    def test_each_function_captures_correct_min_elo(self, monkeypatch):
        captured = []

        def fake_crf(rank, min_date, max_date):
            captured.append(rank["min_elo"])
            return json.dumps({"results": [], "last_updated": 0})

        import src.threads.update_big_queries as ubq

        monkeypatch.setattr(ubq, "get_cross_rank_frequency", fake_crf)

        funcs = get_cross_rank_frequency_funcs()
        for func in funcs:
            func(datetime(2024, 1, 1), datetime(2024, 12, 31))

        assert captured == [rank["min_elo"] for rank in RANKS]


# ── get_cross_rank_frequency registration ─────────────────────────────────────


class TestCrossRankFrequencyRegistration:

    def test_all_rank_functions_registered_in_get_queries(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = {q.__name__ for q in UpdateBigQueriesThread().get_queries(s)}
        for rank in RANKS:
            assert f"get_cross_rank_frequency_{rank['min_elo']}" in names

    def test_registered_count_matches_number_of_ranks(self):
        s = make_season("s", end_delta=timedelta(days=30))
        names = [q.__name__ for q in UpdateBigQueriesThread().get_queries(s)]
        crf_names = [n for n in names if n.startswith("get_cross_rank_frequency_")]
        assert len(crf_names) == len(RANKS)


# ── get_players_statistics ────────────────────────────────────────────────────


class TestGetPlayersStatistics:

    BASE = datetime(2024, 6, 1, 12, 0)
    WINDOW_END = datetime(2024, 12, 31)

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="PS_MAP", name="PS Map")

    @pytest.fixture
    def zone(self):
        return Zone.create(uuid="zps-001", name="France", country_alpha3="FRA", file_name="FRA")

    @pytest.fixture
    def player(self, zone):
        from uuid import UUID

        return Player.create(
            uuid=UUID("aaaaaaaa-0000-0000-0000-000000000090"),
            name="PSPlayer",
            club_tag="PSTAG",
            points=1000,
            country=zone,
        )

    @pytest.fixture
    def player_no_country(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("bbbbbbbb-0000-0000-0000-000000000091"),
            name="PSNoCountry",
            points=1000,
        )

    def _game(self, map_obj):
        return Game.create(
            map=map_obj,
            is_finished=True,
            time=self.BASE + timedelta(days=1),
            average_elo=1000,
        )

    def _pg(self, player, game, is_win=False):
        return PlayerGame.create(player=player, game=game, is_win=is_win)

    def _call(self, o_by="played"):
        return json.loads(get_players_statistics(o_by, self.BASE, self.WINDOW_END))

    def test_result_row_has_expected_keys(self, map_obj, player):
        self._pg(player, self._game(map_obj))
        row = self._call()["results"][0]
        assert set(row.keys()) == {"name", "uuid", "club_tag", "country", "played", "wins", "losses", "mvps"}

    def test_club_tag_is_returned(self, map_obj, player):
        self._pg(player, self._game(map_obj))
        assert self._call()["results"][0]["club_tag"] == "PSTAG"

    def test_club_tag_is_none_when_not_set(self, map_obj, player_no_country):
        self._pg(player_no_country, self._game(map_obj))
        assert self._call()["results"][0]["club_tag"] is None

    def test_country_is_full_object(self, map_obj, player):
        self._pg(player, self._game(map_obj))
        country = self._call()["results"][0]["country"]
        assert country == {"name": "France", "file_name": "FRA", "alpha3": "FRA"}

    def test_country_is_none_when_not_set(self, map_obj, player_no_country):
        self._pg(player_no_country, self._game(map_obj))
        assert self._call()["results"][0]["country"] is None


# ── get_clubs_leaderboard ─────────────────────────────────────────────────────


class TestGetClubsLeaderboard:

    @pytest.fixture
    def season(self):
        now = datetime.now()
        return Season.create(name="CL_S", start_time=now - timedelta(days=30), end_time=now + timedelta(days=30))

    @pytest.fixture
    def player(self):
        from uuid import UUID

        return Player.create(
            uuid=UUID("aaaaaaaa-0000-0000-0000-000000000092"),
            name="CLPlayer",
            club_tag="CURRENT_TAG",
        )

    def _call(self, season):
        return json.loads(get_clubs_leaderboard(season.id))

    def test_uses_player_season_club_tag_not_player_club_tag(self, season, player):
        # PlayerSeason has a different (older) club_tag than Player — should use PlayerSeason's
        PlayerSeason.create(player=player, season=season, rank=1, points=1000, club_tag="SEASON_TAG")
        result = self._call(season)
        tags = [r["name"] for r in result["results"]]
        assert "SEASON_TAG" in tags
        assert "CURRENT_TAG" not in tags

    def test_player_with_no_season_club_tag_excluded(self, season, player):
        # PlayerSeason.club_tag is NULL → player must not appear
        PlayerSeason.create(player=player, season=season, rank=1, points=1000, club_tag=None)
        result = self._call(season)
        assert result["results"] == []


# ── get_top_100_per_country_func ──────────────────────────────────────────────


class TestGetTop100PerCountryFunc:

    @pytest.fixture
    def season(self):
        now = datetime.now()
        return Season.create(name="T100_S", start_time=now - timedelta(days=30), end_time=now + timedelta(days=30))

    @pytest.fixture
    def country_zone(self):
        return Zone.create(
            uuid="aaaaaaaa-0000-0000-0000-000000000100", name="France", country_alpha3="FRA", file_name="FRA"
        )

    @pytest.fixture
    def region_zone(self, country_zone):
        return Zone.create(
            uuid="aaaaaaaa-0000-0000-0000-000000000101", name="Ile-de-France", file_name="FRA_IDF", parent=country_zone
        )

    @pytest.fixture
    def player(self, country_zone):
        from uuid import UUID

        return Player.create(
            uuid=UUID("aaaaaaaa-0000-0000-0000-000000000093"),
            name="T100Player",
            club_tag="OLD_TAG",
            country=country_zone,
        )

    def _call(self, tmp_path, season):
        func = get_top_100_per_country_func(str(tmp_path) + "/", season)
        func(None, None)
        return tmp_path

    def _read(self, tmp_path, season, alpha3):
        with open(tmp_path / f"top_100_by_country/{season.id}/{alpha3}.txt") as f:
            return json.loads(f.read())

    def test_club_tag_from_player_season_not_player(self, tmp_path, season, player):
        # PlayerSeason.club_tag differs from Player.club_tag → file must use PlayerSeason value
        PlayerSeason.create(player=player, season=season, rank=1, points=1000, club_tag="SEASON_TAG")
        self._call(tmp_path, season)
        row = self._read(tmp_path, season, "FRA")["results"][0]
        assert row["club_tag"] == "SEASON_TAG"
        assert row["club_tag"] != "OLD_TAG"

    def test_region_is_none_when_not_set(self, tmp_path, season, player):
        PlayerSeason.create(player=player, season=season, rank=1, points=1000)
        self._call(tmp_path, season)
        row = self._read(tmp_path, season, "FRA")["results"][0]
        assert row["region"] is None

    def test_region_is_object_when_set(self, tmp_path, season, player, region_zone):
        player.region = region_zone
        player.save()
        PlayerSeason.create(player=player, season=season, rank=1, points=1000)
        self._call(tmp_path, season)
        row = self._read(tmp_path, season, "FRA")["results"][0]
        assert row["region"] == {"name": "Ile-de-France", "file_name": "FRA_IDF"}


# ── get_maps_rank_distribution_func ──────────────────────────────────────────


class TestGetMapsRankDistribution:

    @pytest.fixture
    def season(self):
        now = datetime.now()
        return Season.create(name="MRD_S", start_time=now - timedelta(days=30), end_time=now + timedelta(days=30))

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="MRD_MAP_001", name="Test Map")

    def _make_game(self, map_obj, season, average_elo, trackmaster_limit=999999):
        return Game.create(
            map=map_obj,
            average_elo=average_elo,
            min_elo=average_elo - 150,
            max_elo=average_elo + 150,
            trackmaster_limit=trackmaster_limit,
            is_finished=True,
            time=season.start_time + timedelta(days=1),
        )

    def _call(self, tmp_path, season):
        func = get_maps_rank_distribution_func(str(tmp_path) + "/", season)
        func(None, None)

    def _read(self, tmp_path, season, map_uid):
        with open(tmp_path / f"maps_rank_distribution/{season.id}/{map_uid}.txt") as f:
            return json.loads(f.read())

    def _count(self, data, key):
        return next(r["count"] for r in data["results"] if r["rank"] == key)

    def test_file_created_for_map_with_games(self, tmp_path, season, map_obj):
        self._make_game(map_obj, season, average_elo=1000)
        self._call(tmp_path, season)
        path = tmp_path / f"maps_rank_distribution/{season.id}/MRD_MAP_001.txt"
        assert path.exists()

    def test_no_file_when_no_finished_games(self, tmp_path, season, map_obj):
        # Unfinished game — must not produce a file
        Game.create(
            map=map_obj,
            average_elo=1000,
            min_elo=850,
            max_elo=1150,
            is_finished=False,
            time=season.start_time + timedelta(days=1),
        )
        self._call(tmp_path, season)
        path = tmp_path / f"maps_rank_distribution/{season.id}/MRD_MAP_001.txt"
        assert not path.exists()

    def test_m1_game_counted_in_m1_only(self, tmp_path, season, map_obj):
        self._make_game(map_obj, season, average_elo=3150)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season, "MRD_MAP_001")
        assert self._count(data, "m1") == 1
        assert self._count(data, "m2") == 0
        assert self._count(data, "g3") == 0

    def test_tm_game_requires_average_elo_gte_trackmaster_limit(self, tmp_path, season, map_obj):
        # average_elo=4100, trackmaster_limit=4000 → TM
        self._make_game(map_obj, season, average_elo=4100, trackmaster_limit=4000)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season, "MRD_MAP_001")
        assert self._count(data, "tm") == 1
        assert self._count(data, "m3") == 0

    def test_m3_game_has_average_elo_below_trackmaster_limit(self, tmp_path, season, map_obj):
        # average_elo=3700, trackmaster_limit=4000 → M3 (3700 < 4000)
        self._make_game(map_obj, season, average_elo=3700, trackmaster_limit=4000)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season, "MRD_MAP_001")
        assert self._count(data, "m3") == 1
        assert self._count(data, "tm") == 0

    def test_m3_boundary_exactly_at_trackmaster_limit(self, tmp_path, season, map_obj):
        # average_elo == trackmaster_limit → TM (>= condition is inclusive)
        self._make_game(map_obj, season, average_elo=4000, trackmaster_limit=4000)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season, "MRD_MAP_001")
        assert self._count(data, "tm") == 1
        assert self._count(data, "m3") == 0

    def test_multiple_games_on_same_map_aggregated(self, tmp_path, season, map_obj):
        self._make_game(map_obj, season, average_elo=1150)  # s1
        self._make_game(map_obj, season, average_elo=1150)  # s1 again
        self._make_game(map_obj, season, average_elo=2150)  # g1
        self._call(tmp_path, season)
        data = self._read(tmp_path, season, "MRD_MAP_001")
        assert self._count(data, "s1") == 2
        assert self._count(data, "g1") == 1

    def test_games_outside_season_excluded(self, tmp_path, season, map_obj):
        Game.create(
            map=map_obj,
            average_elo=1000,
            min_elo=850,
            max_elo=1150,
            is_finished=True,
            time=season.end_time + timedelta(days=5),  # after season
        )
        self._call(tmp_path, season)
        path = tmp_path / f"maps_rank_distribution/{season.id}/MRD_MAP_001.txt"
        assert not path.exists()

    def test_result_contains_map_uid_and_name(self, tmp_path, season, map_obj):
        self._make_game(map_obj, season, average_elo=1000)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season, "MRD_MAP_001")
        assert data["map_uid"] == "MRD_MAP_001"
        assert data["map_name"] == "Test Map"

    def test_separate_files_for_separate_maps(self, tmp_path, season, map_obj):
        map2 = Map.create(uid="MRD_MAP_002", name="Map 2")
        self._make_game(map_obj, season, average_elo=1150)
        self._make_game(map2, season, average_elo=2150)
        self._call(tmp_path, season)
        d1 = self._read(tmp_path, season, "MRD_MAP_001")
        d2 = self._read(tmp_path, season, "MRD_MAP_002")
        assert self._count(d1, "s1") == 1
        assert self._count(d1, "g1") == 0
        assert self._count(d2, "g1") == 1
        assert self._count(d2, "s1") == 0


class TestGetMapsRankDistributionExhaustive:
    """Exhaustive coverage: all 13 ranks, boundaries, edge cases, isolation."""

    @pytest.fixture
    def season(self):
        now = datetime.now()
        return Season.create(name="MRDE_S", start_time=now - timedelta(days=30), end_time=now + timedelta(days=30))

    @pytest.fixture
    def map_obj(self):
        return Map.create(uid="MRDE_MAP_001", name="Exhaustive Map")

    def _game(self, map_obj, season, average_elo, trackmaster_limit=999999):
        return Game.create(
            map=map_obj,
            average_elo=average_elo,
            min_elo=average_elo - 25,
            max_elo=average_elo + 25,
            trackmaster_limit=trackmaster_limit,
            is_finished=True,
            time=season.start_time + timedelta(days=1),
        )

    def _call(self, tmp_path, season):
        get_maps_rank_distribution_func(str(tmp_path) + "/", season)(None, None)

    def _read(self, tmp_path, season):
        with open(tmp_path / f"maps_rank_distribution/{season.id}/MRDE_MAP_001.txt") as f:
            return json.loads(f.read())

    def _count(self, data, key):
        return next(r["count"] for r in data["results"] if r["rank"] == key)

    def _all_counts(self, data):
        return {r["rank"]: r["count"] for r in data["results"]}

    # ── each rank individually ────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "average_elo,expected_key",
        [
            (0, "b1"),
            (150, "b1"),
            (300, "b2"),
            (450, "b2"),
            (600, "b3"),
            (800, "b3"),
            (1000, "s1"),
            (1150, "s1"),
            (1300, "s2"),
            (1450, "s2"),
            (1600, "s3"),
            (1800, "s3"),
            (2000, "g1"),
            (2200, "g1"),
            (2300, "g2"),
            (2450, "g2"),
            (2600, "g3"),
            (2800, "g3"),
            (3000, "m1"),
            (3150, "m1"),
            (3300, "m2"),
            (3450, "m2"),
            (3600, "m3"),  # trackmaster_limit=999999 >> 3600 → M3
            (3800, "m3"),
        ],
    )
    def test_each_rank_assigned_correctly(self, tmp_path, season, map_obj, average_elo, expected_key):
        self._game(map_obj, season, average_elo=average_elo)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season)
        counts = self._all_counts(data)
        assert counts[expected_key] == 1
        # every other rank must be 0
        assert sum(v for k, v in counts.items() if k != expected_key) == 0

    def test_tm_when_average_elo_equals_trackmaster_limit(self, tmp_path, season, map_obj):
        self._game(map_obj, season, average_elo=4000, trackmaster_limit=4000)
        self._call(tmp_path, season)
        counts = self._all_counts(self._read(tmp_path, season))
        assert counts["tm"] == 1
        assert sum(v for k, v in counts.items() if k != "tm") == 0

    def test_tm_when_average_elo_above_trackmaster_limit(self, tmp_path, season, map_obj):
        self._game(map_obj, season, average_elo=4200, trackmaster_limit=4000)
        self._call(tmp_path, season)
        counts = self._all_counts(self._read(tmp_path, season))
        assert counts["tm"] == 1
        assert sum(v for k, v in counts.items() if k != "tm") == 0

    def test_m3_just_below_trackmaster_limit(self, tmp_path, season, map_obj):
        # average_elo=3999, trackmaster_limit=4000 → M3 (3999 < 4000)
        self._game(map_obj, season, average_elo=3999, trackmaster_limit=4000)
        self._call(tmp_path, season)
        counts = self._all_counts(self._read(tmp_path, season))
        assert counts["m3"] == 1
        assert counts["tm"] == 0

    # ── exact rank lower boundaries ───────────────────────────────────────────

    @pytest.mark.parametrize(
        "average_elo,key",
        [
            (300, "b2"),
            (600, "b3"),
            (1000, "s1"),
            (1300, "s2"),
            (1600, "s3"),
            (2000, "g1"),
            (2300, "g2"),
            (2600, "g3"),
            (3000, "m1"),
            (3300, "m2"),
            (3600, "m3"),
        ],
    )
    def test_lower_boundary_belongs_to_rank(self, tmp_path, season, map_obj, average_elo, key):
        self._game(map_obj, season, average_elo=average_elo)
        self._call(tmp_path, season)
        assert self._count(self._read(tmp_path, season), key) == 1

    @pytest.mark.parametrize(
        "average_elo,key",
        [
            (299, "b1"),
            (599, "b2"),
            (999, "b3"),
            (1299, "s1"),
            (1599, "s2"),
            (1999, "s3"),
            (2299, "g1"),
            (2599, "g2"),
            (2999, "g3"),
            (3299, "m1"),
            (3599, "m2"),
        ],
    )
    def test_one_below_boundary_belongs_to_lower_rank(self, tmp_path, season, map_obj, average_elo, key):
        self._game(map_obj, season, average_elo=average_elo)
        self._call(tmp_path, season)
        assert self._count(self._read(tmp_path, season), key) == 1

    # ── one-game-one-count guarantee ──────────────────────────────────────────

    def test_each_game_counted_exactly_once(self, tmp_path, season, map_obj):
        # Three games at three different ranks
        self._game(map_obj, season, average_elo=700)  # b3
        self._game(map_obj, season, average_elo=1450)  # s2
        self._game(map_obj, season, average_elo=3150)  # m1
        self._call(tmp_path, season)
        counts = self._all_counts(self._read(tmp_path, season))
        assert sum(counts.values()) == 3  # exactly 3 total, no double-counting

    # ── exclusions ────────────────────────────────────────────────────────────

    def test_game_with_average_elo_minus_one_excluded(self, tmp_path, season, map_obj):
        Game.create(
            map=map_obj,
            average_elo=-1,
            min_elo=-1,
            max_elo=-1,
            is_finished=True,
            time=season.start_time + timedelta(days=1),
        )
        self._call(tmp_path, season)
        assert not (tmp_path / f"maps_rank_distribution/{season.id}/MRDE_MAP_001.txt").exists()

    def test_game_before_season_start_excluded(self, tmp_path, season, map_obj):
        Game.create(
            map=map_obj,
            average_elo=1000,
            min_elo=850,
            max_elo=1150,
            is_finished=True,
            time=season.start_time - timedelta(days=1),
        )
        self._call(tmp_path, season)
        assert not (tmp_path / f"maps_rank_distribution/{season.id}/MRDE_MAP_001.txt").exists()

    def test_game_after_season_end_excluded(self, tmp_path, season, map_obj):
        Game.create(
            map=map_obj,
            average_elo=1000,
            min_elo=850,
            max_elo=1150,
            is_finished=True,
            time=season.end_time + timedelta(days=1),
        )
        self._call(tmp_path, season)
        assert not (tmp_path / f"maps_rank_distribution/{season.id}/MRDE_MAP_001.txt").exists()

    def test_unfinished_game_excluded(self, tmp_path, season, map_obj):
        Game.create(
            map=map_obj,
            average_elo=1000,
            min_elo=850,
            max_elo=1150,
            is_finished=False,
            time=season.start_time + timedelta(days=1),
        )
        self._call(tmp_path, season)
        assert not (tmp_path / f"maps_rank_distribution/{season.id}/MRDE_MAP_001.txt").exists()

    # ── results structure ─────────────────────────────────────────────────────

    def test_results_contain_all_13_ranks(self, tmp_path, season, map_obj):
        self._game(map_obj, season, average_elo=1150)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season)
        keys = {r["rank"] for r in data["results"]}
        assert keys == {r["key"] for r in RANKS}

    def test_results_contain_rank_name(self, tmp_path, season, map_obj):
        self._game(map_obj, season, average_elo=1150)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season)
        row = next(r for r in data["results"] if r["rank"] == "s1")
        assert row["name"] == "Silver I"

    def test_last_updated_field_present(self, tmp_path, season, map_obj):
        self._game(map_obj, season, average_elo=1150)
        self._call(tmp_path, season)
        data = self._read(tmp_path, season)
        assert "last_updated" in data
        assert isinstance(data["last_updated"], float)
