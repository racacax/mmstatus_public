from datetime import datetime, timedelta

from models import Game, Map, Season
from src.threads.update_maps import (
    UpdateMapsThread,
    check_season_transition,
    expected_next_season,
    parse_map_season,
)

# ── helpers ───────────────────────────────────────────────────────────────────


# Use real now so Season.get_current_season() (which calls datetime.now() internally) finds these seasons.
def _now():
    return datetime.now().replace(microsecond=0)


TRANSITION = _now() - timedelta(hours=1)


def make_season(name, start=None, end=None):
    now = _now()
    start = start or now - timedelta(days=30)
    end = end or now + timedelta(days=60)
    return Season.create(name=name, start_time=start, end_time=end)


def make_map(uid="MAP001", name=""):
    return Map.create(uid=uid, name=name)


def make_game(map_obj, time=None):
    return Game.create(map=map_obj, time=time or _now())


# ── parse_map_season ──────────────────────────────────────────────────────────


class TestParseMapSeason:
    def test_spring_map(self):
        assert parse_map_season("Spring 2025 - 01") == ("Spring", 2025)

    def test_summer_map(self):
        assert parse_map_season("Summer 2025 - 05") == ("Summer", 2025)

    def test_fall_map(self):
        assert parse_map_season("Fall 2024 - 10") == ("Fall", 2024)

    def test_winter_map(self):
        assert parse_map_season("Winter 2023 - 03") == ("Winter", 2023)

    def test_extra_spaces_before_dash(self):
        assert parse_map_season("Spring 2025  - 01") == ("Spring", 2025)

    def test_no_match_returns_none(self):
        assert parse_map_season("Random Map Name") is None

    def test_empty_string_returns_none(self):
        assert parse_map_season("") is None

    def test_lowercase_season_returns_none(self):
        assert parse_map_season("spring 2025 - 01") is None

    def test_missing_year_returns_none(self):
        assert parse_map_season("Spring - 01") is None

    def test_missing_dash_returns_none(self):
        assert parse_map_season("Spring 2025 01") is None

    def test_country_name_with_year_returns_none(self):
        assert parse_map_season("France 2026") is None

    def test_year_is_integer(self):
        result = parse_map_season("Fall 2024 - 03")
        assert isinstance(result[1], int)

    def test_returns_tuple(self):
        result = parse_map_season("Winter 2022 - 01")
        assert isinstance(result, tuple) and len(result) == 2

    def test_map_number_doesnt_affect_season(self):
        assert parse_map_season("Spring 2025 - 99") == ("Spring", 2025)
        assert parse_map_season("Spring 2025 - 01") == ("Spring", 2025)


# ── expected_next_season ──────────────────────────────────────────────────────


class TestExpectedNextSeason:
    def test_spring_to_summer(self):
        assert expected_next_season("Spring 2025") == ("Summer", 2025)

    def test_summer_to_fall(self):
        assert expected_next_season("Summer 2025") == ("Fall", 2025)

    def test_fall_to_winter_increments_year(self):
        assert expected_next_season("Fall 2025") == ("Winter", 2026)

    def test_winter_to_spring_same_year(self):
        assert expected_next_season("Winter 2025") == ("Spring", 2025)

    def test_winter_2026_to_spring_2026(self):
        assert expected_next_season("Winter 2026") == ("Spring", 2026)

    def test_invalid_season_name_returns_none(self):
        assert expected_next_season("BadSeason 2025") is None

    def test_empty_string_returns_none(self):
        assert expected_next_season("") is None

    def test_missing_year_returns_none(self):
        assert expected_next_season("Spring") is None

    def test_lowercase_returns_none(self):
        assert expected_next_season("spring 2025") is None

    def test_returns_tuple(self):
        result = expected_next_season("Spring 2025")
        assert isinstance(result, tuple) and len(result) == 2

    def test_year_is_integer(self):
        result = expected_next_season("Fall 2024")
        assert isinstance(result[1], int)


# ── check_season_transition ───────────────────────────────────────────────────


class TestCheckSeasonTransition:

    # ── guard conditions ──────────────────────────────────────────────────────

    def test_no_op_when_map_name_does_not_match_pattern(self):
        make_season("Spring 2025")
        m = make_map(name="Random Map Name")
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    def test_no_op_when_map_name_has_year_but_no_season_keyword(self):
        make_season("Spring 2025")
        m = make_map(name="France 2026")
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    def test_no_op_when_no_current_season(self):
        # No seasons in DB at all
        m = make_map(name="Summer 2025 - 01")
        check_season_transition(m)
        assert Season.select().count() == 0

    def test_no_op_when_current_season_name_is_invalid(self):
        make_season("BadSeason 2025")
        m = make_map(name="Summer 2025 - 01")
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    def test_no_op_when_map_is_not_the_next_season(self):
        make_season("Spring 2025")
        m = make_map(name="Fall 2025 - 01")  # skips Summer
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    def test_no_op_when_map_is_current_season(self):
        make_season("Spring 2025")
        m = make_map(name="Spring 2025 - 01")
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    def test_no_op_when_map_is_same_year_different_season_not_next(self):
        make_season("Summer 2025")
        m = make_map(name="Spring 2025 - 01")  # previous season
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    def test_no_op_when_no_games_on_map(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    # ── successful transition ─────────────────────────────────────────────────

    def test_creates_new_season_on_transition(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        assert Season.select().where(Season.name == "Summer 2025").exists()

    def test_new_season_count_increases_by_one(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        make_game(m, time=TRANSITION)
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count + 1

    def test_closes_current_season_with_transition_time(self):
        spring = make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        spring_updated = Season.get_by_id(spring.id)
        assert spring_updated.end_time == TRANSITION

    def test_new_season_start_time_equals_transition_time(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        new_season = Season.get(Season.name == "Summer 2025")
        assert new_season.start_time == TRANSITION

    def test_new_season_end_time_is_120_days_after_transition(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        new_season = Season.get(Season.name == "Summer 2025")
        assert new_season.end_time == TRANSITION + timedelta(days=150)

    def test_transition_time_uses_first_game_not_latest(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        early = TRANSITION
        late = TRANSITION + timedelta(hours=2)
        make_game(m, time=late)
        make_game(m, time=early)
        check_season_transition(m)
        new_season = Season.get(Season.name == "Summer 2025")
        assert new_season.start_time == early

    def test_fall_summer_transition(self):
        make_season("Summer 2025")
        m = make_map(name="Fall 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        assert Season.select().where(Season.name == "Fall 2025").exists()

    def test_fall_to_winter_transition_increments_year(self):
        make_season("Fall 2025")
        m = make_map(name="Winter 2026 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        assert Season.select().where(Season.name == "Winter 2026").exists()

    def test_winter_to_spring_transition_same_year(self):
        make_season("Winter 2025")
        m = make_map(name="Spring 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        assert Season.select().where(Season.name == "Spring 2025").exists()

    def test_winter_to_spring_does_not_create_wrong_year(self):
        make_season("Winter 2025")
        m = make_map(name="Spring 2026 - 01")  # wrong year
        make_game(m, time=TRANSITION)
        initial_count = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == initial_count

    def test_winter_2026_to_spring_2026_transition(self):
        make_season("Winter 2026")
        m = make_map(name="Spring 2026 - 10")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        assert Season.select().where(Season.name == "Spring 2026").exists()

    # ── idempotency ───────────────────────────────────────────────────────────

    def test_idempotent_second_call_does_not_create_duplicate(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        count_after_first = Season.select().count()
        check_season_transition(m)
        assert Season.select().count() == count_after_first

    def test_idempotent_does_not_overwrite_existing_season(self):
        make_season("Spring 2025")
        m = make_map(name="Summer 2025 - 01")
        make_game(m, time=TRANSITION)
        check_season_transition(m)
        new_season = Season.get(Season.name == "Summer 2025")
        original_start = new_season.start_time

        # Add a later game and call again
        make_game(m, time=TRANSITION + timedelta(hours=5))
        check_season_transition(m)
        new_season_again = Season.get(Season.name == "Summer 2025")
        assert new_season_again.start_time == original_start


# ── UpdateMapsThread.run_iteration integration ────────────────────────────────


class TestRunIterationSeasonTransition:
    def _make_thread(self, monkeypatch, map_name):
        """Monkeypatch NadeoLive.get_map_info to return a fixed name."""
        from src.services import NadeoLive

        monkeypatch.setattr(NadeoLive, "get_map_info", lambda uid: {"name": map_name})
        return UpdateMapsThread()

    def test_run_iteration_triggers_season_transition(self, monkeypatch):
        make_season("Spring 2025")
        m = make_map(uid="M1", name="")  # empty name — will be fetched
        make_game(m, time=TRANSITION)
        thread = self._make_thread(monkeypatch, "Summer 2025 - 01")
        thread.run_iteration()
        assert Season.select().where(Season.name == "Summer 2025").exists()

    def test_run_iteration_no_transition_when_map_has_name(self, monkeypatch):
        """Maps that already have a name are not processed by run_iteration."""
        make_season("Spring 2025")
        m = make_map(uid="M1", name="Summer 2025 - 01")  # already named
        make_game(m, time=TRANSITION)
        from src.services import NadeoLive

        called = [False]

        def fake_get(uid):
            called[0] = True
            return {"name": "Summer 2025 - 01"}

        monkeypatch.setattr(NadeoLive, "get_map_info", fake_get)
        thread = UpdateMapsThread()
        thread.run_iteration()
        # API should not be called since map already has a name
        assert called[0] is False
        # No new season created
        assert Season.select().count() == 1

    def test_run_iteration_no_transition_when_not_next_season(self, monkeypatch):
        make_season("Spring 2025")
        m = make_map(uid="M1", name="")
        make_game(m, time=TRANSITION)
        thread = self._make_thread(monkeypatch, "Fall 2025 - 01")  # skips Summer
        thread.run_iteration()
        assert not Season.select().where(Season.name == "Fall 2025").exists()

    def test_run_iteration_updates_map_name(self, monkeypatch):
        make_season("Spring 2025")
        m = make_map(uid="M1", name="")
        make_game(m, time=TRANSITION)
        thread = self._make_thread(monkeypatch, "Summer 2025 - 01")
        thread.run_iteration()
        m_updated = Map.get_by_id(m.uid)
        assert m_updated.name == "Summer 2025 - 01"

    def test_run_iteration_no_maps_does_nothing(self, monkeypatch):
        from src.services import NadeoLive

        called = [False]
        monkeypatch.setattr(NadeoLive, "get_map_info", lambda uid: called.__setitem__(0, True) or {})
        thread = UpdateMapsThread()
        thread.run_iteration()  # should not raise
        assert called[0] is False

    def test_run_iteration_saves_non_season_map_name(self, monkeypatch):
        """A map whose name doesn't match the season pattern is still saved."""
        make_season("Spring 2025")
        m = make_map(uid="M1", name="")
        thread = self._make_thread(monkeypatch, "Some Random Map")
        thread.run_iteration()
        assert Map.get_by_id(m.uid).name == "Some Random Map"

    def test_run_iteration_no_transition_for_non_season_map(self, monkeypatch):
        make_season("Spring 2025")
        m = make_map(uid="M1", name="")
        make_game(m, time=TRANSITION)
        thread = self._make_thread(monkeypatch, "Some Random Map")
        thread.run_iteration()
        assert Season.select().count() == 1

    def test_run_iteration_processes_all_empty_name_maps(self, monkeypatch):
        """All maps with empty names are fetched in one pass."""
        from src.services import NadeoLive

        make_map(uid="M1", name="")
        make_map(uid="M2", name="")
        make_map(uid="M3", name="")
        fetched = []
        monkeypatch.setattr(NadeoLive, "get_map_info", lambda uid: fetched.append(uid) or {"name": f"Name {uid}"})
        UpdateMapsThread().run_iteration()
        assert set(fetched) == {"M1", "M2", "M3"}
        assert Map.get_by_id("M1").name == "Name M1"
        assert Map.get_by_id("M2").name == "Name M2"
        assert Map.get_by_id("M3").name == "Name M3"

    def test_run_iteration_skips_named_maps(self, monkeypatch):
        """Maps that already have a name are not fetched."""
        from src.services import NadeoLive

        make_map(uid="M1", name="Already Named")
        fetched = []
        monkeypatch.setattr(NadeoLive, "get_map_info", lambda uid: fetched.append(uid) or {"name": "x"})
        UpdateMapsThread().run_iteration()
        assert "M1" not in fetched

    def test_run_iteration_exception_records_error(self, monkeypatch):
        """An API exception increments the error counter."""
        from src.services import NadeoLive

        make_map(uid="M1", name="")
        monkeypatch.setattr(NadeoLive, "get_map_info", lambda uid: (_ for _ in ()).throw(RuntimeError("API down")))
        thread = UpdateMapsThread()
        thread.run_iteration()
        assert thread.error_count > 0

    def test_run_iteration_exception_continues_processing_other_maps(self, monkeypatch):
        """An exception on one map does not stop processing subsequent maps."""
        from src.services import NadeoLive

        make_map(uid="M1", name="")
        make_map(uid="M2", name="")
        call_count = [0]

        def fake_get(uid):
            call_count[0] += 1
            if uid == "M1":
                raise RuntimeError("API down")
            return {"name": "Good Name"}

        monkeypatch.setattr(NadeoLive, "get_map_info", fake_get)
        UpdateMapsThread().run_iteration()
        assert Map.get_by_id("M2").name == "Good Name"
        assert call_count[0] == 2

    def test_run_iteration_only_one_transition_across_multiple_maps(self, monkeypatch):
        """Even with multiple maps resolved in one pass, season is created only once."""
        from src.services import NadeoLive

        make_season("Spring 2025")
        m1 = make_map(uid="M1", name="")
        m2 = make_map(uid="M2", name="")
        make_game(m1, time=TRANSITION)
        make_game(m2, time=TRANSITION + timedelta(minutes=30))

        monkeypatch.setattr(NadeoLive, "get_map_info", lambda uid: {"name": "Summer 2025 - 01"})
        UpdateMapsThread().run_iteration()
        assert Season.select().where(Season.name == "Summer 2025").count() == 1
