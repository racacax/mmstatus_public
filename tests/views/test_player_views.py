"""
Tests for win/loss filtering in player-facing API views.

All win/loss counts and rates must only reflect *finished* games.
'played' totals must include every game regardless of is_finished.
"""

from datetime import datetime, timedelta
from uuid import UUID

import pytest

from models import Game, Map, Player, PlayerGame, PlayerSeason, Season
from src.player_views import PlayerAPIViews

PLAYER_UUID = UUID("aaaaaaaa-0000-0000-0000-000000000001")
OPPONENT_UUID = UUID("bbbbbbbb-0000-0000-0000-000000000002")
MAP_UID = "TEST_MAP_UID_001"
MAP_UID_2 = "TEST_MAP_UID_002"


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def player():
    return Player.create(uuid=PLAYER_UUID, name="TestPlayer")


@pytest.fixture
def opponent():
    return Player.create(uuid=OPPONENT_UUID, name="Opponent")


@pytest.fixture
def map_obj():
    return Map.create(uid=MAP_UID, name="Test Map")


@pytest.fixture
def map_obj2():
    return Map.create(uid=MAP_UID_2, name="Test Map 2")


@pytest.fixture
def season():
    now = datetime.now()
    return Season.create(
        name="Test Season",
        start_time=now - timedelta(weeks=4),
        end_time=now + timedelta(weeks=4),
    )


@pytest.fixture
def player_season(player, season):
    return PlayerSeason.create(player=player, season=season, points=1000, rank=50)


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_game(map_obj, is_finished, time=None):
    return Game.create(
        map=map_obj,
        is_finished=is_finished,
        time=time or datetime.now().replace(microsecond=0),
    )


def make_player_game(player, game, is_win=False, is_mvp=False):
    return PlayerGame.create(player=player, game=game, is_win=is_win, is_mvp=is_mvp)


def make_versus_game(map_obj, player, opponent, player_wins, is_finished):
    """Game where player and opponent are on opposite teams."""
    game = make_game(map_obj, is_finished=is_finished)
    make_player_game(player, game, is_win=player_wins)
    make_player_game(opponent, game, is_win=not player_wins)
    return game


def make_along_game(map_obj, player, opponent, player_wins, is_finished):
    """Game where player and opponent are on the same team (identical outcome)."""
    game = make_game(map_obj, is_finished=is_finished)
    make_player_game(player, game, is_win=player_wins)
    make_player_game(opponent, game, is_win=player_wins)
    return game


# ── get_map_statistics ────────────────────────────────────────────────────────


class TestGetMapStatistics:
    """
    wins / losses and their rates must only count finished games.
    played counts every game (finished + unfinished).
    mvprate keeps total played as its denominator (user-visible total, not an outcome).
    """

    def _call(self, **kwargs):
        defaults = dict(player=PLAYER_UUID, order_by="played", order="desc", page=1, min_date=0, max_date=None)
        defaults.update(kwargs)
        return PlayerAPIViews.get_map_statistics(**defaults)

    def _row(self, results, uid=MAP_UID):
        return next(r for r in results if r["map_uid"] == uid)

    # ── wins ──────────────────────────────────────────────────────────────────

    def test_wins_excludes_unfinished_games(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)  # must not count

        _, data = self._call()
        assert self._row(data["results"])["wins"] == 2

    def test_wins_zero_when_all_unfinished(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)

        _, data = self._call()
        assert self._row(data["results"])["wins"] == 0

    # ── losses ────────────────────────────────────────────────────────────────

    def test_losses_excludes_unfinished_games(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)  # must not count

        _, data = self._call()
        assert self._row(data["results"])["losses"] == 2

    def test_losses_zero_when_all_unfinished(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)

        _, data = self._call()
        assert self._row(data["results"])["losses"] == 0

    # ── played ────────────────────────────────────────────────────────────────

    def test_played_includes_unfinished_games(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)

        _, data = self._call()
        assert self._row(data["results"])["played"] == 3

    def test_played_counts_all_when_all_unfinished(self, player, map_obj):
        for _ in range(4):
            make_player_game(player, make_game(map_obj, is_finished=False))

        _, data = self._call()
        assert self._row(data["results"])["played"] == 4

    # ── winrate / lossrate denominators ──────────────────────────────────────

    def test_winrate_denominator_is_finished_count_not_total(self, player, map_obj):
        """2 wins out of 3 finished (+ 2 unfinished) → winrate = 2/3*100, not 2/5*100."""
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)

        _, data = self._call()
        row = self._row(data["results"])
        assert row["played"] == 5
        assert row["wins"] == 2
        assert abs(row["winrate"] - 200 / 3) < 0.1  # 66.67, not 40.0

    def test_lossrate_denominator_is_finished_count_not_total(self, player, map_obj):
        """1 loss out of 3 finished (+ 2 unfinished) → lossrate = 1/3*100, not 1/5*100."""
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)

        _, data = self._call()
        row = self._row(data["results"])
        assert row["losses"] == 1
        assert abs(row["lossrate"] - 100 / 3) < 0.1  # 33.33, not 20.0

    def test_winrate_lossrate_zero_when_no_finished_game(self, player, map_obj):
        """No division-by-zero; both rates are 0 when no game is finished."""
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)

        _, data = self._call()
        row = self._row(data["results"])
        assert row["winrate"] == 0.0
        assert row["lossrate"] == 0.0

    # ── mvprate ───────────────────────────────────────────────────────────────

    def test_mvprate_uses_total_played_not_finished(self, player, map_obj):
        """mvprate is a participation stat; its denominator stays total played."""
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True, is_mvp=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False, is_mvp=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False, is_mvp=False)

        _, data = self._call()
        row = self._row(data["results"])
        # 1 mvp / 3 total games = 33.33 (not 1/2 = 50 if only counting finished)
        assert row["mvps"] == 1
        assert abs(row["mvprate"] - 100 / 3) < 0.1

    # ── multi-map isolation ───────────────────────────────────────────────────

    def test_stats_are_independent_per_map(self, player, map_obj, map_obj2):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)  # unfinished on map1
        make_player_game(player, make_game(map_obj2, is_finished=True), is_win=False)

        _, data = self._call()
        row1 = self._row(data["results"], MAP_UID)
        row2 = self._row(data["results"], MAP_UID_2)
        assert row1["wins"] == 1
        assert row1["played"] == 2
        assert row2["losses"] == 1
        assert row2["played"] == 1

    # ── baseline: all finished ────────────────────────────────────────────────

    def test_all_finished_standard_computation(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)

        status, data = self._call()
        row = self._row(data["results"])
        assert status == 200
        assert row["played"] == 3
        assert row["wins"] == 2
        assert row["losses"] == 1
        assert abs(row["winrate"] - 200 / 3) < 0.1
        assert abs(row["lossrate"] - 100 / 3) < 0.1


# ── get_statistics ────────────────────────────────────────────────────────────


class TestGetStatistics:
    """
    total_wins and total_losses must only count finished games.
    total_played counts all games.
    """

    def _call(self, **kwargs):
        defaults = dict(player=PLAYER_UUID, min_date=0, max_date=None, season=-1)
        defaults.update(kwargs)
        return PlayerAPIViews.get_statistics(**defaults)

    def test_total_wins_excludes_unfinished_games(self, player, map_obj, player_season):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)  # must not count

        status, data = self._call()
        assert status == 200
        assert data["stats"]["total_wins"] == 2

    def test_total_wins_zero_when_all_unfinished(self, player, map_obj, player_season):
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)

        _, data = self._call()
        assert data["stats"]["total_wins"] == 0

    def test_total_losses_excludes_unfinished_games(self, player, map_obj, player_season):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)  # must not count

        _, data = self._call()
        assert data["stats"]["total_losses"] == 2

    def test_total_losses_zero_when_all_unfinished(self, player, map_obj, player_season):
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)

        _, data = self._call()
        assert data["stats"]["total_losses"] == 0

    def test_total_played_includes_unfinished_games(self, player, map_obj, player_season):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)

        _, data = self._call()
        assert data["stats"]["total_played"] == 3

    def test_total_played_counts_all_when_all_unfinished(self, player, map_obj, player_season):
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)

        _, data = self._call()
        assert data["stats"]["total_played"] == 3

    def test_mix_finished_and_unfinished(self, player, map_obj, player_season):
        """3 finished (2W/1L) + 2 unfinished: only the finished ones count in wins/losses."""
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=False), is_win=False)

        _, data = self._call()
        stats = data["stats"]
        assert stats["total_played"] == 5
        assert stats["total_wins"] == 2
        assert stats["total_losses"] == 1

    def test_all_finished_standard_computation(self, player, map_obj, player_season):
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=True)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)
        make_player_game(player, make_game(map_obj, is_finished=True), is_win=False)

        status, data = self._call()
        stats = data["stats"]
        assert status == 200
        assert stats["total_played"] == 3
        assert stats["total_wins"] == 1
        assert stats["total_losses"] == 2


# ── get_opponents_statistics ──────────────────────────────────────────────────


class TestGetOpponentsStatistics:
    """
    games_won/lost_against/along must only count finished games.
    total_played, total_played_against, total_played_along count all games
    (they reflect game participation / team composition, not outcomes).
    """

    def _call(self, **kwargs):
        defaults = dict(
            player=PLAYER_UUID,
            order_by="played",
            order="desc",
            page=1,
            min_date=0,
            max_date=None,
            group_by="uuid",
        )
        defaults.update(kwargs)
        return PlayerAPIViews.get_opponents_statistics(**defaults)

    def _row(self, results):
        return next(r for r in results if str(r["uuid"]) == str(OPPONENT_UUID))

    # ── games_won_against ─────────────────────────────────────────────────────

    def test_games_won_against_excludes_unfinished(self, player, opponent, map_obj):
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=True)
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=False)  # must not count

        _, data = self._call()
        assert self._row(data["results"])["total_games_won_against"] == 1

    def test_games_won_against_zero_when_all_unfinished(self, player, opponent, map_obj):
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=False)
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=False)

        _, data = self._call()
        assert self._row(data["results"])["total_games_won_against"] == 0

    # ── games_lost_against ────────────────────────────────────────────────────

    def test_games_lost_against_excludes_unfinished(self, player, opponent, map_obj):
        make_versus_game(map_obj, player, opponent, player_wins=False, is_finished=True)
        make_versus_game(map_obj, player, opponent, player_wins=False, is_finished=False)  # must not count

        _, data = self._call()
        assert self._row(data["results"])["total_games_lost_against"] == 1

    def test_games_lost_against_zero_when_all_unfinished(self, player, opponent, map_obj):
        make_versus_game(map_obj, player, opponent, player_wins=False, is_finished=False)
        make_versus_game(map_obj, player, opponent, player_wins=False, is_finished=False)

        _, data = self._call()
        assert self._row(data["results"])["total_games_lost_against"] == 0

    # ── games_won_along ───────────────────────────────────────────────────────

    def test_games_won_along_excludes_unfinished(self, player, opponent, map_obj):
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=True)
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=False)  # must not count

        _, data = self._call()
        assert self._row(data["results"])["total_games_won_along"] == 1

    def test_games_won_along_zero_when_all_unfinished(self, player, opponent, map_obj):
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=False)
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=False)

        _, data = self._call()
        assert self._row(data["results"])["total_games_won_along"] == 0

    # ── games_lost_along ──────────────────────────────────────────────────────

    def test_games_lost_along_excludes_unfinished(self, player, opponent, map_obj):
        make_along_game(map_obj, player, opponent, player_wins=False, is_finished=True)
        make_along_game(map_obj, player, opponent, player_wins=False, is_finished=False)  # must not count

        _, data = self._call()
        assert self._row(data["results"])["total_games_lost_along"] == 1

    def test_games_lost_along_zero_when_all_unfinished(self, player, opponent, map_obj):
        make_along_game(map_obj, player, opponent, player_wins=False, is_finished=False)
        make_along_game(map_obj, player, opponent, player_wins=False, is_finished=False)

        _, data = self._call()
        assert self._row(data["results"])["total_games_lost_along"] == 0

    # ── total_played / total_played_against / total_played_along ─────────────

    def test_total_played_includes_unfinished(self, player, opponent, map_obj):
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=True)
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=False)
        make_along_game(map_obj, player, opponent, player_wins=False, is_finished=False)

        _, data = self._call()
        assert self._row(data["results"])["total_played"] == 3

    def test_total_played_against_includes_unfinished(self, player, opponent, map_obj):
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=True)
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=False)

        _, data = self._call()
        assert self._row(data["results"])["total_played_against"] == 2

    def test_total_played_along_includes_unfinished(self, player, opponent, map_obj):
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=True)
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=False)

        _, data = self._call()
        assert self._row(data["results"])["total_played_along"] == 2

    # ── comprehensive scenario ────────────────────────────────────────────────

    def test_mix_all_types_finished_and_unfinished(self, player, opponent, map_obj):
        """3 finished + 2 unfinished; only outcome fields (won/lost) are filtered."""
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=True)  # won_against
        make_versus_game(map_obj, player, opponent, player_wins=False, is_finished=True)  # lost_against
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=True)  # won_along
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=False)  # unfinished, no outcome
        make_along_game(map_obj, player, opponent, player_wins=False, is_finished=False)  # unfinished, no outcome

        _, data = self._call()
        row = self._row(data["results"])
        assert row["total_played"] == 5
        assert row["total_games_won_against"] == 1
        assert row["total_games_lost_against"] == 1
        assert row["total_games_won_along"] == 1
        assert row["total_games_lost_along"] == 0

    def test_all_unfinished_outcome_fields_are_zero(self, player, opponent, map_obj):
        make_versus_game(map_obj, player, opponent, player_wins=True, is_finished=False)
        make_versus_game(map_obj, player, opponent, player_wins=False, is_finished=False)
        make_along_game(map_obj, player, opponent, player_wins=True, is_finished=False)
        make_along_game(map_obj, player, opponent, player_wins=False, is_finished=False)

        _, data = self._call()
        row = self._row(data["results"])
        assert row["total_games_won_against"] == 0
        assert row["total_games_lost_against"] == 0
        assert row["total_games_won_along"] == 0
        assert row["total_games_lost_along"] == 0
        assert row["total_played"] == 4


# ── get_activity_heatmap ──────────────────────────────────────────────────────

# Known weekdays used across heatmap tests (all in January 2024):
#   2024-01-06 = Saturday  → day 6   (DAYOFWEEK=7, minus 1)
#   2024-01-07 = Sunday    → day 0   (DAYOFWEEK=1, minus 1)
#   2024-01-08 = Monday    → day 1   (DAYOFWEEK=2, minus 1)
#   2024-01-09 = Tuesday   → day 2   (DAYOFWEEK=3, minus 1)
#   2024-01-13 = Saturday  → day 6   (DAYOFWEEK=7, minus 1)
#   2024-01-14 = Sunday    → day 0   (DAYOFWEEK=1, minus 1)

SAT = datetime(2024, 1, 6)
SUN = datetime(2024, 1, 7)
MON = datetime(2024, 1, 8)
TUE = datetime(2024, 1, 9)
SAT2 = datetime(2024, 1, 13)
SUN2 = datetime(2024, 1, 14)


def at(base: datetime, hour: int) -> datetime:
    """Return *base* date with the given hour (minutes/seconds zeroed)."""
    return base.replace(hour=hour, minute=0, second=0, microsecond=0)


class TestGetActivityHeatmap:
    def _call(self, **kwargs):
        defaults = dict(player=PLAYER_UUID, min_date=0, max_date=None)
        defaults.update(kwargs)
        return PlayerAPIViews.get_activity_heatmap(**defaults)

    def _cell(self, results, day, hour):
        """Return the cell matching (day, hour), or None if absent."""
        return next((r for r in results if r["day"] == day and r["hour"] == hour), None)

    # ── status / shape ────────────────────────────────────────────────────────

    def test_returns_200(self, player):
        status, _ = self._call()
        assert status == 200

    def test_empty_results_when_no_games(self, player):
        _, data = self._call()
        assert data["results"] == []

    def test_echoes_player_uuid(self, player):
        _, data = self._call()
        assert data["player"] == PLAYER_UUID

    # ── day encoding: DAYOFWEEK() - 1 ────────────────────────────────────────

    def test_sunday_encoded_as_day_zero(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=14) is not None

    def test_monday_encoded_as_day_one(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(MON, 9)))
        _, data = self._call()
        assert self._cell(data["results"], day=1, hour=9) is not None

    def test_saturday_encoded_as_day_six(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SAT, 20)))
        _, data = self._call()
        assert self._cell(data["results"], day=6, hour=20) is not None

    def test_all_seven_days_encode_correctly(self, player, map_obj):
        # Jan 7-13 2024: Sun Mon Tue Wed Thu Fri Sat → days 0-6
        for offset, expected_day in enumerate(range(7)):
            dt = (SUN + __import__("datetime").timedelta(days=offset)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            make_player_game(player, make_game(map_obj, is_finished=True, time=dt))
        _, data = self._call()
        present_days = {r["day"] for r in data["results"]}
        assert present_days == set(range(7))

    # ── hour encoding ─────────────────────────────────────────────────────────

    def test_hour_zero_encoded_correctly(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 0)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=0) is not None

    def test_hour_23_encoded_correctly(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 23)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=23) is not None

    # ── aggregation ───────────────────────────────────────────────────────────

    def test_multiple_games_same_slot_are_summed(self, player, map_obj):
        for _ in range(4):
            make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=14)["count"] == 4

    def test_different_hours_same_day_produce_separate_cells(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 10)))
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 10)))
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=10)["count"] == 2
        assert self._cell(data["results"], day=0, hour=14)["count"] == 1

    def test_same_hour_different_days_produce_separate_cells(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 10)))
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(MON, 10)))
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(MON, 10)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=10)["count"] == 1
        assert self._cell(data["results"], day=1, hour=10)["count"] == 2

    def test_cells_with_no_games_absent_from_results(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        _, data = self._call()
        assert len(data["results"]) == 1
        assert self._cell(data["results"], day=0, hour=15) is None

    # ── ordering ──────────────────────────────────────────────────────────────

    def test_results_ordered_by_day_then_hour(self, player, map_obj):
        # Insert in reverse order to confirm ordering is by value, not insertion
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SAT, 20)))  # day 6 h20
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))  # day 0 h14
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 8)))  # day 0 h8
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(MON, 10)))  # day 1 h10
        _, data = self._call()
        results = data["results"]
        assert [(r["day"], r["hour"]) for r in results] == [(0, 8), (0, 14), (1, 10), (6, 20)]

    # ── date filtering ────────────────────────────────────────────────────────

    def test_min_date_excludes_earlier_games(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))  # before
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(MON, 10)))  # after
        min_ts = int(at(MON, 0).timestamp())
        _, data = self._call(min_date=min_ts)
        assert self._cell(data["results"], day=0, hour=14) is None
        assert self._cell(data["results"], day=1, hour=10) is not None

    def test_max_date_excludes_later_games(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))  # before
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(MON, 10)))  # after
        max_ts = int(at(SUN, 23).timestamp())
        _, data = self._call(max_date=max_ts)
        assert self._cell(data["results"], day=0, hour=14) is not None
        assert self._cell(data["results"], day=1, hour=10) is None

    def test_min_and_max_date_window_keeps_only_matching_games(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SAT, 12)))  # day before window
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))  # inside window
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(TUE, 12)))  # day after window
        min_ts = int(at(SUN, 0).timestamp())
        max_ts = int(at(SUN, 23).timestamp())
        _, data = self._call(min_date=min_ts, max_date=max_ts)
        assert len(data["results"]) == 1
        assert data["results"][0] == {"day": 0, "hour": 14, "count": 1}

    def test_no_games_in_window_returns_empty(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        # Window is Mon only — no games there
        min_ts = int(at(MON, 0).timestamp())
        max_ts = int(at(MON, 23).timestamp())
        _, data = self._call(min_date=min_ts, max_date=max_ts)
        assert data["results"] == []

    def test_same_weekday_across_multiple_weeks_aggregated(self, player, map_obj):
        # Two Sundays — both should land in day=0, hour=10 and be summed
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 10)))
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN2, 10)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=10)["count"] == 2

    # ── player isolation ──────────────────────────────────────────────────────

    def test_only_counts_games_for_requested_player(self, player, opponent, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        make_player_game(opponent, make_game(map_obj, is_finished=True, time=at(SUN, 14)))  # other player
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=14)["count"] == 1

    def test_returns_empty_for_player_with_no_games(self, player, opponent, map_obj):
        make_player_game(opponent, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        _, data = self._call(player=PLAYER_UUID)
        assert data["results"] == []

    # ── finished / unfinished ─────────────────────────────────────────────────

    def test_counts_unfinished_games(self, player, map_obj):
        """Heatmap reflects participation; unfinished games must be counted."""
        make_player_game(player, make_game(map_obj, is_finished=False, time=at(SUN, 14)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=14)["count"] == 1

    def test_finished_and_unfinished_games_both_counted(self, player, map_obj):
        make_player_game(player, make_game(map_obj, is_finished=True, time=at(SUN, 14)))
        make_player_game(player, make_game(map_obj, is_finished=False, time=at(SUN, 14)))
        _, data = self._call()
        assert self._cell(data["results"], day=0, hour=14)["count"] == 2
