from datetime import datetime, timedelta
from uuid import UUID

from models import Game, Map, Player, PlayerGame
from src.threads.recompute_match_elo import RecomputeMatchEloThread

NOW = datetime.now().replace(microsecond=0)


def make_map(uid="MAP001"):
    return Map.create(uid=uid, name="Test Map")


def make_player(uuid_suffix="0001", points=1000):
    return Player.create(
        uuid=UUID(f"bbbbbbbb-0000-0000-0000-{uuid_suffix.zfill(12)}"),
        points=points,
        last_points_update=datetime.fromtimestamp(0),
    )


def make_game(map_obj, time=None, is_finished=True, min_elo=1000, max_elo=2000, average_elo=1500):
    return Game.create(
        map=map_obj,
        time=time or NOW,
        is_finished=is_finished,
        min_elo=min_elo,
        max_elo=max_elo,
        average_elo=average_elo,
    )


def make_player_game(player, game, points_after_match=None):
    return PlayerGame.create(player=player, game=game, points_after_match=points_after_match)


# ── recompute_elo ─────────────────────────────────────────────────────────────


class TestRecomputeElo:
    def test_sets_min_elo_from_points_after_match(self):
        m = make_map()
        game = make_game(m, min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=800)
        make_player_game(p2, game, points_after_match=1200)
        RecomputeMatchEloThread().recompute_elo(game)
        assert Game.get_by_id(game.id).min_elo == 800

    def test_sets_max_elo_from_points_after_match(self):
        m = make_map()
        game = make_game(m, min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=800)
        make_player_game(p2, game, points_after_match=1200)
        RecomputeMatchEloThread().recompute_elo(game)
        assert Game.get_by_id(game.id).max_elo == 1200

    def test_sets_average_elo_from_points_after_match(self):
        m = make_map()
        game = make_game(m, min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=800)
        make_player_game(p2, game, points_after_match=1200)
        RecomputeMatchEloThread().recompute_elo(game)
        assert Game.get_by_id(game.id).average_elo == 1000

    def test_average_elo_is_rounded(self):
        m = make_map()
        game = make_game(m)
        p1 = make_player("0001")
        p2 = make_player("0002")
        p3 = make_player("0003")
        make_player_game(p1, game, points_after_match=1000)
        make_player_game(p2, game, points_after_match=1001)
        make_player_game(p3, game, points_after_match=1001)
        RecomputeMatchEloThread().recompute_elo(game)
        # mean = 1000.67 → rounds to 1001
        assert Game.get_by_id(game.id).average_elo == 1001

    def test_exception_does_not_propagate(self):
        # Passing None as match should trigger an exception inside recompute_elo
        thread = RecomputeMatchEloThread()
        thread.recompute_elo(None)  # must not raise

    def test_exception_increments_error_count(self):
        thread = RecomputeMatchEloThread()
        thread.recompute_elo(None)
        assert thread.error_count > 0


# ── get_divergent_matches ─────────────────────────────────────────────────────


class TestGetDivergentMatches:
    def _range(self, hours_back=2):
        end = NOW + timedelta(hours=1)
        start = NOW - timedelta(hours=hours_back)
        return start, end

    def test_detects_min_divergence(self):
        m = make_map()
        game = make_game(m, min_elo=2000, max_elo=2000, average_elo=2000)
        p1 = make_player("0001")
        p2 = make_player("0002")
        # min(pam)=500, min_elo=2000 → diff=1500 > 300
        make_player_game(p1, game, points_after_match=500)
        make_player_game(p2, game, points_after_match=600)
        start, end = self._range()
        matches = list(RecomputeMatchEloThread().get_divergent_matches(start, end))
        assert any(g.id == game.id for g in matches)

    def test_detects_max_divergence(self):
        m = make_map()
        game = make_game(m, min_elo=1000, max_elo=3000, average_elo=2000)
        p1 = make_player("0001")
        p2 = make_player("0002")
        # max(pam)=1200, max_elo=3000 → diff=1800 > 300
        make_player_game(p1, game, points_after_match=800)
        make_player_game(p2, game, points_after_match=1200)
        start, end = self._range()
        matches = list(RecomputeMatchEloThread().get_divergent_matches(start, end))
        assert any(g.id == game.id for g in matches)

    def test_skips_game_within_threshold(self):
        m = make_map()
        # min_elo=1000, max_elo=1100; pam values 900/1000 → diff=100 ≤ 300
        game = make_game(m, min_elo=1000, max_elo=1100, average_elo=1050)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=900)
        make_player_game(p2, game, points_after_match=1000)
        start, end = self._range()
        matches = list(RecomputeMatchEloThread().get_divergent_matches(start, end))
        assert not any(g.id == game.id for g in matches)

    def test_skips_game_with_null_points_after_match(self):
        m = make_map()
        game = make_game(m, min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=500)
        make_player_game(p2, game, points_after_match=None)  # incomplete
        start, end = self._range()
        matches = list(RecomputeMatchEloThread().get_divergent_matches(start, end))
        assert not any(g.id == game.id for g in matches)

    def test_skips_unfinished_game(self):
        m = make_map()
        game = make_game(m, is_finished=False, min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=500)
        make_player_game(p2, game, points_after_match=600)
        start, end = self._range()
        matches = list(RecomputeMatchEloThread().get_divergent_matches(start, end))
        assert not any(g.id == game.id for g in matches)

    def test_skips_game_with_elo_not_yet_computed(self):
        m = make_map()
        game = make_game(m, min_elo=-1, max_elo=-1, average_elo=-1)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=500)
        make_player_game(p2, game, points_after_match=600)
        start, end = self._range()
        matches = list(RecomputeMatchEloThread().get_divergent_matches(start, end))
        assert not any(g.id == game.id for g in matches)

    def test_skips_game_outside_time_range(self):
        m = make_map()
        old_game = make_game(m, time=NOW - timedelta(days=10), min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, old_game, points_after_match=500)
        make_player_game(p2, old_game, points_after_match=600)
        start = NOW - timedelta(hours=2)
        end = NOW + timedelta(hours=1)
        matches = list(RecomputeMatchEloThread().get_divergent_matches(start, end))
        assert not any(g.id == old_game.id for g in matches)


# ── run_iteration ─────────────────────────────────────────────────────────────


class TestRunIteration:
    def test_returns_count_of_fixed_matches(self):
        m = make_map()
        game = make_game(m, min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=500)
        make_player_game(p2, game, points_after_match=600)
        start = NOW - timedelta(hours=2)
        end = NOW + timedelta(hours=1)
        count = RecomputeMatchEloThread().run_iteration(start, end)
        assert count == 1

    def test_applies_fix(self):
        m = make_map()
        game = make_game(m, min_elo=2000, max_elo=3000, average_elo=2500)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=500)
        make_player_game(p2, game, points_after_match=700)
        start = NOW - timedelta(hours=2)
        end = NOW + timedelta(hours=1)
        RecomputeMatchEloThread().run_iteration(start, end)
        g = Game.get_by_id(game.id)
        assert g.min_elo == 500
        assert g.max_elo == 700
        assert g.average_elo == 600

    def test_returns_zero_when_nothing_to_fix(self):
        m = make_map()
        game = make_game(m, min_elo=600, max_elo=800, average_elo=700)
        p1 = make_player("0001")
        p2 = make_player("0002")
        make_player_game(p1, game, points_after_match=590)
        make_player_game(p2, game, points_after_match=800)
        start = NOW - timedelta(hours=2)
        end = NOW + timedelta(hours=1)
        count = RecomputeMatchEloThread().run_iteration(start, end)
        assert count == 0
