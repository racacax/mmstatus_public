from datetime import datetime, timedelta
from uuid import UUID

from models import Game, Map, Player, PlayerGame, PlayerSeason, Season
from src.services import NadeoLive
from src.threads.update_player_ranks import UpdatePlayerRanksThread

# ── helpers ───────────────────────────────────────────────────────────────────

NOW = datetime.now().replace(microsecond=0)


def make_map(uid="MAP001"):
    return Map.create(uid=uid, name="Test Map")


def make_season(name="Current", start_delta=-timedelta(days=30), end_delta=timedelta(days=60)):
    return Season.create(name=name, start_time=NOW + start_delta, end_time=NOW + end_delta)


def make_player(uuid_suffix="0001", points=1000, last_match=None, last_points_update=None, points_fetch_retries=0):
    return Player.create(
        uuid=UUID(f"aaaaaaaa-0000-0000-0000-{uuid_suffix.zfill(12)}"),
        points=points,
        last_match=last_match,
        last_points_update=last_points_update or datetime.fromtimestamp(0),
        points_fetch_retries=points_fetch_retries,
    )


def make_game(map_obj, time=None, is_finished=True):
    return Game.create(map=map_obj, time=time or NOW, is_finished=is_finished)


def make_player_game(player, game, points_after_match=None, rank_after_match=None):
    return PlayerGame.create(
        player=player,
        game=game,
        points_after_match=points_after_match,
        rank_after_match=rank_after_match,
    )


def fake_ranks(players_points: dict):
    """Build the NadeoLive.get_player_ranks response for {uuid_str: (score, rank)} mapping."""
    return {
        "results": [
            {"player": str(uuid), "score": score, "rank": rank} for uuid, (score, rank) in players_points.items()
        ]
    }


def patch_ranks(monkeypatch, mapping: dict):
    monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: fake_ranks(mapping))


# ── update_player: basic updates ──────────────────────────────────────────────


class TestUpdatePlayerBasic:
    def test_updates_player_points(self):
        season = make_season()
        make_map()
        p = make_player()
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert Player.get_by_id(p.uuid).points == 1500

    def test_updates_player_rank(self):
        season = make_season()
        p = make_player()
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert Player.get_by_id(p.uuid).rank == 200

    def test_updates_last_points_update(self):
        season = make_season()
        p = make_player()
        before = datetime.now().replace(microsecond=0)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert Player.get_by_id(p.uuid).last_points_update >= before

    def test_does_not_create_player_season_if_not_exists(self):
        season = make_season()
        p = make_player()
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert PlayerSeason.get_or_none(player=p, season=season) is None

    def test_updates_existing_player_season_points(self):
        season = make_season()
        p = make_player()
        PlayerSeason.create(player=p, season=season, points=500, rank=999)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        ps = PlayerSeason.get(player=p, season=season)
        assert ps.points == 1500

    def test_updates_existing_player_season_rank(self):
        season = make_season()
        p = make_player()
        PlayerSeason.create(player=p, season=season, points=500, rank=999)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        ps = PlayerSeason.get(player=p, season=season)
        assert ps.rank == 200

    def test_updates_existing_player_season(self):
        season = make_season()
        p = make_player()
        PlayerSeason.create(player=p, season=season, points=500, rank=999)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        ps = PlayerSeason.get(player=p, season=season)
        assert ps.points == 1500
        assert ps.rank == 200

    def test_no_crash_when_no_current_season(self):
        # season=None should not raise (just skip PlayerSeason step)
        p = make_player()
        UpdatePlayerRanksThread().update_player(p, 1500, 200, None)
        # points still updated
        assert Player.get_by_id(p.uuid).points == 1500

    def test_no_player_season_created_when_points_zero(self):
        season = make_season()
        p = make_player()
        UpdatePlayerRanksThread().update_player(p, 0, 0, season)
        assert PlayerSeason.get_or_none(player=p, season=season) is None


# ── update_player: points_after_match (same season) ──────────────────────────


class TestUpdatePlayerPointsAfterMatchSameSeason:
    def test_sets_points_after_match_when_none(self):
        season = make_season()
        m = make_map()
        game = make_game(m, time=NOW - timedelta(hours=1))
        p = make_player(last_match=game)
        pg = make_player_game(p, game, points_after_match=None)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert PlayerGame.get_by_id(pg.id).points_after_match == 1500

    def test_sets_rank_after_match_when_none(self):
        season = make_season()
        m = make_map()
        game = make_game(m, time=NOW - timedelta(hours=1))
        p = make_player(last_match=game)
        make_player_game(p, game, points_after_match=None)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        pg = PlayerGame.get(player=p, game=game)
        assert pg.rank_after_match == 200

    def test_does_not_overwrite_existing_points_after_match(self):
        season = make_season()
        m = make_map()
        game = make_game(m, time=NOW - timedelta(hours=1))
        p = make_player(last_match=game)
        pg = make_player_game(p, game, points_after_match=999)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert PlayerGame.get_by_id(pg.id).points_after_match == 999

    def test_no_update_when_no_last_match(self):
        season = make_season()
        p = make_player(last_match=None)
        # Should not raise and no PlayerGame to update
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert Player.get_by_id(p.uuid).points == 1500

    def test_last_match_not_finished_uses_previous_finished_game(self):
        season = make_season()
        m = make_map()
        finished_game = make_game(m, time=NOW - timedelta(hours=2), is_finished=True)
        unfinished_game = make_game(m, time=NOW - timedelta(hours=1), is_finished=False)
        p = make_player(last_match=unfinished_game)
        make_player_game(p, unfinished_game)
        pg_finished = make_player_game(p, finished_game, points_after_match=None)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert PlayerGame.get_by_id(pg_finished.id).points_after_match == 1500

    def test_last_match_not_finished_no_finished_games_no_crash(self):
        season = make_season()
        m = make_map()
        unfinished = make_game(m, is_finished=False)
        p = make_player(last_match=unfinished)
        make_player_game(p, unfinished)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, season)
        assert Player.get_by_id(p.uuid).points == 1500


# ── update_player: points_after_match (previous season) ──────────────────────


class TestUpdatePlayerPointsAfterMatchPreviousSeason:
    def _setup_transition(self):
        """Create old + new season, a game in the old season, and a PlayerSeason for the old season."""
        transition = NOW - timedelta(hours=2)
        old_season = Season.create(
            name="Old Season",
            start_time=NOW - timedelta(days=60),
            end_time=transition,
        )
        new_season = Season.create(
            name="New Season",
            start_time=transition,
            end_time=NOW + timedelta(days=90),
        )
        m = make_map()
        # game time falls inside old season
        game = make_game(m, time=NOW - timedelta(hours=3))
        return old_season, new_season, game

    def test_uses_old_player_season_points_for_points_after_match(self):
        old_season, new_season, game = self._setup_transition()
        p = make_player(last_match=game)
        PlayerSeason.create(player=p, season=old_season, points=800, rank=300)
        pg = make_player_game(p, game, points_after_match=None)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, new_season)
        assert PlayerGame.get_by_id(pg.id).points_after_match == 800

    def test_uses_old_player_season_rank_for_rank_after_match(self):
        old_season, new_season, game = self._setup_transition()
        p = make_player(last_match=game)
        PlayerSeason.create(player=p, season=old_season, points=800, rank=300)
        pg = make_player_game(p, game, points_after_match=None)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, new_season)
        assert PlayerGame.get_by_id(pg.id).rank_after_match == 300

    def test_falls_back_to_api_points_when_no_old_player_season(self):
        old_season, new_season, game = self._setup_transition()
        p = make_player(last_match=game)
        # No PlayerSeason for old_season
        pg = make_player_game(p, game, points_after_match=None)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, new_season)
        assert PlayerGame.get_by_id(pg.id).points_after_match == 1500

    def test_still_updates_new_player_season_from_api(self):
        old_season, new_season, game = self._setup_transition()
        p = make_player(last_match=game)
        PlayerSeason.create(player=p, season=old_season, points=800, rank=300)
        PlayerSeason.create(player=p, season=new_season, points=0, rank=0)
        make_player_game(p, game)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, new_season)
        ps = PlayerSeason.get(player=p, season=new_season)
        assert ps.points == 1500
        assert ps.rank == 200

    def test_does_not_modify_old_player_season(self):
        old_season, new_season, game = self._setup_transition()
        p = make_player(last_match=game)
        PlayerSeason.create(player=p, season=old_season, points=800, rank=300)
        make_player_game(p, game)
        UpdatePlayerRanksThread().update_player(p, 1500, 200, new_season)
        old_ps = PlayerSeason.get(player=p, season=old_season)
        assert old_ps.points == 800


# ── update_player: error handling ────────────────────────────────────────────


class TestUpdatePlayerErrorHandling:
    def test_exception_increments_error_count(self):
        season = make_season()
        # Pass an invalid player object to trigger an exception
        thread = UpdatePlayerRanksThread()
        thread.update_player(None, 1500, 200, season)
        assert thread.error_count > 0

    def test_exception_does_not_propagate(self):
        season = make_season()
        thread = UpdatePlayerRanksThread()
        # Should not raise
        thread.update_player(None, 1500, 200, season)


# ── update_players ────────────────────────────────────────────────────────────


class TestUpdatePlayers:
    def test_updates_each_player_with_correct_score(self, monkeypatch):
        season = make_season()
        p1 = make_player(uuid_suffix="0001")
        p2 = make_player(uuid_suffix="0002")
        patch_ranks(monkeypatch, {p1.uuid: (1000, 10), p2.uuid: (2000, 5)})
        UpdatePlayerRanksThread().update_players([p1, p2], season)
        assert Player.get_by_id(p1.uuid).points == 1000
        assert Player.get_by_id(p2.uuid).points == 2000

    def test_updates_each_player_with_correct_rank(self, monkeypatch):
        season = make_season()
        p1 = make_player(uuid_suffix="0001")
        patch_ranks(monkeypatch, {p1.uuid: (1000, 42)})
        UpdatePlayerRanksThread().update_players([p1], season)
        assert Player.get_by_id(p1.uuid).rank == 42

    def test_player_absent_from_api_keeps_points(self, monkeypatch):
        season = make_season()
        p = make_player(uuid_suffix="0001", points=500)
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: {"results": []})
        UpdatePlayerRanksThread().update_players([p], season)
        assert Player.get_by_id(p.uuid).points == 500

    def test_player_absent_from_api_increments_retries(self, monkeypatch):
        season = make_season()
        p = make_player(uuid_suffix="0001", points=500, points_fetch_retries=3)
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: {"results": []})
        UpdatePlayerRanksThread().update_players([p], season)
        assert Player.get_by_id(p.uuid).points_fetch_retries == 4

    def test_player_absent_from_api_updates_last_points_update(self, monkeypatch):
        season = make_season()
        p = make_player(uuid_suffix="0001", points=500)
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: {"results": []})
        before = datetime.now().replace(microsecond=0)
        UpdatePlayerRanksThread().update_players([p], season)
        assert Player.get_by_id(p.uuid).last_points_update >= before

    def test_player_absent_after_max_retries_gets_zero(self, monkeypatch):
        season = make_season()
        p = make_player(uuid_suffix="0001", points=500, points_fetch_retries=19)
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: {"results": []})
        UpdatePlayerRanksThread().update_players([p], season)
        assert Player.get_by_id(p.uuid).points == 0

    def test_player_absent_after_max_retries_resets_retries(self, monkeypatch):
        season = make_season()
        p = make_player(uuid_suffix="0001", points=500, points_fetch_retries=19)
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: {"results": []})
        UpdatePlayerRanksThread().update_players([p], season)
        assert Player.get_by_id(p.uuid).points_fetch_retries == 0

    def test_player_present_resets_retries(self, monkeypatch):
        season = make_season()
        p = make_player(uuid_suffix="0001", points=500, points_fetch_retries=5)
        patch_ranks(monkeypatch, {p.uuid: (1500, 100)})
        UpdatePlayerRanksThread().update_players([p], season)
        assert Player.get_by_id(p.uuid).points_fetch_retries == 0

    def test_api_exception_records_error(self, monkeypatch):
        season = make_season()
        p = make_player()
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: (_ for _ in ()).throw(RuntimeError("down")))
        thread = UpdatePlayerRanksThread()
        thread.update_players([p], season)
        assert thread.error_count > 0

    def test_api_exception_does_not_propagate(self, monkeypatch):
        season = make_season()
        p = make_player()
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: (_ for _ in ()).throw(RuntimeError("down")))
        UpdatePlayerRanksThread().update_players([p], season)  # must not raise

    def test_updates_player_season_for_each_player_when_exists(self, monkeypatch):
        season = make_season()
        p1 = make_player(uuid_suffix="0001")
        p2 = make_player(uuid_suffix="0002")
        PlayerSeason.create(player=p1, season=season, points=0, rank=0)
        PlayerSeason.create(player=p2, season=season, points=0, rank=0)
        patch_ranks(monkeypatch, {p1.uuid: (1000, 10), p2.uuid: (2000, 5)})
        UpdatePlayerRanksThread().update_players([p1, p2], season)
        assert PlayerSeason.get(player=p1, season=season).points == 1000
        assert PlayerSeason.get(player=p2, season=season).points == 2000

    def test_does_not_create_player_season_when_not_exists(self, monkeypatch):
        season = make_season()
        p1 = make_player(uuid_suffix="0001")
        patch_ranks(monkeypatch, {p1.uuid: (1000, 10)})
        UpdatePlayerRanksThread().update_players([p1], season)
        assert PlayerSeason.get_or_none(player=p1, season=season) is None


# ── run_iteration ─────────────────────────────────────────────────────────────


class TestRunIteration:
    def test_no_api_call_when_no_outdated_players(self, monkeypatch):
        make_season()
        called = [False]
        monkeypatch.setattr(NadeoLive, "get_player_ranks", lambda ids: called.__setitem__(0, True) or {"results": []})
        # Player recently updated — not outdated
        make_player(last_points_update=datetime.now())
        UpdatePlayerRanksThread().run_iteration()
        assert called[0] is False

    def test_processes_player_with_epoch_last_update(self, monkeypatch):
        make_season()
        p = make_player(points=500, last_points_update=datetime.fromtimestamp(0))
        patch_ranks(monkeypatch, {p.uuid: (1500, 100)})
        UpdatePlayerRanksThread().run_iteration()
        assert Player.get_by_id(p.uuid).points == 1500

    def test_processes_player_with_outdated_update(self, monkeypatch):
        make_season()
        p = make_player(points=500, last_points_update=datetime.now() - timedelta(hours=13))
        patch_ranks(monkeypatch, {p.uuid: (1500, 100)})
        UpdatePlayerRanksThread().run_iteration()
        assert Player.get_by_id(p.uuid).points == 1500

    def test_skips_zero_points_player_if_previously_updated(self, monkeypatch):
        make_season()
        called_with = []
        # recently updated, 0 points → should be skipped
        p = make_player(points=0, last_points_update=datetime.now() - timedelta(hours=13))
        monkeypatch.setattr(
            NadeoLive,
            "get_player_ranks",
            lambda ids: called_with.extend(ids) or {"results": []},
        )
        UpdatePlayerRanksThread().run_iteration()
        assert str(p.uuid) not in called_with

    def test_processes_at_most_100_players(self, monkeypatch):
        make_season()
        [make_player(uuid_suffix=str(i).zfill(4)) for i in range(110)]
        processed = []
        monkeypatch.setattr(
            NadeoLive,
            "get_player_ranks",
            lambda ids: processed.extend(ids) or {"results": []},
        )
        UpdatePlayerRanksThread().run_iteration()
        assert len(processed) <= 100

    def test_processes_oldest_first(self, monkeypatch):
        make_season()
        older = make_player(uuid_suffix="0001", points=500, last_points_update=datetime.now() - timedelta(hours=20))
        newer = make_player(uuid_suffix="0002", points=500, last_points_update=datetime.now() - timedelta(hours=14))
        order = []
        monkeypatch.setattr(
            NadeoLive,
            "get_player_ranks",
            lambda ids: order.extend(ids) or {"results": []},
        )
        UpdatePlayerRanksThread().run_iteration()
        assert order.index(str(older.uuid)) < order.index(str(newer.uuid))

    def test_retries_zero_points_player_with_pending_retries(self, monkeypatch):
        make_season()
        called_with = []
        # 0 points but retries > 0 — should still be fetched
        p = make_player(points=0, last_points_update=datetime.now() - timedelta(hours=13), points_fetch_retries=2)
        monkeypatch.setattr(
            NadeoLive,
            "get_player_ranks",
            lambda ids: called_with.extend(ids) or {"results": []},
        )
        UpdatePlayerRanksThread().run_iteration()
        assert str(p.uuid) in called_with

    def test_no_crash_when_no_seasons(self, monkeypatch):
        p = make_player()
        patch_ranks(monkeypatch, {p.uuid: (1000, 10)})
        UpdatePlayerRanksThread().run_iteration()  # must not raise
