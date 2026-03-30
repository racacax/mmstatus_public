from datetime import datetime, timedelta
from uuid import UUID

from models import Player, PlayerSeason, Season
from src.threads.update_top_player_positions import UpdateTopPlayersPositionThread

# ── helpers ───────────────────────────────────────────────────────────────────

NOW = datetime.now().replace(microsecond=0)


def make_season(name="Current", active=True):
    if active:
        return Season.create(name=name, start_time=NOW - timedelta(days=30), end_time=NOW + timedelta(days=30))
    return Season.create(name=name, start_time=NOW - timedelta(days=60), end_time=NOW - timedelta(days=30))


def make_player(suffix, points):
    return Player.create(uuid=UUID(f"aaaaaaaa-0000-0000-0000-{suffix:012d}"), points=points)


def make_player_season(player, season, rank=99999):
    return PlayerSeason.create(player=player, season=season, points=player.points, rank=rank)


def run():
    UpdateTopPlayersPositionThread.run_iteration()


def fresh(model):
    return model.__class__.get_by_id(
        model.__class__._meta.primary_key.python_value(getattr(model, str(model.__class__._meta.primary_key.name)))
    )


def reload_player(p):
    return Player.get_by_id(p.uuid)


def reload_ps(ps):
    return PlayerSeason.get_by_id(ps.id)


# ── Player.rank updates ───────────────────────────────────────────────────────


class TestPlayerRankUpdates:

    def test_single_player_gets_rank_1(self):
        p = make_player(1, points=5000)
        run()
        assert reload_player(p).rank == 1

    def test_players_ranked_by_points_descending(self):
        p1 = make_player(1, points=5000)
        p2 = make_player(2, points=3000)
        p3 = make_player(3, points=1000)
        run()
        assert reload_player(p1).rank == 1
        assert reload_player(p2).rank == 2
        assert reload_player(p3).rank == 3

    def test_ranks_are_sequential_starting_at_1(self):
        players = [make_player(i, points=5000 - i * 100) for i in range(5)]
        run()
        ranks = sorted(reload_player(p).rank for p in players)
        assert ranks == list(range(1, 6))

    def test_only_top_200_are_ranked(self):
        # Create 201 players; the lowest-points one should not get rank 201 since
        # paginate(1, 200) excludes it — but Player.rank is not reset, so we just
        # verify the top 200 received ranks 1-200.
        players = [make_player(i, points=10000 - i * 10) for i in range(201)]
        run()
        top_200_ranks = sorted(reload_player(p).rank for p in players[:200])
        assert top_200_ranks == list(range(1, 201))

    def test_player_rank_updated_regardless_of_season_membership(self):
        # Player with no PlayerSeason still gets Player.rank updated
        p = make_player(1, points=5000)
        # no make_player_season called
        run()
        assert reload_player(p).rank == 1

    def test_equal_points_players_both_ranked(self):
        p1 = make_player(1, points=3000)
        p2 = make_player(2, points=3000)
        run()
        ranks = {reload_player(p1).rank, reload_player(p2).rank}
        assert ranks == {1, 2}


# ── PlayerSeason.rank updates ─────────────────────────────────────────────────


class TestPlayerSeasonRankUpdates:

    def test_player_season_rank_updated_for_current_season(self):
        s = make_season(active=True)
        p = make_player(1, points=5000)
        ps = make_player_season(p, s, rank=99)
        run()
        assert reload_ps(ps).rank == 1

    def test_player_season_rank_matches_player_rank(self):
        s = make_season(active=True)
        p1 = make_player(1, points=5000)
        p2 = make_player(2, points=3000)
        ps1 = make_player_season(p1, s)
        ps2 = make_player_season(p2, s)
        run()
        assert reload_ps(ps1).rank == reload_player(p1).rank
        assert reload_ps(ps2).rank == reload_player(p2).rank

    def test_player_season_not_updated_when_no_active_season(self):
        s = make_season(active=False)
        p = make_player(1, points=5000)
        ps = make_player_season(p, s, rank=42)
        run()
        # No active season → PlayerSeason unchanged
        assert reload_ps(ps).rank == 42

    def test_player_season_not_updated_for_past_season(self):
        past = make_season(name="Past", active=False)
        current = make_season(name="Current", active=True)
        p = make_player(1, points=5000)
        past_ps = make_player_season(p, past, rank=77)
        make_player_season(p, current)
        run()
        # Past season's PlayerSeason rank must remain untouched
        assert reload_ps(past_ps).rank == 77

    def test_player_without_player_season_does_not_create_one(self):
        make_season(active=True)
        p = make_player(1, points=5000)
        # no PlayerSeason created
        run()
        assert PlayerSeason.select().where(PlayerSeason.player_id == p.uuid).count() == 0

    def test_player_season_rank_order_matches_points_order(self):
        s = make_season(active=True)
        p1 = make_player(1, points=9000)
        p2 = make_player(2, points=7000)
        p3 = make_player(3, points=5000)
        ps1 = make_player_season(p1, s)
        ps2 = make_player_season(p2, s)
        ps3 = make_player_season(p3, s)
        run()
        ranks = [reload_ps(ps).rank for ps in [ps1, ps2, ps3]]
        assert ranks == sorted(ranks)
        assert ranks[0] == 1

    def test_only_players_with_player_season_in_current_season_updated(self):
        s = make_season(active=True)
        p1 = make_player(1, points=5000)
        p2 = make_player(2, points=4000)
        ps1 = make_player_season(p1, s, rank=99)
        # p2 has no PlayerSeason
        run()
        # p1's PlayerSeason updated, p2 has no PlayerSeason to update
        assert reload_ps(ps1).rank == 1
        assert PlayerSeason.select().where(PlayerSeason.player_id == p2.uuid).count() == 0

    def test_multiple_seasons_only_current_updated(self):
        past = make_season(name="Past", active=False)
        current = make_season(name="Current", active=True)
        p = make_player(1, points=5000)
        past_ps = make_player_season(p, past, rank=55)
        current_ps = make_player_season(p, current, rank=99)
        run()
        assert reload_ps(current_ps).rank == 1
        assert reload_ps(past_ps).rank == 55  # untouched
