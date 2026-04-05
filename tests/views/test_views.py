from datetime import datetime, timedelta
from uuid import UUID

from models import Player, PlayerSeason, Season, Zone
from src.views import APIViews


from src.utils import RANKS as _RANKS
from src.views import APIViews as _APIViews

# ── helpers ───────────────────────────────────────────────────────────────────


def make_season(name="Test Season", active=True, offset_days=30):
    now = datetime.now()
    if active:
        return Season.create(
            name=name, start_time=now - timedelta(days=offset_days), end_time=now + timedelta(days=offset_days)
        )
    return Season.create(
        name=name, start_time=now - timedelta(days=offset_days * 2), end_time=now - timedelta(days=offset_days)
    )


def make_player(uid, name="Player", country=None, club_tag=None):
    return Player.create(uuid=UUID(uid), name=name, country=country, club_tag=club_tag)


def make_player_season(player, season, points, rank, club_tag=None):
    return PlayerSeason.create(player=player, season=season, points=points, rank=rank, club_tag=club_tag)


def make_zone(uid, name, alpha3):
    return Zone.create(uuid=uid, name=name, country_alpha3=alpha3, file_name=alpha3)


def call(**kwargs):
    defaults = dict(season=-1, page=1, limit=50)
    defaults.update(kwargs)
    return APIViews.get_global_leaderboard(**defaults)


# ── get_global_leaderboard ────────────────────────────────────────────────────


class TestGetGlobalLeaderboard:

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_200(self):
        s = make_season()
        status, _ = call(season=s.id)
        assert status == 200

    def test_empty_results_when_no_players(self):
        s = make_season()
        _, data = call(season=s.id)
        assert data["results"] == []

    def test_result_row_has_expected_keys(self):
        s = make_season()
        p = make_player("aaaaaaaa-0000-0000-0000-000000000001", "Alice")
        make_player_season(p, s, points=1000, rank=1)
        _, data = call(season=s.id)
        assert set(data["results"][0].keys()) == {"rank", "points", "name", "uuid", "club_tag", "country"}

    def test_response_has_pagination_fields(self):
        s = make_season()
        _, data = call(season=s.id)
        assert "page" in data
        assert "limit" in data
        assert "total" in data

    def test_uuid_is_string(self):
        s = make_season()
        p = make_player("aaaaaaaa-0000-0000-0000-000000000002", "Bob")
        make_player_season(p, s, points=500, rank=1)
        _, data = call(season=s.id)
        assert isinstance(data["results"][0]["uuid"], str)

    # ── total count ───────────────────────────────────────────────────────────

    def test_total_reflects_all_players_in_season(self):
        s = make_season()
        for i in range(5):
            p = make_player(f"aaaaaaaa-0000-0000-0000-{i:012d}", f"P{i}")
            make_player_season(p, s, points=1000 - i * 100, rank=i + 1)
        _, data = call(season=s.id, limit=2)
        assert data["total"] == 5

    def test_total_is_zero_for_empty_season(self):
        s = make_season()
        _, data = call(season=s.id)
        assert data["total"] == 0

    # ── ordering ──────────────────────────────────────────────────────────────

    def test_results_ordered_by_rank_ascending(self):
        s = make_season()
        for rank, i in [(3, 0), (1, 1), (2, 2)]:
            p = make_player(f"bbbbbbbb-0000-0000-0000-{i:012d}", f"P{i}")
            make_player_season(p, s, points=1000 - rank * 100, rank=rank)
        _, data = call(season=s.id)
        ranks = [r["rank"] for r in data["results"]]
        assert ranks == sorted(ranks)
        assert ranks[0] == 1

    def test_rank_1_player_appears_first(self):
        s = make_season()
        for rank, name, i in [(2, "Second", 0), (1, "First", 1)]:
            p = make_player(f"cccccccc-0000-0000-0000-{i:012d}", name)
            make_player_season(p, s, points=5000 - rank * 100, rank=rank)
        _, data = call(season=s.id)
        assert data["results"][0]["name"] == "First"

    # ── pagination ────────────────────────────────────────────────────────────

    def test_page_1_returns_first_n_results(self):
        s = make_season()
        for i in range(10):
            p = make_player(f"dddddddd-0000-0000-0000-{i:012d}", f"P{i}")
            make_player_season(p, s, points=1000 - i * 10, rank=i + 1)
        _, data = call(season=s.id, page=1, limit=3)
        assert len(data["results"]) == 3
        assert data["results"][0]["rank"] == 1

    def test_page_2_returns_next_results(self):
        s = make_season()
        for i in range(10):
            p = make_player(f"eeeeeeee-0000-0000-0000-{i:012d}", f"P{i}")
            make_player_season(p, s, points=1000 - i * 10, rank=i + 1)
        _, data = call(season=s.id, page=2, limit=3)
        assert len(data["results"]) == 3
        assert data["results"][0]["rank"] == 4

    def test_last_page_returns_remaining_results(self):
        s = make_season()
        for i in range(5):
            p = make_player(f"ffffffff-0000-0000-0000-{i:012d}", f"P{i}")
            make_player_season(p, s, points=1000 - i * 10, rank=i + 1)
        _, data = call(season=s.id, page=2, limit=3)
        assert len(data["results"]) == 2

    def test_page_beyond_total_returns_empty(self):
        s = make_season()
        p = make_player("aaaaaaaa-1000-0000-0000-000000000001", "Solo")
        make_player_season(p, s, points=1000, rank=1)
        _, data = call(season=s.id, page=99, limit=50)
        assert data["results"] == []

    def test_page_echoed_in_response(self):
        s = make_season()
        _, data = call(season=s.id, page=3, limit=10)
        assert data["page"] == 3

    def test_limit_echoed_in_response(self):
        s = make_season()
        _, data = call(season=s.id, page=1, limit=25)
        assert data["limit"] == 25

    # ── limit clamping ────────────────────────────────────────────────────────

    def test_limit_capped_at_200(self):
        s = make_season()
        _, data = call(season=s.id, limit=999)
        assert data["limit"] == 200

    def test_limit_minimum_is_1(self):
        s = make_season()
        _, data = call(season=s.id, limit=0)
        assert data["limit"] == 1

    def test_negative_limit_clamped_to_1(self):
        s = make_season()
        _, data = call(season=s.id, limit=-5)
        assert data["limit"] == 1

    def test_page_minimum_is_1(self):
        s = make_season()
        _, data = call(season=s.id, page=0)
        assert data["page"] == 1

    def test_negative_page_clamped_to_1(self):
        s = make_season()
        _, data = call(season=s.id, page=-3)
        assert data["page"] == 1

    # ── season filter ─────────────────────────────────────────────────────────

    def test_only_players_from_requested_season_returned(self):
        s1 = make_season("S1")
        s2 = make_season("S2")
        p1 = make_player("aaaaaaaa-2000-0000-0000-000000000001", "InS1")
        p2 = make_player("aaaaaaaa-2000-0000-0000-000000000002", "InS2")
        make_player_season(p1, s1, points=1000, rank=1)
        make_player_season(p2, s2, points=1000, rank=1)
        _, data = call(season=s1.id)
        names = [r["name"] for r in data["results"]]
        assert "InS1" in names
        assert "InS2" not in names

    def test_player_in_two_seasons_appears_in_each(self):
        s1 = make_season("S1")
        s2 = make_season("S2")
        p = make_player("aaaaaaaa-3000-0000-0000-000000000001", "Multi")
        make_player_season(p, s1, points=800, rank=1)
        make_player_season(p, s2, points=900, rank=1)
        _, d1 = call(season=s1.id)
        _, d2 = call(season=s2.id)
        assert d1["results"][0]["points"] == 800
        assert d2["results"][0]["points"] == 900

    # ── default season ────────────────────────────────────────────────────────

    def test_default_season_uses_current_season(self):
        s = make_season(active=True)
        p = make_player("aaaaaaaa-4000-0000-0000-000000000001", "Current")
        make_player_season(p, s, points=1000, rank=1)
        _, data = call(season=-1)
        names = [r["name"] for r in data["results"]]
        assert "Current" in names

    # ── country field ─────────────────────────────────────────────────────────

    def test_country_populated_from_zone(self):
        s = make_season()
        z = make_zone("zfra-view-001", "France", "FRA")
        p = make_player("aaaaaaaa-5000-0000-0000-000000000001", "Frenchman", country=z)
        make_player_season(p, s, points=1000, rank=1)
        _, data = call(season=s.id)
        assert data["results"][0]["country"] == {"name": "France", "file_name": "FRA", "alpha3": "FRA"}

    def test_country_is_none_when_player_has_no_country(self):
        s = make_season()
        p = make_player("aaaaaaaa-5000-0000-0000-000000000002", "Stateless")
        make_player_season(p, s, points=1000, rank=1)
        _, data = call(season=s.id)
        assert data["results"][0]["country"] is None

    # ── club_tag field ────────────────────────────────────────────────────────

    def test_club_tag_returned(self):
        s = make_season()
        p = make_player("aaaaaaaa-6000-0000-0000-000000000001", "Tagged", club_tag="CLUB")
        make_player_season(p, s, points=1000, rank=1, club_tag="CLUB")
        _, data = call(season=s.id)
        assert data["results"][0]["club_tag"] == "CLUB"

    def test_club_tag_is_none_when_not_set(self):
        s = make_season()
        p = make_player("aaaaaaaa-6000-0000-0000-000000000002", "NoTag")
        make_player_season(p, s, points=1000, rank=1)
        _, data = call(season=s.id)
        assert data["results"][0]["club_tag"] is None

    # ── points field ──────────────────────────────────────────────────────────

    def test_points_match_player_season_points(self):
        s = make_season()
        p = make_player("aaaaaaaa-7000-0000-0000-000000000001", "Scorer")
        make_player_season(p, s, points=3750, rank=1)
        _, data = call(season=s.id)
        assert data["results"][0]["points"] == 3750


# ── get_active_matches_per_rank ───────────────────────────────────────────────


def _make_map():
    import uuid
    from models import Map

    return Map.create(uid=str(uuid.uuid4())[:20], name="M")


def _make_active_game(map_obj, min_elo, max_elo, trackmaster_limit=999999, is_finished=False):
    from models import Game
    from datetime import datetime

    return Game.create(
        map=map_obj,
        min_elo=min_elo,
        max_elo=max_elo,
        trackmaster_limit=trackmaster_limit,
        is_finished=is_finished,
        time=datetime.now(),
    )


def _call_active():
    return _APIViews.get_active_matches_per_rank()


def _by_key(results, key):
    return next(r for r in results if r["rank"] == key)


class TestGetActiveMatchesPerRank:

    def test_returns_200(self):
        status, _ = _call_active()
        assert status == 200

    def test_response_contains_all_13_ranks(self):
        _, data = _call_active()
        keys = {r["rank"] for r in data["results"]}
        expected = {r["key"] for r in _RANKS}
        assert keys == expected

    def test_all_counts_zero_when_no_active_games(self):
        _, data = _call_active()
        assert all(r["count"] == 0 for r in data["results"])

    def test_finished_game_not_counted(self):
        m = _make_map()
        _make_active_game(m, 3000, 3299, is_finished=True)
        _, data = _call_active()
        assert all(r["count"] == 0 for r in data["results"])

    def test_game_without_elo_data_not_counted(self):
        m = _make_map()
        # min_elo and max_elo stay at default -1
        from models import Game
        from datetime import datetime

        Game.create(map=m, is_finished=False, time=datetime.now())
        _, data = _call_active()
        assert all(r["count"] == 0 for r in data["results"])

    def test_game_entirely_in_m1_range_counted_only_in_m1(self):
        m = _make_map()
        _make_active_game(m, min_elo=3000, max_elo=3299)
        _, data = _call_active()
        assert _by_key(data["results"], "m1")["count"] == 1
        assert _by_key(data["results"], "m2")["count"] == 0
        assert _by_key(data["results"], "g3")["count"] == 0

    def test_game_spanning_two_ranks_counted_in_both(self):
        # Silver II (1300-1599) to Silver III (1600-1999)
        m = _make_map()
        _make_active_game(m, min_elo=1300, max_elo=1700)
        _, data = _call_active()
        assert _by_key(data["results"], "s2")["count"] == 1
        assert _by_key(data["results"], "s3")["count"] == 1
        assert _by_key(data["results"], "s1")["count"] == 0
        assert _by_key(data["results"], "g1")["count"] == 0

    def test_tm_game_with_valid_trackmaster_limit_counted_as_tm(self):
        m = _make_map()
        # max_elo=4200, trackmaster_limit=4000 (<=max_elo) → TM
        _make_active_game(m, min_elo=3600, max_elo=4200, trackmaster_limit=4000)
        _, data = _call_active()
        assert _by_key(data["results"], "tm")["count"] == 1

    def test_tm_range_with_invalid_trackmaster_limit_counted_as_m3_not_tm(self):
        m = _make_map()
        # max_elo=4200 but trackmaster_limit=4500 (>max_elo) → not TM, counts as M3
        _make_active_game(m, min_elo=3600, max_elo=4200, trackmaster_limit=4500)
        _, data = _call_active()
        assert _by_key(data["results"], "tm")["count"] == 0
        assert _by_key(data["results"], "m3")["count"] == 1

    def test_m3_game_below_tm_threshold_not_affected_by_tm_logic(self):
        m = _make_map()
        # max_elo=3999 → clearly M3, never reaches TM territory
        _make_active_game(m, min_elo=3600, max_elo=3999)
        _, data = _call_active()
        assert _by_key(data["results"], "m3")["count"] == 1
        assert _by_key(data["results"], "tm")["count"] == 0

    def test_multiple_active_games_counted_independently(self):
        m = _make_map()
        _make_active_game(m, min_elo=1000, max_elo=1299)  # s1
        _make_active_game(m, min_elo=1000, max_elo=1299)  # s1 again
        _, data = _call_active()
        assert _by_key(data["results"], "s1")["count"] == 2

    def test_old_active_game_excluded_by_time_cutoff(self):
        from models import Game

        m = _make_map()
        # Game started 2 hours ago — older than 30-minute cutoff, must not appear
        Game.create(
            map=m,
            min_elo=1000,
            max_elo=1299,
            is_finished=False,
            time=__import__("datetime").datetime.now() - __import__("datetime").timedelta(hours=2),
        )
        _, data = _call_active()
        assert all(r["count"] == 0 for r in data["results"])
