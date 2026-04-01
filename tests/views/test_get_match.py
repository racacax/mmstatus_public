from datetime import datetime, timedelta
from uuid import UUID

from models import Game, Map, Player, PlayerGame
from src.views import APIViews

# ── helpers ───────────────────────────────────────────────────────────────────

MAP_UID = "TEST_MAP_UID_001"
MAP_UID_2 = "TEST_MAP_UID_002"


def make_map(uid=MAP_UID, name="Test Map"):
    return Map.create(uid=uid, name=name)


def make_game(map_obj, is_finished=True, time=None, min_elo=-1, max_elo=-1, average_elo=-1, rounds=None):
    return Game.create(
        map=map_obj,
        is_finished=is_finished,
        time=time or datetime.now().replace(microsecond=0),
        min_elo=min_elo,
        max_elo=max_elo,
        average_elo=average_elo,
        rounds=rounds,
    )


def make_player(uid, name="Player"):
    return Player.create(uuid=UUID(uid), name=name)


def make_player_game(
    player, game, is_win=False, is_mvp=False, score=None, position=None, points_after_match=None, rank_after_match=None
):
    return PlayerGame.create(
        player=player,
        game=game,
        is_win=is_win,
        is_mvp=is_mvp,
        points=score,
        position=position,
        points_after_match=points_after_match,
        rank_after_match=rank_after_match,
    )


def call(match_id):
    return APIViews.get_match(match_id=match_id)


# ── get_match ─────────────────────────────────────────────────────────────────


class TestGetMatch:

    # ── 404 ───────────────────────────────────────────────────────────────────

    def test_returns_404_for_unknown_id(self):
        status, data = call(match_id=999999)
        assert status == 404

    def test_404_body_has_message_key(self):
        _, data = call(match_id=999999)
        assert "message" in data

    # ── shape / contract ──────────────────────────────────────────────────────

    def test_returns_200_for_existing_match(self):
        m = make_map()
        g = make_game(m)
        status, _ = call(g.id)
        assert status == 200

    def test_top_level_keys_present(self):
        m = make_map()
        g = make_game(m)
        _, data = call(g.id)
        assert set(data.keys()) == {
            "id",
            "time",
            "is_finished",
            "map",
            "min_elo",
            "max_elo",
            "average_elo",
            "trackmaster_points_limit",
            "rounds",
            "players",
        }

    def test_map_keys_present(self):
        m = make_map()
        g = make_game(m)
        _, data = call(g.id)
        assert set(data["map"].keys()) == {"uid", "name"}

    def test_player_row_keys_present(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0000-0000-0000-000000000001")
        make_player_game(p, g)
        _, data = call(g.id)
        assert set(data["players"][0].keys()) == {
            "uuid",
            "name",
            "position",
            "is_win",
            "is_mvp",
            "score",
            "elo_before",
            "elo_after",
            "elo_gained",
            "rank_after",
        }

    # ── match fields ──────────────────────────────────────────────────────────

    def test_id_matches_requested_game(self):
        m = make_map()
        g = make_game(m)
        _, data = call(g.id)
        assert data["id"] == g.id

    def test_is_finished_true(self):
        m = make_map()
        g = make_game(m, is_finished=True)
        _, data = call(g.id)
        assert data["is_finished"] is True

    def test_is_finished_false(self):
        m = make_map()
        g = make_game(m, is_finished=False)
        _, data = call(g.id)
        assert data["is_finished"] is False

    def test_time_is_unix_timestamp(self):
        m = make_map()
        t = datetime(2024, 6, 15, 12, 0, 0)
        g = make_game(m, time=t)
        _, data = call(g.id)
        assert data["time"] == t.timestamp()

    def test_elo_fields_returned(self):
        m = make_map()
        g = make_game(m, min_elo=3000, max_elo=5000, average_elo=4000)
        _, data = call(g.id)
        assert data["min_elo"] == 3000
        assert data["max_elo"] == 5000
        assert data["average_elo"] == 4000

    def test_rounds_returned(self):
        m = make_map()
        g = make_game(m, rounds=5)
        _, data = call(g.id)
        assert data["rounds"] == 5

    def test_rounds_none_when_not_set(self):
        m = make_map()
        g = make_game(m)
        _, data = call(g.id)
        assert data["rounds"] is None

    # ── map fields ────────────────────────────────────────────────────────────

    def test_map_uid_returned(self):
        m = make_map(uid=MAP_UID, name="My Map")
        g = make_game(m)
        _, data = call(g.id)
        assert data["map"]["uid"] == MAP_UID

    def test_map_name_returned(self):
        m = make_map(uid=MAP_UID, name="My Map")
        g = make_game(m)
        _, data = call(g.id)
        assert data["map"]["name"] == "My Map"

    # ── players list ──────────────────────────────────────────────────────────

    def test_empty_players_list_when_no_player_games(self):
        m = make_map()
        g = make_game(m)
        _, data = call(g.id)
        assert data["players"] == []

    def test_returns_all_players_in_match(self):
        m = make_map()
        g = make_game(m)
        p1 = make_player("aaaaaaaa-0001-0000-0000-000000000001")
        p2 = make_player("aaaaaaaa-0001-0000-0000-000000000002")
        p3 = make_player("aaaaaaaa-0001-0000-0000-000000000003")
        make_player_game(p1, g)
        make_player_game(p2, g)
        make_player_game(p3, g)
        _, data = call(g.id)
        assert len(data["players"]) == 3

    def test_does_not_include_players_from_other_matches(self):
        m = make_map()
        g1 = make_game(m)
        g2 = make_game(m)
        p1 = make_player("aaaaaaaa-0002-0000-0000-000000000001")
        p2 = make_player("aaaaaaaa-0002-0000-0000-000000000002")
        make_player_game(p1, g1)
        make_player_game(p2, g2)
        _, data = call(g1.id)
        assert len(data["players"]) == 1

    # ── player fields ─────────────────────────────────────────────────────────

    def test_player_uuid_returned(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0003-0000-0000-000000000001", "Alice")
        make_player_game(p, g)
        _, data = call(g.id)
        assert str(data["players"][0]["uuid"]) == "aaaaaaaa-0003-0000-0000-000000000001"

    def test_player_name_returned(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0003-0000-0000-000000000002", "Alice")
        make_player_game(p, g)
        _, data = call(g.id)
        assert data["players"][0]["name"] == "Alice"

    def test_is_win_true(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0004-0000-0000-000000000001")
        make_player_game(p, g, is_win=True)
        _, data = call(g.id)
        assert data["players"][0]["is_win"] is True

    def test_is_win_false(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0004-0000-0000-000000000002")
        make_player_game(p, g, is_win=False)
        _, data = call(g.id)
        assert data["players"][0]["is_win"] is False

    def test_is_mvp_true(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0005-0000-0000-000000000001")
        make_player_game(p, g, is_mvp=True)
        _, data = call(g.id)
        assert data["players"][0]["is_mvp"] is True

    def test_score_returned(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0006-0000-0000-000000000001")
        make_player_game(p, g, score=42)
        _, data = call(g.id)
        assert data["players"][0]["score"] == 42

    def test_score_none_when_not_set(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0006-0000-0000-000000000002")
        make_player_game(p, g, score=None)
        _, data = call(g.id)
        assert data["players"][0]["score"] is None

    def test_position_returned(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0007-0000-0000-000000000001")
        make_player_game(p, g, position=2)
        _, data = call(g.id)
        assert data["players"][0]["position"] == 2

    def test_elo_after_returned(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0008-0000-0000-000000000001")
        make_player_game(p, g, points_after_match=4500)
        _, data = call(g.id)
        assert data["players"][0]["elo_after"] == 4500

    def test_rank_after_returned(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0009-0000-0000-000000000001")
        make_player_game(p, g, rank_after_match=37)
        _, data = call(g.id)
        assert data["players"][0]["rank_after"] == 37

    # ── elo computation ───────────────────────────────────────────────────────

    def test_elo_gained_computed_from_previous_match(self):
        m = make_map()
        t = datetime.now().replace(microsecond=0)
        g1 = make_game(m, time=t - timedelta(hours=1))
        g2 = make_game(m, time=t)
        p = make_player("aaaaaaaa-0010-0000-0000-000000000001")
        make_player_game(p, g1, points_after_match=4400)
        make_player_game(p, g2, points_after_match=4420)
        _, data = call(g2.id)
        player = data["players"][0]
        assert player["elo_before"] == 4400
        assert player["elo_after"] == 4420
        assert player["elo_gained"] == 20

    def test_elo_gained_negative_on_loss(self):
        m = make_map()
        t = datetime.now().replace(microsecond=0)
        g1 = make_game(m, time=t - timedelta(hours=1))
        g2 = make_game(m, time=t)
        p = make_player("aaaaaaaa-0010-0000-0000-000000000002")
        make_player_game(p, g1, points_after_match=5000)
        make_player_game(p, g2, points_after_match=4980)
        _, data = call(g2.id)
        player = data["players"][0]
        assert player["elo_gained"] == -20

    def test_elo_before_is_none_for_first_ever_match(self):
        m = make_map()
        g = make_game(m)
        p = make_player("aaaaaaaa-0011-0000-0000-000000000001")
        make_player_game(p, g, points_after_match=3000)
        _, data = call(g.id)
        player = data["players"][0]
        assert player["elo_before"] is None
        assert player["elo_gained"] is None

    def test_elo_gained_none_when_elo_after_not_set(self):
        m = make_map()
        t = datetime.now().replace(microsecond=0)
        g1 = make_game(m, time=t - timedelta(hours=1))
        g2 = make_game(m, time=t)
        p = make_player("aaaaaaaa-0011-0000-0000-000000000002")
        make_player_game(p, g1, points_after_match=4000)
        make_player_game(p, g2, points_after_match=None)
        _, data = call(g2.id)
        player = data["players"][0]
        assert player["elo_gained"] is None

    def test_previous_match_with_null_elo_is_skipped(self):
        """A game with points_after_match=None must not be used as elo_before source."""
        m = make_map()
        t = datetime.now().replace(microsecond=0)
        g1 = make_game(m, time=t - timedelta(hours=2))
        g2 = make_game(m, time=t - timedelta(hours=1))
        g3 = make_game(m, time=t)
        p = make_player("aaaaaaaa-0012-0000-0000-000000000001")
        make_player_game(p, g1, points_after_match=4200)
        make_player_game(p, g2, points_after_match=None)  # must be skipped
        make_player_game(p, g3, points_after_match=4250)
        _, data = call(g3.id)
        player = data["players"][0]
        assert player["elo_before"] == 4200
        assert player["elo_gained"] == 50

    def test_elo_computed_independently_per_player(self):
        m = make_map()
        t = datetime.now().replace(microsecond=0)
        g_prev = make_game(m, time=t - timedelta(hours=1))
        g_cur = make_game(m, time=t)
        p1 = make_player("aaaaaaaa-0013-0000-0000-000000000001", "Alice")
        p2 = make_player("aaaaaaaa-0013-0000-0000-000000000002", "Bob")
        make_player_game(p1, g_prev, points_after_match=4000)
        make_player_game(p2, g_prev, points_after_match=5000)
        make_player_game(p1, g_cur, points_after_match=4050)
        make_player_game(p2, g_cur, points_after_match=4960)
        _, data = call(g_cur.id)
        by_name = {pl["name"]: pl for pl in data["players"]}
        assert by_name["Alice"]["elo_gained"] == 50
        assert by_name["Bob"]["elo_gained"] == -40

    def test_only_games_strictly_before_current_used_for_elo_before(self):
        """A player's elo_before must not be influenced by games at the same ID."""
        m = make_map()
        t = datetime.now().replace(microsecond=0)
        g1 = make_game(m, time=t - timedelta(hours=1))
        g2 = make_game(m, time=t)
        p = make_player("aaaaaaaa-0014-0000-0000-000000000001")
        make_player_game(p, g1, points_after_match=3800)
        make_player_game(p, g2, points_after_match=3850)
        _, data = call(g2.id)
        assert data["players"][0]["elo_before"] == 3800

    # ── player ordering ───────────────────────────────────────────────────────

    def test_players_sorted_by_position_ascending(self):
        m = make_map()
        g = make_game(m)
        p1 = make_player("aaaaaaaa-0020-0000-0000-000000000001")
        p2 = make_player("aaaaaaaa-0020-0000-0000-000000000002")
        p3 = make_player("aaaaaaaa-0020-0000-0000-000000000003")
        make_player_game(p1, g, position=3)
        make_player_game(p2, g, position=1)
        make_player_game(p3, g, position=2)
        _, data = call(g.id)
        positions = [pl["position"] for pl in data["players"]]
        assert positions == [1, 2, 3]

    def test_players_with_null_position_sorted_last(self):
        m = make_map()
        g = make_game(m)
        p1 = make_player("aaaaaaaa-0021-0000-0000-000000000001")
        p2 = make_player("aaaaaaaa-0021-0000-0000-000000000002")
        make_player_game(p1, g, position=None)
        make_player_game(p2, g, position=1)
        _, data = call(g.id)
        assert data["players"][0]["position"] == 1
        assert data["players"][1]["position"] is None
