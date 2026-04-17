from datetime import datetime, timedelta
from unittest.mock import patch
from uuid import UUID

from models import Player, PlayerSeason, Season
from src.threads.update_players import UpdatePlayersThread

NOW = datetime.now().replace(microsecond=0)


def make_season(active=True):
    if active:
        return Season.create(
            name="UP_S",
            start_time=NOW - timedelta(days=30),
            end_time=NOW + timedelta(days=30),
        )
    return Season.create(
        name="UP_S_OLD",
        start_time=NOW - timedelta(days=90),
        end_time=NOW - timedelta(days=1),
    )


def make_player(suffix, club_tag=None):
    return Player.create(
        uuid=UUID(f"aaaaaaaa-0000-0000-0000-{suffix.zfill(12)}"),
        name="Player",
        club_tag=club_tag,
        last_name_update=datetime.fromtimestamp(0),
    )


def fake_api(players, names_map, club_tags_map):
    """Patch both API calls and call update_players."""
    ids = [str(p.uuid) for p in players]
    names = {i: names_map.get(i, "Name") for i in ids}
    tags = [{"accountId": i, "clubTag": t} for i, t in club_tags_map.items() if i in ids]
    with patch("src.threads.update_players.NadeoOauth.get_player_display_names", return_value=names):
        with patch("src.threads.update_players.NadeoCore.get_player_club_tags", return_value=tags):
            UpdatePlayersThread().update_players(players)


# ── PlayerSeason.club_tag sync ────────────────────────────────────────────────


class TestUpdatePlayersClubTagSync:

    def test_player_season_club_tag_updated_for_current_season(self):
        season = make_season(active=True)
        p = make_player("0001")
        ps = PlayerSeason.create(player=p, season=season, rank=1, points=1000, club_tag=None)

        fake_api([p], {}, {str(p.uuid): "NEW_TAG"})

        ps_updated = PlayerSeason.get_by_id(ps.id)
        assert ps_updated.club_tag == "NEW_TAG"

    def test_player_season_club_tag_set_to_none_when_player_has_no_tag(self):
        season = make_season(active=True)
        p = make_player("0002", club_tag="OLD")
        ps = PlayerSeason.create(player=p, season=season, rank=1, points=1000, club_tag="OLD")

        # API returns no club tag for this player
        fake_api([p], {}, {})

        ps_updated = PlayerSeason.get_by_id(ps.id)
        assert ps_updated.club_tag is None

    def test_no_player_season_update_when_no_current_season(self):
        make_season(active=False)
        p = make_player("0003")
        # No active season → update_players must not crash
        fake_api([p], {}, {str(p.uuid): "TAG"})
        # No PlayerSeason exists — just verifying no exception raised

    def test_only_current_season_player_season_updated(self):
        past_season = make_season(active=False)
        current_season = make_season(active=True)
        p = make_player("0004")
        ps_past = PlayerSeason.create(player=p, season=past_season, rank=1, points=500, club_tag="PAST")
        ps_current = PlayerSeason.create(player=p, season=current_season, rank=1, points=1000, club_tag=None)

        fake_api([p], {}, {str(p.uuid): "NEW_TAG"})

        assert PlayerSeason.get_by_id(ps_past.id).club_tag == "PAST"
        assert PlayerSeason.get_by_id(ps_current.id).club_tag == "NEW_TAG"

    def test_player_club_tag_updated_on_player_model(self):
        make_season(active=True)
        p = make_player("0005")

        fake_api([p], {}, {str(p.uuid): "MY_TAG"})

        assert Player.get_by_id(p.uuid).club_tag == "MY_TAG"


# ── name fallback when absent from API ───────────────────────────────────────


def fake_api_absent_name(players, club_tags_map):
    """Simulate API returning no name entry for any player."""
    tags = [{"accountId": str(p.uuid), "clubTag": t} for p, t in club_tags_map.items()]
    with patch("src.threads.update_players.NadeoOauth.get_player_display_names", return_value={}):
        with patch("src.threads.update_players.NadeoCore.get_player_club_tags", return_value=tags):
            UpdatePlayersThread().update_players(players)


class TestUpdatePlayersNameFallback:

    def test_keeps_existing_name_when_absent_from_api(self):
        p = make_player("0010")
        Player.update(name="ExistingName").where(Player.uuid == p.uuid).execute()
        p = Player.get_by_id(p.uuid)

        fake_api_absent_name([p], {})

        assert Player.get_by_id(p.uuid).name == "ExistingName"

    def test_sets_name_unknown_when_no_existing_name_and_absent_from_api(self):
        p = make_player("0011")
        Player.update(name="").where(Player.uuid == p.uuid).execute()
        p = Player.get_by_id(p.uuid)

        fake_api_absent_name([p], {})

        assert Player.get_by_id(p.uuid).name == "Name unknown"

    def test_uses_api_name_when_present(self):
        p = make_player("0012")
        fake_api([p], {str(p.uuid): "APIName"}, {})

        assert Player.get_by_id(p.uuid).name == "APIName"

    def test_api_name_overrides_existing_name(self):
        p = make_player("0013")
        Player.update(name="OldName").where(Player.uuid == p.uuid).execute()
        fake_api([p], {str(p.uuid): "NewName"}, {})

        assert Player.get_by_id(p.uuid).name == "NewName"
