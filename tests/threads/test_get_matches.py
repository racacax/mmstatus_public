import settings
from models import Game, PlayerGame, Player, Map
from src.services import NadeoLive
from src.threads.get_matches import GetMatchesThread
from tests.mock_payloads.match import get_match


def test_insert_match_correct_id(monkeypatch, mock_get_match):
    """
    thread has to create a match with the highest start id between env and db last id
    """
    fake_id = 1234
    monkeypatch.setattr(settings, "START_ID", fake_id)
    th1 = GetMatchesThread()
    th1.insert_match()
    assert Game.select(Game.id).get_or_none().id == fake_id + 1
    th2 = GetMatchesThread()
    th2.insert_match()
    assert Game.select(Game.id).order_by(Game.id.desc())[0].id == fake_id + 2


def test_insert_match_all_participants(monkeypatch, mock_get_match):
    """
    all participants needs to be inserted
    """
    th1 = GetMatchesThread()
    th1.insert_match()
    current_match = Game.select(Game).get()
    assert PlayerGame.select().count() == 6
    assert Player.select().count() == 6
    for pg in PlayerGame.select(PlayerGame):
        assert pg.game_id == current_match.id


def test_match_exception_returns_false(monkeypatch):
    monkeypatch.setattr(NadeoLive, "get_match", lambda _: {"exception": "Yep"})
    th1 = GetMatchesThread()
    th1.insert_match()
    assert th1.insert_match() == False
    assert Game.select(Game).filter().count() == 0


def test_match_exception_will_rerun_thread(monkeypatch, mock_get_match):
    call_obj = {"call": 0}

    def get_match_tmp_mock(call_obj):
        """
        throw error, then give correct payload
        """
        call_obj["call"] += 1
        if call_obj["call"] == 1:
            raise Exception("Error")
        else:
            return get_match(match_id=settings.START_ID + 1)

    monkeypatch.setattr(NadeoLive, "get_match", lambda _: get_match_tmp_mock(call_obj))
    th1 = GetMatchesThread()
    th1.insert_match()
    assert call_obj["call"] == 2
    assert Game.select(Game).filter().count() == 1


def test_insertion_map_only_once(monkeypatch, mock_get_match):
    th1 = GetMatchesThread()
    th1.insert_match()
    th1.match_id += 1
    th1.insert_match()
    assert Map.select().filter().count() == 1
