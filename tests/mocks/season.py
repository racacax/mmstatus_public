from datetime import datetime, timedelta

import pytest

import settings
from models import Season
from src.services import NadeoLive
from src.threads.get_matches import GetMatchesThread
from tests.mock_payloads.match import get_match_participants, get_match


@pytest.fixture
def create_current_season():
    a_week_ago = datetime.now() - timedelta(weeks=1)
    in_one_week = datetime.now() + timedelta(weeks=1)
    return Season.create(name="Current Season", start_time=a_week_ago, end_time=in_one_week)


@pytest.fixture
def mock_get_match(monkeypatch):
    monkeypatch.setattr(NadeoLive, "get_match_participants", lambda _: get_match_participants())
    monkeypatch.setattr(NadeoLive, "get_match", lambda id: get_match(match_id=id))


@pytest.fixture
def insert_matches(monkeypatch, mock_get_match):
    monkeypatch.setattr(settings, "START_ID", 1)
    thread = GetMatchesThread()
    for i in range(5):
        thread.insert_match()
        thread.match_id += 1
