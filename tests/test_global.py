import pytest

from models import Season


def test_init():
    assert True


@pytest.mark.order(1)
def test_sql_insertion():
    assert Season.select(Season.id).count() == 0
    Season.create(start_time=0, end_time=0, name="Test season")
    assert Season.select(Season.id).count() == 1


@pytest.mark.order(2)
def test_sql_doesnt_keep_data():
    assert Season.select(Season.id).count() == 0
