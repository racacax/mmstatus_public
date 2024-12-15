# auto-generated snapshot
import json
import os
import sys
import time

from iso3166 import countries

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from peewee import *
from datetime import datetime
import peewee

from playhouse.migrate import MySQLMigrator

from models import Player, PlayerGame, Game, Map, Zone, Season, PlayerSeason
from settings import db

migrator = MySQLMigrator(db)


def migrate():
    season = Season.get_current_season()
    for player in (
        Player.select().join(Game).where(Game.time >= datetime(2024, 4, 2, 17, 0))
    ):
        print(player)
        PlayerSeason.get_or_create(player=player, season=season)


if __name__ == "__main__":
    migrate()
