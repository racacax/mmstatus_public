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

from models import Player, PlayerGame, Game, Map, Zone, Season
from settings import db

migrator = MySQLMigrator(db)


def migrate():
    Season.get_or_create(
        name="Spring 2024",
        defaults={
            "start_time": datetime.fromtimestamp(1712070000),
            "end_time": datetime.fromtimestamp(1719846000),
        },
    )


if __name__ == "__main__":
    migrate()
