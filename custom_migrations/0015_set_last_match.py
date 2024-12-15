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
    players = Player.select().where(Player.last_match == None)
    for p in players:
        pgs = (
            PlayerGame.select()
            .join(Game)
            .where(PlayerGame.player_id == p.uuid)
            .order_by(Game.id.desc())
        )
        if len(pgs) > 0:
            print(p.uuid)
            p.last_match = pgs[0].game
            p.save()
        else:
            print("skip", p.uuid)


if __name__ == "__main__":
    migrate()
