# auto-generated snapshot
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from peewee import *
import datetime
import peewee

from playhouse.migrate import MySQLMigrator

from models import Player, PlayerGame, Game
from settings import db

migrator = MySQLMigrator(db)


def migrate():
    while True:
        count = 0
        print("try")
        with db.atomic():
            for p in Player.select(Player).where(Player.last_match == None):
                a = p.player_games.select(PlayerGame, Game).join(Game).order_by(Game.id.desc())
                if len(a) > 0:
                    count += 1
                    p.last_match = p.player_games.select(PlayerGame, Game).join(Game).order_by(Game.id.desc())[0].game
                    p.save()
        if count == 0:
            break


if __name__ == '__main__':
    migrate()
