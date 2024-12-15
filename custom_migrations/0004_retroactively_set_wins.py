# auto-generated snapshot
import os
import sys
import time

from src.services import NadeoLive

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from peewee import *
import datetime
import peewee

from playhouse.migrate import MySQLMigrator

from models import Player, PlayerGame, Game
from settings import db

migrator = MySQLMigrator(db)


def migrate():
    for g in Game.select(Game).where(Game.is_finished == True):
        try:
            print(g.id)
            participants = NadeoLive.get_match_participants(g.id)
            participants = {p["participant"]: p for p in participants}

            for p in g.player_games:
                player = p.player
                p.is_mvp = participants.get(str(player.uuid), {"mvp": None})["mvp"]
                p.is_win = (
                    participants.get(str(player.uuid), {"teamPosition": None})[
                        "teamPosition"
                    ]
                    == 0
                )
                p.save()
        except Exception as e:
            print(e)
        time.sleep(1)
    print("finished")


if __name__ == "__main__":
    migrate()
