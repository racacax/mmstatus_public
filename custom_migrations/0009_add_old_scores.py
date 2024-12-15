# auto-generated snapshot
import os
import sys
import time
from glob import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from peewee import *
from datetime import datetime
import peewee

from playhouse.migrate import MySQLMigrator

from models import Player, PlayerGame, Game, Map
from settings import db

migrator = MySQLMigrator(db)


def migrate():
    files = glob("resources/match_data/*")
    for file in files:
        match_id = file.split("/")[-1].split(".txt")[0]
        game = Game.get_or_none(id=int(match_id))
        if not game:
            print("skipping ", match_id)
            continue
        print(match_id)
        f = open(file, "r")
        content = f.read().split("\n")
        rounds = int(content[-2])
        data = {}
        for i in range(1, len(content) - 2):
            sp = content[i].split(";")
            data[sp[0]] = {"score": int(sp[1]), "position": int(sp[2])}
        game.rounds = rounds
        game.save()
        for p in game.player_games:
            p.points = data.get(str(p.player_id), {"score": None})["score"]
            p.position = data.get(str(p.player_id), {"position": None})["position"]
            p.save()
        f.close()


if __name__ == "__main__":
    migrate()
