# auto-generated snapshot
import os
import sys
import time

from src.services import NadeoLive

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from peewee import *
from datetime import datetime
import peewee

from playhouse.migrate import MySQLMigrator

from models import Player, PlayerGame, Game, Map
from settings import db

migrator = MySQLMigrator(db)


def migrate():
    index = (
        Game.select(Game.id)
        .where(Game.time < datetime(2024, 5, 1, 12, 45))
        .order_by(Game.id.desc())[0]
        .id
    )
    while True:
        index -= 1
        try:
            print(index)
            g = Game.filter(id=index).get_or_none()
            if g:
                print("skip, already good")
                continue
            match = NadeoLive.get_match(index)

            if "exception" in match:
                print("error")
                continue
            name: str = match["name"]
            if "Official 3v3" not in name:
                print("skip, royal match")
                time.sleep(1)
                continue
            map, _ = Map.get_or_create(uid=match["publicConfig"]["maps"][0])
            participants = NadeoLive.get_match_participants(index)
            teams = NadeoLive.get_match_teams(index)
            teams = {p["position"]: p for p in teams}
            if not g:
                g = Game.create(
                    id=match["id"],
                    time=datetime.fromtimestamp(match["startDate"]),
                    is_finished=match["status"] == "COMPLETED",
                    map=map,
                    trackmaster_limit=9999999,
                )
            print(g.time)
            for p in participants:
                player, created = Player.get_or_create(uuid=p["participant"])
                pg, created = PlayerGame.get_or_create(game=g, player=player)
                pg.is_mvp = p["mvp"]
                pg.is_win = teams.get(p["teamPosition"], {"rank": 1})["rank"] == 1
                print(pg.is_win, pg.is_mvp)
                pg.save()
            time.sleep(2)
            if g.time < datetime(2024, 4, 10, 17, 0):
                exit(0)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    migrate()
