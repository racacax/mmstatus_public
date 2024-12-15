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
    for p in Player.filter():
        current_points = p.points
        pgs = (
            PlayerGame.select()
            .join(Game)
            .switch(PlayerGame)
            .join(Player)
            .where(
                PlayerGame.id < 1028297,
                PlayerGame.points_after_match == None,
                Player.uuid == p.uuid,
                Game.is_finished == True,
            )
            .order_by(Game.id.desc())
        )
        if len(pgs) == 0:
            continue
        min_points = pgs[-1].game.average_elo
        points = []
        print(p.uuid)
        try:
            for pg in pgs:
                if pg.is_win:
                    if pg.position in [1, 2]:
                        if pg.is_mvp:
                            current_points -= 60
                        else:
                            current_points -= 40
                    elif pg.position in [3, 4]:
                        current_points -= 30
                    elif pg.position in [5, 6]:
                        current_points -= 20
                    else:
                        current_points -= 30
                else:
                    if pg.is_mvp and pg.game.average_elo > 3000:
                        pass
                    elif pg.is_mvp:
                        current_points -= 20
                    elif pg.position in [2, 3]:
                        current_points += 20
                    elif pg.position in [4, 5]:
                        current_points += 30
                    elif pg.position == 6:
                        current_points += 40
                    else:
                        current_points += 30
                if current_points < 0:
                    current_points = 0
                points.append(current_points)

            print(points)
            index = 0
            with db.atomic():
                for pg in pgs:
                    pg.points_after_match = points[index]
                    index += 1
                    pg.save()
        except Exception as e:
            print(p, e)


if __name__ == "__main__":
    migrate()
