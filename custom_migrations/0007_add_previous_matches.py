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
    indexes = [
        8993975,
        8991879,
        8990108,
        8988345,
        8988237,
        8988124,
        8988008,
        8987901,
        8987796,
        8987687,
        8987579,
        8987474,
        8987366,
        8987258,
        8987147,
        8987036,
        8986927,
        8986810,
        8986712,
        8986601,
        8986491,
        8986382,
        8986278,
        8986173,
        8986067,
        8985961,
        8985857,
        8985751,
        8985637,
        8985517,
        8985409,
        8983992,
        8965175,
        8965067,
        8964958,
        8964848,
        8964739,
        8964635,
        8964530,
        8964423,
        8964312,
        8964201,
        8964092,
        8963981,
        8963866,
        8963752,
        8963642,
        8963534,
        8963415,
        8963296,
        8963181,
        8963070,
        8962957,
        8962848,
        8962730,
        8962620,
        8962497,
        8962384,
        8962273,
        8962155,
        8962041,
        8961922,
        8961809,
        8961694,
        8961578,
        8961470,
        8961361,
        8961247,
        8961141,
        8961035,
        8960920,
        8960699,
        8960589,
        8960482,
        8960374,
        8960268,
        8960156,
        8960046,
        8959941,
        8959832,
        8959729,
        8959621,
        8959514,
        8959405,
        8959301,
        8959195,
        8959086,
        8958975,
        8958868,
        8958761,
        8958650,
        8958542,
        8958433,
        8958325,
        8958218,
        8958106,
        8957999,
        8957892,
        8957787,
        8957676,
        8957565,
        8957461,
        8957351,
        8957246,
        8957136,
        8957022,
        8956910,
        8956802,
        8956693,
        8956584,
        8956473,
        8956367,
        8956261,
        8956151,
        8956043,
        8955937,
        8955826,
        8955718,
        8955607,
        8955496,
        8955381,
        8955267,
        8955152,
        8955043,
        8954922,
        8954803,
        8954685,
        8954565,
        8954448,
        8954320,
        8954205,
        8954086,
        8953976,
        8953861,
        8953754,
        8953537,
        8953433,
        8953324,
        8953214,
        8953099,
        8952984,
        8952877,
        8952770,
        8952657,
        8952545,
        8952436,
        8952329,
        8952220,
        8952108,
        8951999,
        8951887,
        8951778,
        8951666,
        8951566,
        8951464,
        8951358,
        8951249,
        8951143,
        8951034,
        8950925,
        8950823,
        8950719,
        8950617,
        8950509,
        8950407,
        8950305,
        8950196,
        8950091,
        8949978,
        8949875,
        8949771,
        8949669,
        8949559,
        8949453,
        8949344,
        8949240,
        8949132,
        8948988,
        8948882,
        8948778,
        8948673,
        8948563,
        8948454,
        8948348,
        8948246,
        8948145,
        8948040,
        8947936,
        8947825,
        8947718,
        8947608,
        8947499,
        8947386,
        8947276,
        8947166,
        8947052,
        8946935,
        8946818,
        8946708,
        8946596,
        8946489,
        8946374,
        8946256,
        8946138,
        8946023,
        8945903,
        8945784,
        8945658,
        8945552,
        8945443,
        8945339,
        8945230,
        8945123,
        8945016,
        8944907,
        8944803,
        8944700,
        8944595,
        8944488,
        8944386,
        8944280,
        8944179,
        8944076,
        8943971,
        8943867,
        8943761,
        8943656,
        8943549,
        8943440,
        8943334,
        8943223,
        8943112,
        8943008,
        8942901,
        8942793,
        8942672,
        8942568,
        8942452,
        8942339,
        8942232,
        8942122,
        8942018,
        8941916,
        8941815,
        8941663,
        8941558,
        8941455,
        8941351,
        8941246,
        8941142,
        8941038,
        8940932,
        8940825,
        8940718,
        8940664,
    ]

    for index in indexes:
        while True:
            try:
                print(index)
                g = Game.filter(id=index).get_or_none()
                match = NadeoLive.get_match(index)

                if "exception" in match:
                    print("error")
                    continue
                name: str = match["name"]
                if "Official 3v3" not in name:
                    print("skip, royal match")
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
                time.sleep(0.3)
                if g.time < datetime(2024, 4, 1, 17, 0):
                    exit(0)
                break
            except Exception as e:
                print(e)


if __name__ == "__main__":
    migrate()
