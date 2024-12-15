import time
from datetime import datetime

import settings
from models import PlayerGame, Game, Map, Player
from src.services import NadeoLive


def get_matches():
    last_game = Game.select().order_by(Game.id.desc()).paginate(1, 1).get_or_none()
    print("get_matches last game", last_game)
    id = (last_game and last_game.id or settings.START_ID) + 1
    id = max(id, settings.START_ID)

    while True:
        while True:
            try:
                with settings.db.atomic():
                    print("Adding " + str(id))
                    match = NadeoLive.get_match(id)
                    if "exception" in match:
                        print("error")
                        break
                    name: str = match["name"]
                    if "Official 3v3" in name:
                        participants = NadeoLive.get_match_participants(id)
                        players_o = []
                        map, _ = Map.get_or_create(uid=match["publicConfig"]["maps"][0])
                        f_trackmaster = (
                            Player.select(Player.points)
                            .where(Player.rank <= 10, Player.points >= 4000)
                            .order_by(Player.rank.desc())
                        )

                        if len(f_trackmaster) > 0:
                            tm_limit = f_trackmaster[0].points
                        else:
                            tm_limit = 99999
                        g = Game.create(
                            id=match["id"],
                            time=datetime.fromtimestamp(match["startDate"]),
                            is_finished=match["status"] == "COMPLETED",
                            map=map,
                            trackmaster_limit=tm_limit,
                        )
                        for p in participants:
                            player, created = Player.get_or_create(
                                uuid=p["participant"]
                            )
                            player.last_match = g
                            players_o.append(player)
                            if created:
                                player.last_points_update = datetime.fromtimestamp(0)
                            player.save()

                        for p in players_o:
                            PlayerGame.create(game=g, player=p)
            except Exception as e:
                print("get_matches error", id, e)
                if "lock" in str(e).lower():
                    print("we'll restart if it's only a deadlock")
                    continue
            id += 1
        time.sleep(30)
