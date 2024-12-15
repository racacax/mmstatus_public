import time
from datetime import datetime, timedelta

from models import Game
from settings import db
from src.services import NadeoLive


def update_matches():
    while True:
        try:
            games = Game.select(Game).where(
                Game.is_finished == False,
                Game.time < datetime.now() - timedelta(minutes=4),
            )
            # we ignore games newer than 4 minutes ago. There is almost no chance they are finished
            print(str(len(games)) + " non terminated games")
            for game in games:
                while True:
                    try:
                        with db.atomic():
                            match = NadeoLive.get_match(game.id)
                            if match["status"] == "COMPLETED":
                                teams = NadeoLive.get_match_teams(game.id)
                                print("Terminating " + game.__str__())
                                game.is_finished = True
                                if len(teams) > 0:
                                    game.rounds = teams[0]["score"]
                                if len(teams) > 1:
                                    game.rounds += teams[1]["score"]
                                game.save()

                                print("notice we need to update player ranks")
                                participants = NadeoLive.get_match_participants(game.id)
                                participants = {
                                    p["participant"]: p for p in participants
                                }

                                teams = {p["position"]: p for p in teams}
                                for p in game.player_games:
                                    player = p.player
                                    p.is_mvp = participants.get(
                                        str(player.uuid), {"mvp": None}
                                    )["mvp"]
                                    p.is_win = (
                                        teams.get(
                                            participants.get(
                                                str(player.uuid), {"teamPosition": 0}
                                            )["teamPosition"],
                                            {"rank": 1},
                                        )["rank"]
                                        == 1
                                    )
                                    p.position = participants.get(
                                        str(player.uuid), {"position": None}
                                    )["position"]
                                    if p.position is not None:
                                        p.position += 1
                                    p.points = participants.get(
                                        str(player.uuid), {"score": None}
                                    )["score"]
                                    p.save()
                                    player.last_points_update = datetime.fromtimestamp(
                                        0
                                    )
                                    player.save()
                        break
                    except Exception as e:
                        print("update_matches error", game, e)
                        if "lock" in str(e).lower():
                            print("we'll restart if it's only a deadlock (update)")
                            continue
                        break
        except Exception as e2:
            print("update_matches error", e2)
        time.sleep(30)
