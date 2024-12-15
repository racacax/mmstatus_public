import time
from datetime import datetime, timedelta

from models import Player, PlayerGame, Season, PlayerSeason
from src.services import NadeoLive


def update_player_ranks():
    while True:
        try:
            season = Season.get_current_season()
            players = (
                Player.select(Player)
                .where(
                    (Player.last_points_update < datetime.now() - timedelta(hours=12))
                    & (
                        (Player.points != 0)
                        | (Player.last_points_update == datetime.fromtimestamp(0))
                    )
                )
                .order_by(Player.last_points_update.asc())
                .paginate(1, 100)
            )
            count = len(players)
            if count == 0:
                continue
            try:
                ids = [str(p.uuid) for p in players]
                print("Updating player ranks", ids)
                ranks = NadeoLive.get_player_ranks(ids)
                scores = {p["player"]: p["score"] for p in ranks["results"]}
                ranks = {p["player"]: p["rank"] for p in ranks["results"]}
                for p in players:
                    try:
                        p.points = scores.get(str(p.uuid), 0)
                        p.rank = ranks.get(str(p.uuid), 0)
                        p.last_points_update = datetime.now()
                        p.save()

                        ps, _ = PlayerSeason.get_or_create(player=p, season=season)
                        ps.points = p.points
                        ps.rank = p.rank
                        ps.save()
                        if p.last_match and p.last_match.is_finished:
                            pg = PlayerGame.get(game=p.last_match, player=p)
                            if pg.points_after_match is None:
                                pg.points_after_match = p.points
                                pg.rank_after_match = p.rank
                                pg.save()
                    except Exception as e:
                        print("update player ranks error", p, e)
            except Exception as e:
                print("update player ranks error", players, e)
        except Exception as e2:
            print("update player ranks error", e2)
        time.sleep(10)
