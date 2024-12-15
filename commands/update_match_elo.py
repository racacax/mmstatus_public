import time
from datetime import datetime

from models import Game, Player, PlayerGame


def update_match_elo():
    while True:
        try:
            games = (
                Game.select(Game)
                .join(PlayerGame)
                .join(Player)
                .where(
                    Game.average_elo == -1,
                    Player.last_points_update != datetime.fromtimestamp(0),
                )
                .group_by(Game)
            )
            for game in games:
                print("updating elo for game ", game)
                try:
                    min_elo = 999999999
                    max_elo = 0
                    sum_elo = 0
                    for p in game.player_games:
                        player = p.player
                        if min_elo > player.points:
                            min_elo = player.points
                        if max_elo < player.points:
                            max_elo = player.points
                        sum_elo += player.points
                    game.min_elo = min_elo
                    game.max_elo = max_elo
                    game.average_elo = sum_elo / len(game.player_games)
                    game.save()
                except Exception as e:
                    print("update_match_elo error", game, e)
        except Exception as e2:
            print("update_match_elo error", e2)
        time.sleep(5)
