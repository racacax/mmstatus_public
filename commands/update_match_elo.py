import time
import traceback
from datetime import datetime

from models import Game, Player, PlayerGame
from src.log_utils import create_logger

logger = create_logger("update_match_elo")


def update_match_elo():
    while True:
        try:
            logger.info("Fetching matches with uncomputed average elo")
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
            logger.info(f"Found {len(games)} with uncomputed elo")
            for game in games:
                logger.info(
                    f"Updating elo for match id {game.id}", extra={"game": game}
                )
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
                    logger.error(
                        f"Error while updating match with id {game.id}",
                        extra={"exception": e, "traceback": traceback.format_exc()},
                    )
        except Exception as e2:
            logger.error(
                f"General error in the thread",
                extra={"exception": e2, "traceback": traceback.format_exc()},
            )
        logger.info("Waiting 5s before starting the thread again...")
        time.sleep(5)
