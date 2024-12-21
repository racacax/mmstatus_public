import time
import traceback
from datetime import datetime

from models import Player
from src.log_utils import create_logger

logger = create_logger("update_player_positions")


def update_player_positions():
    # update top 200 leaderboard every minute
    while True:
        try:
            logger.info("Fetching top 200 players by points")
            players = (Player.select().order_by(Player.points.desc())).paginate(1, 200)
            position = 1
            logger.info(
                "Updating positions for players",
                extra={"players": [str(p.uuid) for p in players]},
            )
            now = datetime.now()
            for p in players:
                p.rank = position
                position += 1
                p.save()
            logger.info(f"update_player_positions done in {(datetime.now() - now)}")
        except Exception as e:
            logger.error(
                f"Error while updating player positions",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )
        logger.info("Waiting 60s before recomputing positions again...")
        time.sleep(60)
