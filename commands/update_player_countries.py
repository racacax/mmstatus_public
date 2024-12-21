import time
import traceback

from models import Player
from src.log_utils import create_logger

logger = create_logger("update_player_countries")


def update_player_countries():
    while True:
        try:
            logger.info("Fetching first 50 players without country info")
            players = (
                Player.select(Player)
                .where(Player.zone != None, Player.country == None)
                .paginate(1, 50)
            )
            logger.info(f"Found {len(players)} players")
            try:
                if len(players) == 0:
                    logger.info("Waiting 10s before updating data...")
                    time.sleep(10)
                    continue
                ids = [str(p.uuid) for p in players]
                logger.info("Updating countries for players", {"ids": ids})
                for p in players:
                    final_zone = p.zone
                    while final_zone.country_alpha3 is None and final_zone.parent:
                        final_zone = final_zone.parent
                    p.country = final_zone
                    p.save()
            except Exception as e:
                logger.error(
                    f"Error while updating players info",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
        except Exception as e2:
            logger.error(
                f"General error in the thread",
                extra={"exception": e2, "traceback": traceback.format_exc()},
            )
        logger.info("Waiting 10s before updating data...")
        time.sleep(10)
