import time
import traceback

from models import Player
from src.log_utils import create_logger
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_player_countries")


class UpdatePlayerCountriesThread(AbstractThread):
    @staticmethod
    def run_iteration():
        logger.info("Fetching first 50 players without country info")
        players = Player.select(Player).where(Player.zone != None, Player.country == None).paginate(1, 50)
        logger.info(f"Found {len(players)} players")
        try:
            if len(players) == 0:
                return
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
                "Error while updating players info",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )

    def handle(self):
        while True:
            try:
                self.run_iteration()
            except Exception as e:
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 10s before updating data...")
            time.sleep(10)
