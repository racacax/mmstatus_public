import time
import traceback

from models import Player
from src.log_utils import create_logger
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_player_regions")


class UpdatePlayerRegionsThread(AbstractThread):
    def run_iteration(self):
        logger.info("Fetching first 50 players without region info")
        players = Player.select(Player).where(Player.country != None, Player.region == None).paginate(1, 50)
        logger.info(f"Found {len(players)} players")
        try:
            if len(players) == 0:
                return
            ids = [str(p.uuid) for p in players]
            logger.info("Updating regions for players", {"ids": ids})
            for p in players:
                zone = p.zone
                region = p.country
                while zone is not None and zone.id != p.country_id:
                    if zone.parent_id == p.country_id:
                        region = zone
                        break
                    zone = zone.parent
                p.region = region
                p.save()
        except Exception as e:
            self._record_error()
            logger.error(
                "Error while updating players region info",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )

    def handle(self):
        while True:
            try:
                self.run_iteration()
            except Exception as e:
                self._record_error()
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 10s before updating data...")
            time.sleep(10)
