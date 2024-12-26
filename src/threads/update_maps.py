import time
import traceback

from models import Map
from src.log_utils import create_logger
from src.services import NadeoLive
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_maps")


class UpdateMapsThread(AbstractThread):
    @staticmethod
    def run_iteration():
        maps = Map.filter(name="")
        logger.info(f"Found {len(maps)} with empty name")
        for m in maps:
            logger.info(f"Fetching info for map with uid {m.uid}")
            try:
                mp = NadeoLive.get_map_info(m.uid)
                m.name = mp["name"]
                m.save()
            except Exception as e:
                logger.error(
                    f"Error while fetching info for map with uid {m.uid}",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )

    def handle(self):
        logger.info("Starting update_maps thread...")
        while True:
            self.run_iteration()
            logger.info("Waiting 30s before fetching maps data")
            time.sleep(30)
