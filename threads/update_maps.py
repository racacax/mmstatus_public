import time
import traceback

from models import Map
from src.log_utils import create_logger
from src.services import NadeoLive

logger = create_logger("update_maps")


def update_maps():
    logger.info("Starting update_maps thread...")
    while True:
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
        logger.info("Waiting 30s before fetching maps data")
        time.sleep(30)
