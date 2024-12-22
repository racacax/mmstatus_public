import time
import traceback

from models import Player, Zone
from src.log_utils import create_logger
from src.services import NadeoCore

logger = create_logger("update_player_zones")


def update_player_zones():
    while True:
        try:
            logger.info("Fetch first 50 players without zone info")
            players = Player.select(Player).where(Player.zone == None).paginate(1, 50)
            try:
                logger.info(f"Found {len(players)} players")
                if len(players) == 0:
                    logger.info("Waiting 10s before starting thread again")
                    time.sleep(10)
                    continue
                ids = [str(p.uuid) for p in players]
                logger.info("Updating zones for players", extra={"ids": ids})
                player_zones = NadeoCore.get_player_zones(ids)
                logger.info("get_player_zones response", extra={"response": player_zones})
                player_zones = {p["accountId"]: p["zoneId"] for p in player_zones}
                for p in players:
                    p.zone = Zone.get_or_none(uuid=player_zones.get(str(p.uuid), None))
                    p.save()
            except Exception as e:
                logger.error(
                    "Error while updating players zones",
                    extra={
                        "exception": e,
                        "traceback": traceback.format_exc(),
                    },
                )
        except Exception as e2:
            logger.error(
                "General error in the thread",
                extra={"exception": e2, "traceback": traceback.format_exc()},
            )
        logger.info("Waiting 10s before starting thread again")
        time.sleep(10)
