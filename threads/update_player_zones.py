import time
import traceback

from models import Player, Zone
from src.log_utils import create_logger
from src.services import NadeoCore
from threads.abstract_thread import AbstractThread

logger = create_logger("update_player_zones")


class UpdatePlayerZonesThread(AbstractThread):

    @staticmethod
    def get_zones_and_update(players: list[Player]):
        """
        Get and update all zones of the selected list of players
        :param players: Players that need to get a Zone
        :return:
        """
        ids = [str(p.uuid) for p in players]
        logger.info("Updating zones for players", extra={"ids": ids})
        player_zones = NadeoCore.get_player_zones(ids)  # API allows up to 50 ids
        logger.info("get_player_zones response", extra={"response": player_zones})
        player_zones = {p["accountId"]: p["zoneId"] for p in player_zones}
        for p in players:
            p.zone = Zone.get_or_none(uuid=player_zones.get(str(p.uuid), None))  # Zone has to exist in db
            p.save()

    def run_iteration(self):
        """
        Will get first 50 players without a zone
        For each of them, we will get the zone and update in the database
        """
        logger.info("Fetch first 50 players without zone info")
        players = Player.select(Player).where(Player.zone == None).paginate(1, 50)
        try:
            logger.info(f"Found {len(players)} players")
            if len(players) > 0:
                self.get_zones_and_update(players)
        except Exception as e:
            logger.error(
                "Error while updating players zones",
                extra={
                    "exception": e,
                    "traceback": traceback.format_exc(),
                },
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
            logger.info("Waiting 10s before starting thread again")
            time.sleep(10)
