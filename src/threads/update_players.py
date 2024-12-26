import time
import traceback
from datetime import datetime, timedelta

from models import Player
from src.log_utils import create_logger
from src.services import NadeoOauth, NadeoCore
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_players")

MAX_PLAYERS = 50


class UpdatePlayersThread(AbstractThread):
    """
    Use the OAuth API to fetch players name and club tags
    Fetch players that didn't have name update in the last 24 hours (max 50)
    """

    @staticmethod
    def update_players(players: list[Player]):
        ids = [str(p.uuid) for p in players]
        logger.info("Update players", extra={"ids": ids})
        try:
            names = NadeoOauth.get_player_display_names(ids)
            logger.info("get_player_display_names response", extra={"names": names})
            club_tags = NadeoCore.get_player_club_tags(ids)
            logger.info("get_player_club_tags response", extra={"response": club_tags})
            club_tags = {entry["accountId"]: entry["clubTag"] for entry in (club_tags or [])}
            for p in players:
                p.name = names.get(str(p.uuid), "Name unknown")
                p.club_tag = club_tags.get(str(p.uuid), None)
                p.last_name_update = datetime.now()
                p.save()
        except Exception as e:
            logger.error(
                "Error while updating players",
                extra={
                    "exception": e,
                    "traceback": traceback.format_exc(),
                },
            )

    def run_iteration(self) -> int:
        """
        Get players with outdated names and club tags and update them.
        :return: seconds to wait to restart the thread
        """
        logger.info("Fetch first 50 players with name update older than 24 hours")
        players = (
            Player.select(Player)
            .where(Player.last_name_update < (datetime.now() - timedelta(hours=24)))
            .order_by(Player.last_name_update.asc())
            .paginate(1, MAX_PLAYERS)
        )
        count = len(players)
        logger.info(f"Found {count} players")
        if count > 0:
            self.update_players(players)
        return 1 if count == MAX_PLAYERS else 10

    def handle(self):
        while True:
            try:
                seconds_to_wait = self.run_iteration()
                logger.info(f"Waiting {seconds_to_wait}s before starting thread again...")
                time.sleep(seconds_to_wait)
            except Exception as e:
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
