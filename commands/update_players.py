import time
import traceback
from datetime import datetime, timedelta

from models import Player
from src.log_utils import create_logger
from src.services import NadeoOauth, NadeoCore

logger = create_logger("update_players")


def update_players():
    while True:
        count = 0
        try:
            logger.info("Fetch first 50 players with name update older than 24 hours")
            players = (
                Player.select(Player)
                .where(Player.last_name_update < (datetime.now() - timedelta(hours=24)))
                .order_by(Player.last_name_update.asc())
                .paginate(1, 50)
            )
            count = len(players)
            logger.info(f"Found {count} players")
            if count == 0:
                logger.info("Waiting 10s before starting thread again...")
                time.sleep(10)
                continue
            ids = [str(p.uuid) for p in players]
            logger.info("Update players", extra={"ids": ids})
            try:
                names = NadeoOauth.get_player_display_names(ids)
                club_tags = {
                    entry["accountId"]: entry["clubTag"] for entry in (NadeoCore.get_player_club_tags(ids) or [])
                }
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

            logger.info("Waiting 5s before starting thread again...")
            time.sleep(5)

        except Exception as e2:
            logger.error(
                "General error in the thread",
                extra={"exception": e2, "traceback": traceback.format_exc()},
            )
        if count == 50:
            logger.info("Waiting 1s before starting thread again...")
            time.sleep(1)
        else:
            logger.info("Waiting 10s before starting thread again...")
            time.sleep(10)
