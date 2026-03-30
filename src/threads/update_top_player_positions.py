import time
import traceback
from datetime import datetime

from models import Player, PlayerSeason, Season
from src.log_utils import create_logger
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_top_player_positions")


class UpdateTopPlayersPositionThread(AbstractThread):
    """
    Since player positions can change every minute, ranking might not be accurate.
    At low elo, it doesn't really matter. But at high elo, we could get multiple players
    with the same position despite points being different.
    To compensate that, top 200 players by points get their position update every minute.
    """

    @staticmethod
    def run_iteration():
        logger.info("Fetching top 200 players by points")
        players = list((Player.select().order_by(Player.points.desc())).paginate(1, 200))
        logger.info(
            "Updating positions for players",
            extra={"players": [str(p.uuid) for p in players]},
        )
        now = datetime.now()
        for position, p in enumerate(players, start=1):
            p.rank = position
            p.save()

        season = Season.get_current_season()
        if season:
            rank_by_uuid = {str(p.uuid): p.rank for p in players}
            player_seasons = PlayerSeason.select().where(
                PlayerSeason.season_id == season.id,
                PlayerSeason.player_id.in_(list(rank_by_uuid.keys())),
            )
            for ps in player_seasons:
                ps.rank = rank_by_uuid[str(ps.player_id)]
                ps.save()

        logger.info(f"update_top_player_positions done in {(datetime.now() - now)}")

    def handle(self):
        while True:
            try:
                self.run_iteration()
            except Exception as e:
                self._record_error()
                logger.error(
                    "Error while updating player positions",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 60s before recomputing positions again...")
            time.sleep(60)
