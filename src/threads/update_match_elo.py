import time
import traceback
from datetime import datetime
from statistics import mean

from models import Game, Player, PlayerGame
from src.log_utils import create_logger
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_match_elo")


class UpdateMatchEloThread(AbstractThread):
    """
    Fetch matches and compute statistics about players elo.
    """

    @staticmethod
    def update_elo(match: Game):
        logger.info(f"Updating elo for match id {match.id}", extra={"match": match})
        try:
            player_points = [p.player.points for p in match.player_games]
            match.min_elo = min(player_points)
            match.max_elo = max(player_points)
            match.average_elo = mean(player_points)
            match.save()
        except Exception as e:
            logger.error(
                f"Error while updating match with id {match.id}",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )

    def run_iteration(self):
        matches = (
            Game.select(Game)
            .join(PlayerGame)
            .join(Player)
            .where(
                Game.average_elo == -1,
                Player.last_points_update != datetime.fromtimestamp(0),  # Player points need to be up to date
            )
            .group_by(Game)
        )
        logger.info(f"Found {len(matches)} with uncomputed elo")
        for match in matches:
            self.update_elo(match)

    def handle(self):
        while True:
            try:
                logger.info("Fetching matches with uncomputed average elo")
                self.run_iteration()
            except Exception as e:
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 5s before starting the thread again...")
            time.sleep(5)
