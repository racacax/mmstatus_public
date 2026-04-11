import time
import traceback
from datetime import datetime, timedelta
from statistics import mean

from peewee import fn

from models import Game, PlayerGame
from src.log_utils import create_logger
from src.threads.abstract_thread import AbstractThread

logger = create_logger("recompute_match_elo")

DIVERGENCE_THRESHOLD = 300


class RecomputeMatchEloThread(AbstractThread):
    """
    Periodically checks finished matches where the stored min_elo or max_elo
    diverges by more than 300 points from the actual points_after_match values,
    and recomputes min_elo, max_elo and average_elo from points_after_match.

    Only processes matches where:
      - is_finished = True
      - min_elo has already been computed (min_elo != -1)
      - every PlayerGame has a non-null points_after_match
      - min_elo - min(points_after_match) > 300
        OR max_elo - max(points_after_match) > 300
    """

    def recompute_elo(self, match: Game) -> None:
        try:
            player_points = [pg.points_after_match for pg in match.player_games]
            match.min_elo = min(player_points)
            match.max_elo = max(player_points)
            match.average_elo = round(mean(player_points))
            match.save()
            logger.info(
                f"Recomputed elo for match {match.id}",
                extra={
                    "match_id": match.id,
                    "min_elo": match.min_elo,
                    "max_elo": match.max_elo,
                    "average_elo": match.average_elo,
                },
            )
        except Exception as e:
            self._record_error()
            logger.error(
                f"Error while recomputing elo for match {getattr(match, 'id', None)}",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )

    def get_divergent_matches(self, start_time: datetime, end_time: datetime):
        return (
            Game.select(Game)
            .join(PlayerGame)
            .where(
                Game.is_finished == True,
                Game.min_elo != -1,
                Game.time >= start_time,
                Game.time <= end_time,
            )
            .group_by(Game.id)
            .having(
                # All players must have points_after_match set
                fn.COUNT(PlayerGame.id) == fn.COUNT(PlayerGame.points_after_match),
                # Stored stats are more than threshold above actual points_after_match
                (Game.min_elo - fn.MIN(PlayerGame.points_after_match) > DIVERGENCE_THRESHOLD)
                | (Game.max_elo - fn.MAX(PlayerGame.points_after_match) > DIVERGENCE_THRESHOLD),
            )
        )

    def run_iteration(self, start_time: datetime, end_time: datetime) -> int:
        matches = list(self.get_divergent_matches(start_time, end_time))
        count = len(matches)
        logger.info(
            f"Found {count} matches with divergent elo in range",
            extra={"start_time": start_time, "end_time": end_time},
        )
        for match in matches:
            self.recompute_elo(match)
        return count

    def handle(self):
        while True:
            try:
                end_time = datetime.now()
                start_time = end_time - timedelta(hours=1)
                self.run_iteration(start_time, end_time)
            except Exception as e:
                self._record_error()
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 60s before starting thread again...")
            time.sleep(60)
