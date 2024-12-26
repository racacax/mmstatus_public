import time
import traceback
from datetime import datetime, timedelta

from models import Player, PlayerGame, Season, PlayerSeason
from src.log_utils import create_logger
from src.services import NadeoLive
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_player_ranks")


class UpdatePlayerRanksThread(AbstractThread):
    """
    Will fetch players with points and position not updated in 12 hours
    and update them.
    Notes:
        We ignore players with 0 points except if they have never been updated.
        We check the oldest entries first

    """

    @staticmethod
    def update_player(p: Player, points: int, rank: int, season: Season) -> None:
        try:
            p.points = points
            p.rank = rank
            p.last_points_update = datetime.now()
            p.save()

            ps, _ = PlayerSeason.get_or_create(player=p, season=season)
            ps.points = p.points
            ps.rank = p.rank
            ps.save()
            if p.last_match and p.last_match.is_finished:
                pg = PlayerGame.get(game=p.last_match, player=p)
                if pg.points_after_match is None:
                    pg.points_after_match = p.points
                    pg.rank_after_match = p.rank
                    pg.save()
        except Exception as e:
            logger.error(
                "Error while updating player rank",
                extra={
                    "exception": e,
                    "traceback": traceback.format_exc(),
                    "player": p,
                },
            )

    def update_players(self, players: list[Player], season: Season):
        try:
            ids = [str(p.uuid) for p in players]
            logger.info("Updating players", extra={"ids": ids})
            ranks = NadeoLive.get_player_ranks(ids)
            logger.info("get_player_ranks response", extra={"response": ranks})
            scores = {p["player"]: p["score"] for p in ranks["results"]}
            ranks = {p["player"]: p["rank"] for p in ranks["results"]}
            for p in players:
                self.update_player(p, scores.get(str(p.uuid), 0), ranks.get(str(p.uuid), 0), season)
        except Exception as e:
            logger.error(
                "Error while updating players ranks",
                extra={
                    "exception": e,
                    "traceback": traceback.format_exc(),
                },
            )

    def run_iteration(self):
        logger.info("Getting players with outdated points (oldest 100)")
        season = Season.get_current_season()
        players = (
            Player.select(Player)
            .where(
                (Player.last_points_update < datetime.now() - timedelta(hours=12))
                & ((Player.points != 0) | (Player.last_points_update == datetime.fromtimestamp(0)))
            )
            .order_by(Player.last_points_update.asc())
            .paginate(1, 100)
        )
        count = len(players)
        logger.info(f"Found {count} players")
        if count > 0:
            self.update_players(players, season)

    def handle(self):
        while True:
            try:
                self.run_iteration()
            except Exception as e:
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 10s before starting thread again...")
            time.sleep(10)
