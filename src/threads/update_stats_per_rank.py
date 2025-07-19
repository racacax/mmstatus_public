import time
import traceback
from datetime import datetime, timedelta

from peewee import fn

from models import RankStatRollup, Game
from src.log_utils import create_logger
from src.threads.abstract_thread import AbstractThread
from src.utils import RANKS, STATS_PERIODS

logger = create_logger("update_stats_per_rank")


def get_last_time_range(period: str, base_time=None):
    if not base_time:
        base_time = datetime.now()
    if period == "HOURLY":
        end_time = base_time.replace(minute=59, second=59, microsecond=0) - timedelta(hours=1)
        start_time = end_time.replace(minute=0, second=0)
    elif period == "DAILY":
        end_time = base_time.replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
        start_time = end_time.replace(hour=0, minute=0, second=0)
    elif period == "WEEKLY":
        end_time = (base_time - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
        end_time -= timedelta(days=((end_time.isoweekday()) % 7))
        start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=6)
    elif period == "MONTHLY":
        end_time = base_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        start_time = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise NotImplementedError()
    return start_time, end_time


def get_stats_obj(rank_id, start_time, end_time, period_id):
    return (
        RankStatRollup.select(RankStatRollup.id)
        .where(
            (RankStatRollup.rank == rank_id)
            & (RankStatRollup.start_time == start_time)
            & (RankStatRollup.end_time == end_time)
            & (RankStatRollup.period == period_id)
        )
        .get_or_none()
    )


class UpdateStatsPerRank(AbstractThread):

    @classmethod
    def compute_stats(cls, start_time: datetime, end_time: datetime, period_id: int, rank_id: int, condition):
        logger.info(
            f"Computing stats for period {period_id} rank {rank_id} between {start_time} and {end_time}",
        )
        games_stats = (
            Game.select(fn.MAX(Game.time), fn.COUNT(Game.id))
            .where(condition & (Game.time >= start_time) & (Game.time <= end_time))
            .dicts()
        )
        stats = [g for g in games_stats]
        if len(stats) > 0:
            stats = stats[0]
            logger.info(
                "Create object",
            )
            RankStatRollup.create(
                start_time=start_time,
                end_time=end_time,
                period=period_id,
                rank=rank_id,
                count=stats["id"],
                last_game_time=stats["time"],
            )

    """
        Statistics about matches count are precomputed for performance improvement.
        Stats are saved hourly, weekly and monthly
    """

    @classmethod
    def create_if_not_exists(cls, period, period_id, start_time, end_time):
        for i in range(len(RANKS) - 1, -1, -1):
            logger.info(
                f"Checking info for {period}, {start_time}, {end_time}",
            )
            rank = RANKS[i]
            if i == 0:
                max_elo = 999999
            else:
                max_elo = RANKS[i - 1]["min_elo"] - 1
            stats_obj = get_stats_obj(rank["id"], start_time, end_time, period_id)
            if not stats_obj:
                if rank["key"] == "tm":
                    condition = Game.trackmaster_limit <= max_elo
                else:
                    condition = Game.trackmaster_limit >= rank["min_elo"]
                condition = (Game.min_elo <= max_elo) & (Game.max_elo >= rank["min_elo"]) & condition
                cls.compute_stats(start_time, end_time, period_id, rank["id"], condition)

    @classmethod
    def run(cls):
        for period, period_id in STATS_PERIODS.items():
            start_time, end_time = get_last_time_range(period)
            cls.create_if_not_exists(period, period_id, start_time, end_time)

    def handle(self):
        while True:
            try:
                self.run()
            except Exception as e:
                logger.error(
                    "Error while updating stats per rank",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 180s before retrying...")
            time.sleep(180)
