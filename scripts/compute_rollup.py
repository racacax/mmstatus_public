import os
from datetime import datetime, timedelta

from peewee import fn

from models import Game
from src.threads.update_stats_per_rank import get_last_time_range, UpdateStatsPerRank
from src.utils import STATS_PERIODS


def get_or_create_all(min_date, max_date, period):
    cursor: datetime = min_date
    #
    while cursor < max_date:
        start_time, end_time = get_last_time_range(period, cursor)
        print(f"Creating stats for {start_time} to {end_time} ({period})")
        UpdateStatsPerRank.create_if_not_exists(period, STATS_PERIODS[period], start_time, end_time)
        if period == 'HOURLY':
            cursor += timedelta(hours=1)
        elif period == 'DAILY':
            cursor += timedelta(days=1)
        elif period == 'WEEKLY':
            cursor += timedelta(days=7)
        elif period == 'MONTHLY':
            cursor = cursor.replace(day=7) + timedelta(days=30)
def migrate_forward():
    """
    Migration to compute all previous rollup stats.
    """
    if os.environ.get("ENVIRONMENT") != "test":
        min_max = Game.select(fn.MIN(Game.time).alias('min'), fn.MAX(Game.time).alias('max')).dicts().get_or_none()

        #MONTHLY
        print('Perform monthly stats')
        get_or_create_all(min_max['min'], min_max['max'], 'MONTHLY')
        #WEEKLY
        print('Perform weekly stats')
        get_or_create_all(min_max['min'], min_max['max'], 'WEEKLY')
        #DAILY
        print('Perform daily stats')
        get_or_create_all(min_max['min'], min_max['max'], "DAILY")
        #HOURLY
        print('Perform hourly stats')
        get_or_create_all(min_max['min'], min_max['max'], "HOURLY")

migrate_forward()