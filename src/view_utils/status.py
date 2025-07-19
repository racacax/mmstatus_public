from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from typing import Union, TypedDict

from peewee import fn, Case, Expression

from models import RankStatRollup, Game
from src.utils import DateUtils, STATS_PERIODS, RANKS


class RollupObject(TypedDict):
    end_time: datetime
    count: Decimal
    last_game_time: Union[datetime, None]
    rank: int


class LiveStatsObject(TypedDict):
    r0: Decimal
    r0_date: Union[datetime, None]
    r1: Decimal
    r1_date: Union[datetime, None]
    r2: Decimal
    r2_date: Union[datetime, None]
    r3: Decimal
    r3_date: Union[datetime, None]
    r4: Decimal
    r4_date: Union[datetime, None]
    r5: Decimal
    r5_date: Union[datetime, None]
    r6: Decimal
    r6_date: Union[datetime, None]
    r7: Decimal
    r7_date: Union[datetime, None]
    r8: Decimal
    r8_date: Union[datetime, None]
    r9: Decimal
    r9_date: Union[datetime, None]
    r10: Decimal
    r10_date: Union[datetime, None]
    r11: Decimal
    r11_date: Union[datetime, None]
    r12: Decimal
    r12_date: Union[datetime, None]


def get_rollup_stats(
    min_date: datetime, max_date: datetime
) -> tuple[Union[list[RollupObject], None], Union[Expression, None]]:
    monthly_range, remaining_monthly_ranges = DateUtils.divide_range_by_full_months(min_date, max_date)

    weekly_ranges = []
    remaining_weekly_ranges = []
    for r in remaining_monthly_ranges:
        weekly_range, remaining_weekly_range = DateUtils.divide_range_by_full_weeks(r[0], r[1])
        if weekly_range:
            weekly_ranges.append(weekly_range)
        if remaining_weekly_range:
            remaining_weekly_ranges += remaining_weekly_range

    daily_ranges = []
    remaining_daily_ranges = []
    for r in remaining_weekly_ranges:
        daily_range, remaining_daily_range = DateUtils.divide_range_by_full_days(r[0], r[1])
        if daily_range:
            daily_ranges.append(daily_range)
        if remaining_daily_range:
            remaining_daily_ranges += remaining_daily_range

    hourly_ranges = []
    remaining_hourly_ranges = []
    for r in remaining_daily_ranges:
        hourly_range, remaining_hourly_range = DateUtils.divide_range_by_full_hours(r[0], r[1])
        if hourly_range:
            hourly_ranges.append(hourly_range)
        if remaining_hourly_range:
            remaining_hourly_ranges += remaining_hourly_range

    if len(remaining_hourly_ranges):
        direct_compute_condition = DateUtils.format_ranges_to_sql(Game.time, Game.time, remaining_hourly_ranges or [])
    else:
        direct_compute_condition = None
    conditions = [
        (
            DateUtils.format_ranges_to_sql(
                RankStatRollup.start_time,
                RankStatRollup.end_time,
                monthly_range and [monthly_range] or [],
                RankStatRollup.period == STATS_PERIODS["MONTHLY"],
            )
        ),
        (
            DateUtils.format_ranges_to_sql(
                RankStatRollup.start_time,
                RankStatRollup.end_time,
                weekly_ranges or [],
                RankStatRollup.period == STATS_PERIODS["WEEKLY"],
            )
        ),
        (
            DateUtils.format_ranges_to_sql(
                RankStatRollup.start_time,
                RankStatRollup.end_time,
                daily_ranges or [],
                RankStatRollup.period == STATS_PERIODS["DAILY"],
            )
        ),
        (
            DateUtils.format_ranges_to_sql(
                RankStatRollup.start_time,
                RankStatRollup.end_time,
                hourly_ranges or [],
                RankStatRollup.period == STATS_PERIODS["HOURLY"],
            )
        ),
    ]

    conditions = list(filter(lambda con: con is not None, conditions))
    if len(conditions):
        final_condition = None
        for c in conditions:
            if final_condition is None:
                final_condition = c
            else:
                final_condition |= c
        rollup_stats = (
            RankStatRollup.select(
                fn.MAX(RankStatRollup.end_time),
                fn.SUM(RankStatRollup.count),
                fn.MAX(RankStatRollup.last_game_time),
                RankStatRollup.rank,
            )
            .where(final_condition)
            .group_by(RankStatRollup.rank)
            .dicts()
        )
        rollup_stats = [stat for stat in rollup_stats]
    else:
        rollup_stats = None

    return rollup_stats, direct_compute_condition


def get_condition(min_elo: int, max_elo: int, alias, is_tm=False) -> tuple[Expression, Expression]:
    if is_tm:
        condition = Game.trackmaster_limit <= max_elo
    else:
        condition = Game.trackmaster_limit >= min_elo
    condition = (Game.min_elo <= max_elo) & (Game.max_elo >= min_elo) & condition
    return (
        fn.SUM(Case(None, [(condition, 1)], 0)).alias(alias),
        fn.MAX(Case(None, [(condition, Game.time)], None)).alias(alias + "_date"),
    )


def compute_remaining_stats(remaining_condition: Expression) -> Union[LiveStatsObject, None]:
    conditions: list[Expression] = []
    for i in range(len(RANKS) - 1, -1, -1):
        if i == 0:
            max_elo = 999999
        else:
            max_elo = RANKS[i - 1]["min_elo"] - 1
        conditions.extend(
            get_condition(
                RANKS[i]["min_elo"],
                max_elo,
                f'r{RANKS[i]["id"]}',
                RANKS[i]["min_rank"] is not None,
            )
        )

    g: Union[LiveStatsObject, None] = Game.select(*conditions).where(remaining_condition).dicts().get_or_none()
    return g


def get_merged_stats(
    rollup_stats: Union[list[RollupObject], None], live_stats: Union[LiveStatsObject, None]
) -> Union[list[RollupObject], None]:
    merged_stats = rollup_stats and deepcopy(rollup_stats) or None
    if live_stats and merged_stats:
        for i in range(len(merged_stats)):
            key = f'r{merged_stats[i]["rank"]}'
            if key in live_stats:
                merged_stats[i]["count"] += live_stats[key] or 0
                if f"{key}_date" in live_stats and live_stats[f"{key}_date"]:
                    merged_stats[i]["last_game_time"] = max(
                        live_stats[f"{key}_date"], merged_stats[i]["last_game_time"]
                    )
    elif live_stats:
        merged_stats = [
            {
                "count": live_stats.get(f'r{r["id"]}'),
                "last_game_time": live_stats.get(f'r{r["id"]}_date'),
                "rank": r["id"],
            }
            for r in RANKS
        ]
    return merged_stats
