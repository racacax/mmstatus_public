"""
Manually recompute big-query cache files for one or more seasons.

Usage:
    python scripts/recompute_big_queries.py 3 5 7              # all stats for seasons 3, 5, 7
    python scripts/recompute_big_queries.py                    # interactive: prompts for IDs
    python scripts/recompute_big_queries.py --stats top_100    # only queries whose name contains 'top_100'
    python scripts/recompute_big_queries.py 3 --stats top_100 activity_per_country

Season IDs and --stats filters can appear in any order.
"""

import sys

from models import Season
from src.threads.update_big_queries import UpdateBigQueriesThread


def recompute(season_ids: list[int], filters: list[str] | None = None):
    thread = UpdateBigQueriesThread()
    for season_id in season_ids:
        season = Season.get_or_none(Season.id == season_id)
        if not season:
            print(f"Season {season_id} not found, skipping.")
            continue
        print(f"Recomputing season {season_id} ({season.name})...")
        queries = thread.get_queries(season)
        if filters:
            queries = [q for q in queries if any(f in q.__name__ for f in filters)]
        for q in queries:
            print(f"  {q.__name__}")
            thread.run_query(q, season)
        print(f"Done with season {season_id}.")


if __name__ == "__main__":
    args = sys.argv[1:]
    filters: list[str] | None = None

    if "--stats" in args:
        idx = args.index("--stats")
        filters = args[idx + 1 :]
        args = args[:idx]

    if args:
        season_ids = [int(x) for x in args]
    else:
        raw = input("Season IDs to recompute (comma-separated): ")
        season_ids = [int(x.strip()) for x in raw.split(",") if x.strip()]

    recompute(season_ids, filters)
