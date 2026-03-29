"""
Manually recompute big-query cache files for one or more seasons.

Usage:
    python scripts/recompute_big_queries.py 3 5 7   # season IDs as positional args
    python scripts/recompute_big_queries.py          # interactive: prompts for IDs
"""

import sys

from models import Season
from src.threads.update_big_queries import UpdateBigQueriesThread


def recompute(season_ids: list[int]):
    thread = UpdateBigQueriesThread()
    for season_id in season_ids:
        season = Season.get_or_none(Season.id == season_id)
        if not season:
            print(f"Season {season_id} not found, skipping.")
            continue
        print(f"Recomputing season {season_id} ({season.name})...")
        queries = thread.get_queries(season)
        for q in queries:
            print(f"  {q.__name__}")
            thread.run_query(q, season)
        print(f"Done with season {season_id}.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        season_ids = [int(x) for x in sys.argv[1:]]
    else:
        raw = input("Season IDs to recompute (comma-separated): ")
        season_ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
    recompute(season_ids)
