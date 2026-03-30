"""
Recompute PlayerSeason.rank for the top 200 players of a given season,
based solely on PlayerSeason.points (no Player table involved).

Useful for past seasons where Player.rank may have since changed but all
top-200 players are guaranteed to have participated in that season.

Usage:
    python scripts/recompute_season_positions.py 3      # season ID as argument
    python scripts/recompute_season_positions.py        # interactive prompt
"""

import sys

from models import PlayerSeason, Season


def recompute_positions(season_id: int):
    season = Season.get_or_none(Season.id == season_id)
    if not season:
        print(f"Season {season_id} not found.")
        return

    print(f"Recomputing positions for season {season_id} ({season.name})...")

    top_200 = list(
        PlayerSeason.select()
        .where(PlayerSeason.season_id == season_id)
        .order_by(PlayerSeason.points.desc())
        .paginate(1, 200)
    )

    for position, ps in enumerate(top_200, start=1):
        ps.rank = position
        ps.save()

    print(f"Updated {len(top_200)} PlayerSeason ranks for season {season_id}.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        season_id = int(sys.argv[1])
    else:
        season_id = int(input("Season ID: ").strip())
    recompute_positions(season_id)
