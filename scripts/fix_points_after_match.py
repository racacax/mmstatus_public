"""
Fix PlayerGame.points_after_match entries that are incorrectly set to 0 or NULL.

For each finished game in the given time range where a player's
points_after_match is 0 or NULL, this script looks at that player's
immediately preceding and following finished game (across all time).
If both neighbours have points_after_match > 300, the bad value is replaced
with the average of the two surrounding values (rounded to the nearest integer).

Rows are skipped when:
  - There is no previous or next finished game for that player
  - Either neighbour has points_after_match = NULL, 0, or <= 300

The fix is applied in batches of players so memory usage stays bounded even
on tables with millions of PlayerGame rows.

Usage:
    # Dry-run (default) — show how many rows would be fixed
    python scripts/fix_points_after_match.py --start "2026-01-01" --end "2026-04-01"
    python scripts/fix_points_after_match.py --hours 48

    # Apply
    python scripts/fix_points_after_match.py --start "2026-01-01" --end "2026-04-01" --apply
    python scripts/fix_points_after_match.py --hours 48 --apply

    # Interactive date prompt (no time args supplied)
    python scripts/fix_points_after_match.py --apply

    # Custom batch size
    python scripts/fix_points_after_match.py --apply --batch 500
"""

import argparse
import sys
from datetime import datetime, timedelta

from settings import db

MIN_NEIGHBOUR_POINTS = 300
DEFAULT_BATCH_SIZE = 1000

DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M:%S"


def parse_datetime(s: str) -> datetime:
    for fmt in (DATETIME_FMT, DATE_FMT):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse date/datetime: {s!r}  (expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)")


def prompt_range() -> tuple[datetime, datetime]:
    print("No time range supplied — enter dates interactively.")
    start_raw = input("Start date/datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS): ").strip()
    end_raw = input("End   date/datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS): ").strip()
    return parse_datetime(start_raw), parse_datetime(end_raw)


def fetch_affected_player_ids(start_time: datetime, end_time: datetime) -> list:
    cursor = db.execute_sql(
        """
        SELECT DISTINCT pg.player_id
        FROM playergame pg
        INNER JOIN game g ON pg.game_id = g.id
        WHERE g.is_finished = 1
          AND (pg.points_after_match = 0 OR pg.points_after_match IS NULL)
          AND g.time >= %s
          AND g.time <= %s
        ORDER BY pg.player_id
        """,
        [start_time, end_time],
    )
    return [row[0] for row in cursor.fetchall()]


def count_fixable(player_ids: list, start_time: datetime, end_time: datetime) -> int:
    """Return how many rows *would* be updated, without changing anything."""
    if not player_ids:
        return 0
    placeholders = ",".join(["%s"] * len(player_ids))
    cursor = db.execute_sql(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT sub.id
            FROM (
                SELECT
                    pg.id,
                    pg.points_after_match,
                    g.time AS game_time,
                    LAG(pg.points_after_match) OVER (
                        PARTITION BY pg.player_id ORDER BY g.time, pg.id
                    ) AS prev_p,
                    LEAD(pg.points_after_match) OVER (
                        PARTITION BY pg.player_id ORDER BY g.time, pg.id
                    ) AS next_p
                FROM playergame pg
                INNER JOIN game g ON pg.game_id = g.id
                WHERE g.is_finished = 1
                  AND pg.player_id IN ({placeholders})
            ) sub
            WHERE (sub.points_after_match = 0 OR sub.points_after_match IS NULL)
              AND sub.game_time >= %s
              AND sub.game_time <= %s
              AND sub.prev_p IS NOT NULL
              AND sub.next_p IS NOT NULL
              AND sub.prev_p > %s
              AND sub.next_p > %s
        ) counted
        """,
        player_ids + [start_time, end_time, MIN_NEIGHBOUR_POINTS, MIN_NEIGHBOUR_POINTS],
    )
    return cursor.fetchone()[0]


def fix_batch(player_ids: list, start_time: datetime, end_time: datetime) -> int:
    """Apply the fix for a batch of player IDs. Returns the number of updated rows."""
    placeholders = ",".join(["%s"] * len(player_ids))
    cursor = db.execute_sql(
        f"""
        UPDATE playergame AS target
        JOIN (
            SELECT
                sub.id,
                ROUND((sub.prev_p + sub.next_p) / 2) AS fixed_points
            FROM (
                SELECT
                    pg.id,
                    pg.points_after_match,
                    g.time AS game_time,
                    LAG(pg.points_after_match) OVER (
                        PARTITION BY pg.player_id ORDER BY g.time, pg.id
                    ) AS prev_p,
                    LEAD(pg.points_after_match) OVER (
                        PARTITION BY pg.player_id ORDER BY g.time, pg.id
                    ) AS next_p
                FROM playergame pg
                INNER JOIN game g ON pg.game_id = g.id
                WHERE g.is_finished = 1
                  AND pg.player_id IN ({placeholders})
            ) sub
            WHERE (sub.points_after_match = 0 OR sub.points_after_match IS NULL)
              AND sub.game_time >= %s
              AND sub.game_time <= %s
              AND sub.prev_p IS NOT NULL
              AND sub.next_p IS NOT NULL
              AND sub.prev_p > %s
              AND sub.next_p > %s
        ) AS computed ON target.id = computed.id
        SET target.points_after_match = computed.fixed_points
        """,
        player_ids + [start_time, end_time, MIN_NEIGHBOUR_POINTS, MIN_NEIGHBOUR_POINTS],
    )
    return cursor.rowcount


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Actually write changes (default is dry-run)")
    parser.add_argument(
        "--batch",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Players per batch (default: {DEFAULT_BATCH_SIZE})",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--hours", type=float, metavar="N", help="Process the last N hours")
    group.add_argument("--start", metavar="DATETIME", help="Start of the range (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end", metavar="DATETIME", help="End of range (default: now); only used with --start")
    args = parser.parse_args()

    if args.hours is not None:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=args.hours)
    elif args.start is not None:
        start_time = parse_datetime(args.start)
        end_time = parse_datetime(args.end) if args.end else datetime.now()
    else:
        try:
            start_time, end_time = prompt_range()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(0)

    dry_run = not args.apply
    batch_size = args.batch

    if dry_run:
        print("DRY-RUN mode — pass --apply to write changes\n")

    print(f"Range: {start_time}  →  {end_time}")
    print("Fetching affected player IDs...", flush=True)
    player_ids = fetch_affected_player_ids(start_time, end_time)
    total_players = len(player_ids)
    print(f"Found {total_players} players with at least one points_after_match = 0 or NULL in range\n")

    if total_players == 0:
        print("Nothing to fix.")
        return

    total_batches = (total_players + batch_size - 1) // batch_size

    if dry_run:
        print("Counting fixable rows per batch...")
        total_fixable = 0
        for i in range(0, total_players, batch_size):
            batch = player_ids[i : i + batch_size]
            n = count_fixable(batch, start_time, end_time)
            total_fixable += n
            batch_num = i // batch_size + 1
            print(f"  Batch {batch_num}/{total_batches}: {n} rows fixable")
        print(f"\nTotal rows that would be fixed: {total_fixable}")
        print("\nRe-run with --apply to apply the changes.")
        return

    confirm = input(f"About to fix rows for {total_players} players in {total_batches} batches. Continue? [y/N] ")
    if confirm.strip().lower() != "y":
        print("Aborted.")
        sys.exit(0)

    total_fixed = 0
    for i in range(0, total_players, batch_size):
        batch = player_ids[i : i + batch_size]
        n = fix_batch(batch, start_time, end_time)
        total_fixed += n
        batch_num = i // batch_size + 1
        print(f"Batch {batch_num}/{total_batches}: fixed {n} rows (running total: {total_fixed})", flush=True)

    print(f"\nDone. Fixed {total_fixed} PlayerGame records.")


if __name__ == "__main__":
    main()
