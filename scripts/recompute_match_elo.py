"""
Recompute min_elo, max_elo and average_elo for finished matches where the
stored values diverge by more than 300 points from the actual
points_after_match values.

Only matches where every PlayerGame has a non-null points_after_match are
considered. The recomputed values are derived entirely from points_after_match.

Usage:
    # Explicit date range
    python scripts/recompute_match_elo.py --start "2026-01-01" --end "2026-04-01"

    # Last N hours (convenience shorthand)
    python scripts/recompute_match_elo.py --hours 48

    # Interactive prompt (no arguments)
    python scripts/recompute_match_elo.py
"""

import argparse
import sys
from datetime import datetime, timedelta

from src.threads.recompute_match_elo import RecomputeMatchEloThread

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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
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

    print(f"Range: {start_time}  →  {end_time}")

    thread = RecomputeMatchEloThread()
    count = thread.run_iteration(start_time, end_time)
    print(f"Done. Recomputed elo for {count} match(es).")


if __name__ == "__main__":
    main()
