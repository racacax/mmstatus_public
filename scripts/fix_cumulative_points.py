"""
Fix runs of consecutive 0/NULL points_after_match entries for players in a time range.

Unlike fix_points_after_match.py (which only handles isolated single zeros),
this script handles entire streaks by smoothing the curve between the last
known-good value before the streak and the first known-good value after it.

Shape of intermediate values is derived from score rank within each game
(player with highest score = 1st, etc.):
  1st → raw delta +40
  2nd → raw delta +20
  3rd → raw delta −20
  4th → raw delta −40

The raw deltas are scaled so their cumulative sum matches the actual net change
(P_after − P_before), preserving the relative shape.  When raw deltas sum to
zero or any position in the streak is missing/tied, linear interpolation is used.

Streaks are detected across the player's full game history; a streak is fixed
if at least one of its rows falls within the given time range.

Only 2v2 games (exactly 4 participants per game) are considered.

Rows are skipped when:
  - No valid game (points_after_match > MIN_NEIGHBOUR_POINTS) exists before or
    after the streak
  - The streak contains a position value outside 1-4 AND linear fallback is
    disabled (not applicable here — linear fallback is always used)

Usage:
    # Dry-run (default) — show what would be fixed
    python scripts/fix_cumulative_points.py --start "2026-01-01" --end "2026-04-01"
    python scripts/fix_cumulative_points.py --hours 48

    # Apply
    python scripts/fix_cumulative_points.py --start "2026-01-01" --end "2026-04-01" --apply
    python scripts/fix_cumulative_points.py --hours 48 --apply

    # Interactive date prompt
    python scripts/fix_cumulative_points.py --apply

    # Custom batch size
    python scripts/fix_cumulative_points.py --apply --batch 500
"""

import argparse
import sys
from datetime import datetime, timedelta

from settings import db

MIN_NEIGHBOUR_POINTS = 300
DEFAULT_BATCH_SIZE = 200

POSITION_DELTA = {1: 40, 2: 20, 3: -20, 4: -40}

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


def setup_temp_tables() -> None:
    """Precompute 4-player game IDs once into a temp table for reuse across all queries."""
    print("Building 4-player game index (one-time)...", flush=True)
    db.execute_sql("DROP TEMPORARY TABLE IF EXISTS tmp_4p_games")
    db.execute_sql("""
        CREATE TEMPORARY TABLE tmp_4p_games (game_id INT PRIMARY KEY)
        SELECT game_id FROM playergame GROUP BY game_id HAVING COUNT(*) = 4
        """)


def fetch_affected_player_ids(start_time: datetime, end_time: datetime) -> list:
    cursor = db.execute_sql(
        """
        SELECT DISTINCT pg.player_id
        FROM playergame pg
        INNER JOIN game g ON pg.game_id = g.id
        INNER JOIN tmp_4p_games t ON pg.game_id = t.game_id
        WHERE g.is_finished = 1
          AND (pg.points_after_match = 0 OR pg.points_after_match IS NULL)
          AND g.time >= %s
          AND g.time <= %s
        ORDER BY pg.player_id
        """,
        [start_time, end_time],
    )
    return [row[0] for row in cursor.fetchall()]


def fetch_player_games(player_id) -> list:
    """Return all 2v2 finished games for a player ordered by time, then id.

    Position is derived from score rank within each game via self-join:
    1 + count of opponents who scored strictly more = this player's rank.
    Each row: (pg_id, points_after_match, derived_position, game_time)
    """
    cursor = db.execute_sql(
        """
        SELECT pg.id, pg.points_after_match,
               1 + SUM(CASE WHEN pg2.points > pg.points THEN 1 ELSE 0 END) AS derived_pos,
               g.time
        FROM playergame pg
        INNER JOIN game g ON pg.game_id = g.id
        INNER JOIN tmp_4p_games t ON pg.game_id = t.game_id
        INNER JOIN playergame pg2 ON pg2.game_id = pg.game_id
        WHERE g.is_finished = 1
          AND pg.player_id = %s
        GROUP BY pg.id, pg.points_after_match, g.time
        ORDER BY g.time, pg.id
        """,
        [player_id],
    )
    return cursor.fetchall()


def _is_bad(value) -> bool:
    return value is None or value == 0


def _compute_streak_fixes(
    streak_rows: list,
    p_before: int,
    p_after: int,
) -> list:
    """Return list of (pg_id, fixed_value) for a streak.

    Uses position-based shape when all positions are valid, otherwise falls
    back to linear interpolation.
    """
    n = len(streak_rows)
    positions = [r[2] for r in streak_rows]
    actual_change = p_after - p_before

    use_position_shape = all(pos in POSITION_DELTA for pos in positions)

    if use_position_shape:
        raw_deltas = [POSITION_DELTA[pos] for pos in positions]
        raw_total = sum(raw_deltas)
    else:
        raw_total = 0

    fixes = []
    if raw_total != 0:
        cumulative_raw = 0
        for i, (row, rd) in enumerate(zip(streak_rows, raw_deltas)):
            cumulative_raw += rd
            fixed = round(p_before + cumulative_raw * actual_change / raw_total)
            fixes.append((row[0], fixed))
    else:
        # Linear interpolation
        for i, row in enumerate(streak_rows):
            fixed = round(p_before + actual_change * (i + 1) / n)
            fixes.append((row[0], fixed))

    return fixes


def compute_player_fixes(
    rows: list,
    start_time: datetime,
    end_time: datetime,
) -> list:
    """Walk a player's game history, find streaks overlapping the range, compute fixes.

    rows: list of (pg_id, points_after_match, position, game_time)
    Returns list of (pg_id, fixed_value).
    """
    fixes = []
    n = len(rows)
    i = 0

    while i < n:
        val = rows[i][1]
        if not _is_bad(val):
            i += 1
            continue

        # Found start of a streak of bad values
        streak_start = i
        while i < n and _is_bad(rows[i][1]):
            i += 1
        streak_end = i  # exclusive

        # Check if any streak row falls within the requested range
        in_range = any(start_time <= rows[j][3] <= end_time for j in range(streak_start, streak_end))
        if not in_range:
            continue

        # Need a valid neighbour on each side
        if streak_start == 0 or streak_end == n:
            continue

        p_before = rows[streak_start - 1][1]
        p_after = rows[streak_end][1]

        if not p_before or p_before <= MIN_NEIGHBOUR_POINTS:
            continue
        if not p_after or p_after <= MIN_NEIGHBOUR_POINTS:
            continue

        streak_rows = rows[streak_start:streak_end]
        fixes.extend(_compute_streak_fixes(streak_rows, p_before, p_after))

    return fixes


def process_batch(
    player_ids: list,
    start_time: datetime,
    end_time: datetime,
    apply: bool,
) -> tuple[int, int]:
    """Process a batch of players. Returns (streaks_found, rows_fixed_or_fixable)."""
    total_streaks = 0
    total_rows = 0

    for player_id in player_ids:
        rows = fetch_player_games(player_id)
        fixes = compute_player_fixes(rows, start_time, end_time)
        if not fixes:
            continue
        total_streaks += 1
        total_rows += len(fixes)

        if apply:
            for pg_id, fixed_val in fixes:
                db.execute_sql(
                    "UPDATE playergame SET points_after_match = %s WHERE id = %s",
                    [fixed_val, pg_id],
                )

    return total_streaks, total_rows


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
    parser.add_argument(
        "--end",
        metavar="DATETIME",
        help="End of range (default: now); only used with --start",
    )
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
    setup_temp_tables()
    print("Fetching affected player IDs...", flush=True)
    player_ids = fetch_affected_player_ids(start_time, end_time)
    total_players = len(player_ids)
    print(f"Found {total_players} players with at least one bad points_after_match in range\n")

    if total_players == 0:
        print("Nothing to fix.")
        return

    total_batches = (total_players + batch_size - 1) // batch_size

    if not dry_run:
        confirm = input(f"About to fix rows for {total_players} players in {total_batches} batches. Continue? [y/N] ")
        if confirm.strip().lower() != "y":
            print("Aborted.")
            sys.exit(0)

    total_streaks = 0
    total_rows = 0

    for i in range(0, total_players, batch_size):
        batch = player_ids[i : i + batch_size]
        batch_num = i // batch_size + 1
        streaks, rows = process_batch(batch, start_time, end_time, apply=not dry_run)
        total_streaks += streaks
        total_rows += rows
        print(
            f"  Batch {batch_num}/{total_batches}: "
            f"{streaks} players with streaks, {rows} rows {'fixable' if dry_run else 'fixed'} "
            f"(running total: {total_rows})",
            flush=True,
        )

    action = "would be fixed" if dry_run else "fixed"
    print(f"\nDone. {total_rows} rows {action} across {total_streaks} player streaks.")
    if dry_run:
        print("Re-run with --apply to apply the changes.")


if __name__ == "__main__":
    main()
