import re
import time
import traceback
from datetime import timedelta

from models import Game, Map, Season
from src.log_utils import create_logger
from src.services import NadeoLive
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_maps")

SEASON_NAMES = ["Winter", "Spring", "Summer", "Fall"]
_MAP_SEASON_RE = re.compile(r"^(Spring|Summer|Fall|Winter)\s+(\d{4})\s*-")
_SEASON_NAME_RE = re.compile(r"^(Spring|Summer|Fall|Winter)\s+(\d{4})$")


def parse_map_season(map_name: str):
    """Return (season_name, year) if map_name matches 'Season YYYY - …', else None."""
    m = _MAP_SEASON_RE.match(map_name)
    if not m:
        return None
    return m.group(1), int(m.group(2))


def expected_next_season(current_season_name: str):
    """Return (season_name, year) for the season following current_season_name, or None."""
    m = _SEASON_NAME_RE.match(current_season_name)
    if not m:
        return None
    idx = SEASON_NAMES.index(m.group(1))
    year = int(m.group(2))
    next_idx = (idx + 1) % 4
    next_year = year + 1 if next_idx == 0 else year
    return SEASON_NAMES[next_idx], next_year


def check_season_transition(map_obj: Map):
    """
    After a map name is resolved, check whether it signals a new TM season.

    If the map belongs to the season immediately following the current one,
    close the current season and open the new one, both anchored to the
    start time of the first game played on that map.
    """
    parsed = parse_map_season(map_obj.name)
    if not parsed:
        return

    map_season_name, map_season_year = parsed

    current_season = Season.get_current_season()
    if not current_season:
        return

    expected = expected_next_season(current_season.name)
    if not expected or (map_season_name, map_season_year) != expected:
        return

    first_game = Game.select().where(Game.map == map_obj).order_by(Game.time.asc()).first()
    if not first_game:
        return

    transition_time = first_game.time
    new_season_name = f"{map_season_name} {map_season_year}"

    if Season.select().where(Season.name == new_season_name).exists():
        return

    current_season.end_time = transition_time
    current_season.save()

    Season.create(
        name=new_season_name,
        start_time=transition_time,
        end_time=transition_time + timedelta(days=150),
    )

    logger.info(
        f"Season transition: {current_season.name} → {new_season_name} at {transition_time}",
        extra={"map_uid": map_obj.uid, "map_name": map_obj.name},
    )


class UpdateMapsThread(AbstractThread):
    def run_iteration(self):
        maps = Map.filter(name="")
        logger.info(f"Found {len(maps)} with empty name")
        for m in maps:
            logger.info(f"Fetching info for map with uid {m.uid}")
            try:
                mp = NadeoLive.get_map_info(m.uid)
                m.name = mp["name"]
                m.save()
                check_season_transition(m)
            except Exception as e:
                self._record_error()
                logger.error(
                    f"Error while fetching info for map with uid {m.uid}",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )

    def handle(self):
        logger.info("Starting update_maps thread...")
        while True:
            self.run_iteration()
            logger.info("Waiting 30s before fetching maps data")
            time.sleep(30)
