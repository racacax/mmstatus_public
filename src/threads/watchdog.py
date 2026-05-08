import sys
import time
from datetime import datetime, timedelta

from models import Game
from src.log_utils import create_logger
from src.threads.abstract_thread import AbstractThread

logger = create_logger("watchdog")

STALE_THRESHOLD_MINUTES = 10
CHECK_INTERVAL_SECONDS = 60


class WatchdogThread(AbstractThread):
    def handle(self):
        while True:
            time.sleep(CHECK_INTERVAL_SECONDS)

            uptime = datetime.now() - self.start_time
            if uptime < timedelta(minutes=STALE_THRESHOLD_MINUTES):
                continue

            last_match = Game.select(Game.time).order_by(Game.time.desc()).paginate(1, 1).get_or_none()
            if last_match is None:
                continue

            age = datetime.now() - last_match.time
            if age > timedelta(minutes=STALE_THRESHOLD_MINUTES):
                logger.error(
                    f"No new match in {age}. Last match at {last_match.time}. Forcing exit to trigger restart.",
                )
                sys.exit(1)
