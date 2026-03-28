import threading
import time

from src.threads.get_matches import GetMatchesThread
from src.threads.update_big_queries import (
    UpdateBigQueriesThread,
)
from src.threads.update_main_nadeo_token import UpdateMainNadeoTokenThread
from src.threads.update_maps import UpdateMapsThread
from src.threads.update_match_elo import UpdateMatchEloThread
from src.threads.update_matches import UpdateMatchesThread
from src.threads.update_player_countries import UpdatePlayerCountriesThread
from src.threads.update_stats_per_rank import UpdateStatsPerRank
from src.threads.update_top_player_positions import UpdateTopPlayersPositionThread
from src.threads.update_player_ranks import UpdatePlayerRanksThread
from src.threads.update_player_zones import UpdatePlayerZonesThread
from src.threads.update_players import UpdatePlayersThread
from src.log_utils import create_logger
from src.thread_health import write_health_file

from settings import ENABLE_OAUTH, ENABLE_THREADS

logger = create_logger("manager")


def start_thread(cls):
    instance = cls()
    t = threading.Thread(target=instance.run)
    t.start()
    return t, instance


if ENABLE_THREADS:
    thread_classes = [
        UpdateBigQueriesThread,
        GetMatchesThread,
        UpdateMapsThread,
        UpdatePlayerZonesThread,
        UpdatePlayerCountriesThread,
        UpdatePlayerRanksThread,
        UpdateMatchesThread,
        UpdateMatchEloThread,
        UpdateTopPlayersPositionThread,
        UpdateMainNadeoTokenThread,
        UpdateStatsPerRank,
    ]
    if ENABLE_OAUTH:
        thread_classes.append(UpdatePlayersThread)

    active_threads = {cls: start_thread(cls) for cls in thread_classes}
    write_health_file(active_threads)

    while True:
        time.sleep(60)
        for cls, (t, instance) in list(active_threads.items()):
            if not t.is_alive():
                logger.warning(f"Thread {cls.__name__} has crashed. Restarting...")
                new_t, new_instance = start_thread(cls)
                new_instance._record_error()
                active_threads[cls] = new_t, new_instance
        write_health_file(active_threads)
else:
    while True:
        time.sleep(1)
