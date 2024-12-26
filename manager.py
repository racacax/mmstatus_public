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
from src.threads.update_top_player_positions import UpdateTopPlayersPositionThread
from src.threads.update_player_ranks import UpdatePlayerRanksThread
from src.threads.update_player_zones import UpdatePlayerZonesThread
from src.threads.update_players import UpdatePlayersThread

from settings import ENABLE_OAUTH, ENABLE_THREADS

if ENABLE_THREADS:
    threads = [
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
    ]
    if ENABLE_OAUTH:
        threads.append(UpdatePlayersThread)
    for thread in threads:
        threading.Thread(target=thread().handle, args=[]).start()

while True:
    time.sleep(1)
