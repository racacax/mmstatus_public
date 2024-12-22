import threading
import time

from threads.get_matches import GetMatchesThread
from threads.update_big_queries import (
    update_big_queries,
)
from threads.update_maps import update_maps
from threads.update_match_elo import update_match_elo
from threads.update_matches import update_matches
from threads.update_player_countries import update_player_countries
from threads.update_player_positions import update_player_positions
from threads.update_player_ranks import update_player_ranks
from threads.update_player_zones import update_player_zones
from threads.update_players import update_players

from settings import ENABLE_OAUTH, ENABLE_THREADS
from src.services import NadeoCore

if ENABLE_THREADS:
    threading.Thread(target=update_big_queries, args=[]).start()
    threading.Thread(target=GetMatchesThread().handle, args=[]).start()
    threading.Thread(target=update_maps, args=[]).start()
    threading.Thread(target=update_player_zones, args=[]).start()
    threading.Thread(target=update_player_countries, args=[]).start()
    threading.Thread(target=update_player_ranks, args=[]).start()
    threading.Thread(target=update_matches, args=[]).start()
    threading.Thread(target=update_match_elo, args=[]).start()
    threading.Thread(target=update_player_positions, args=[]).start()

    def update_main_nadeo_token():
        while True:
            try:
                NadeoCore.refresh_token()
            except Exception as e:
                print(e)
            time.sleep(42800)

    threading.Thread(target=update_main_nadeo_token, args=[]).start()

    if ENABLE_OAUTH:
        threading.Thread(target=update_players, args=[]).start()

while True:
    time.sleep(1)
