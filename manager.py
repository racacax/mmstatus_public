import threading
import time

from commands.get_matches import get_matches
from commands.update_big_queries import (
    update_big_queries,
)
from commands.update_maps import update_maps
from commands.update_match_elo import update_match_elo
from commands.update_matches import update_matches
from commands.update_player_countries import update_player_countries
from commands.update_player_positions import update_player_positions
from commands.update_player_ranks import update_player_ranks
from commands.update_player_zones import update_player_zones
from commands.update_players import update_players

from settings import ENABLE_OAUTH, ENABLE_THREADS
from src.services import NadeoCore

if ENABLE_THREADS:
    threading.Thread(target=update_big_queries, args=[]).start()
    threading.Thread(target=get_matches, args=[]).start()
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
