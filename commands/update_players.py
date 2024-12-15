import time
from datetime import datetime, timedelta

from models import Player
from src.services import NadeoOauth, NadeoCore


def update_players():
    while True:
        try:
            players = (
                Player.select(Player)
                .where(Player.last_name_update < (datetime.now() - timedelta(hours=24)))
                .order_by(Player.last_name_update.asc())
                .paginate(1, 50)
            )
            count = len(players)
            if count == 0:
                continue
            ids = [str(p.uuid) for p in players]
            print("update_players", ids)
            try:
                names = NadeoOauth.get_player_display_names(ids)
                club_tags = {
                    entry["accountId"]: entry["clubTag"]
                    for entry in (NadeoCore.get_player_club_tags(ids) or [])
                }
                for p in players:
                    p.name = names.get(str(p.uuid), "Name unknown")
                    p.club_tag = club_tags.get(str(p.uuid), None)
                    p.last_name_update = datetime.now()
                    p.save()
            except Exception as e:
                print("update_players error", players, e)
            time.sleep(5)

        except Exception as e2:
            print("update_players error", e2)
        time.sleep(1)
