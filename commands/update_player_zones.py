import time

from models import Player, Zone
from src.services import NadeoCore


def update_player_zones():
    while True:
        try:
            players = Player.select(Player).where(Player.zone == None).paginate(1, 50)
            try:
                if len(players) == 0:
                    continue
                ids = [str(p.uuid) for p in players]
                print("Updating zone", ids)
                player_zones = NadeoCore.get_player_zones(ids)
                player_zones = {p["accountId"]: p["zoneId"] for p in player_zones}
                for p in players:
                    p.zone = Zone.get_or_none(uuid=player_zones.get(str(p.uuid), None))
                    p.save()
            except Exception as e:
                print("update_player_zones error", players, e)
        except Exception as e2:
            print("update_player_zones error", e2)
        time.sleep(10)
