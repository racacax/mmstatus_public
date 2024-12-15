import time

from models import Player


def update_player_countries():
    while True:
        try:
            players = (
                Player.select(Player)
                .where(Player.zone != None, Player.country == None)
                .paginate(1, 50)
            )
            try:
                if len(players) == 0:
                    continue
                ids = [str(p.uuid) for p in players]
                print("Updating countries", ids)
                for p in players:
                    final_zone = p.zone
                    while final_zone.country_alpha3 is None and final_zone.parent:
                        final_zone = final_zone.parent
                    p.country = final_zone
                    p.save()
            except Exception as e:
                print("update_player_countries error", players, e)
        except Exception as e2:
            print("update_player_countries error", e2)
        time.sleep(10)
