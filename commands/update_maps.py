import time
from models import Map
from src.services import NadeoLive


def update_maps():

    while True:
        maps = Map.filter(name="")
        for m in maps:
            print(m.uid)
            try:
                mp = NadeoLive.get_map_info(m.uid)
                m.name = mp["name"]
                m.save()
            except Exception as e:
                print(m, e)
        time.sleep(30)
