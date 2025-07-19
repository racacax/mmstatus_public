from peewee import JOIN, fn

from models import Map, Game

map_name = input("Map Name:")
max_time = fn.MAX(Game.time).alias("max_time")
maps = (
    Map.select(Map.uid, max_time)
    .join(Game, JOIN.LEFT_OUTER)
    .where(Map.name == map_name)
    .group_by(Map.uid)
    .order_by(max_time.desc())
    .dicts()
)
if len(maps) == 0:
    print("No maps found")
elif len(maps) == 1:
    print("Only one map found")
else:
    merged_map = maps[0]
    print(f"All maps will be merged in Map with id {merged_map['uid']}. Last Game time is {merged_map['max_time']}")
    if input("Press any key to continue... Press q to quit") != "q":
        for map in maps[1:]:
            print(f"Switching matches with map uid {map['uid']} to {merged_map['uid']}...")
            print(Game.update(map_id=merged_map["uid"]).where(Game.map_id == map["uid"]).execute())
            Map.delete_by_id(map["uid"])
            print(f"Deleted map with id {map['uid']}.")
    else:
        print("Exiting...")
