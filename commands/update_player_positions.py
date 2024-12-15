import time
from datetime import datetime

from models import Player


def update_player_positions():
    # update top 200 leaderboard every minute
    while True:
        try:
            players = (Player.select().order_by(Player.points.desc())).paginate(1, 200)
            position = 1
            print("update_player_positions", [str(p.uuid) for p in players])
            now = datetime.now()
            for p in players:
                p.rank = position
                position += 1
                p.save()
            print("update_player_positions done in", (datetime.now() - now))
        except Exception as e2:
            print("update_player_positions error", e2)
        time.sleep(60)
