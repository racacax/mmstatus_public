# auto-generated snapshot
from peewee import *
import datetime
import peewee


snapshot = Snapshot()


@snapshot.append
class Map(peewee.Model):
    uid = CharField(max_length=64, primary_key=True)
    name = CharField(default='', max_length=128)
    class Meta:
        table_name = "map"


@snapshot.append
class Game(peewee.Model):
    map = snapshot.ForeignKeyField(backref='games', index=True, model='map')
    min_elo = IntegerField(default=0)
    average_elo = IntegerField(default=0)
    max_elo = IntegerField(default=0)
    time = DateTimeField(default=datetime.datetime(1970, 1, 1, 1, 0))
    is_finished = BooleanField(default=False)
    class Meta:
        table_name = "game"
        indexes = (
            (('id',), True),
            )


@snapshot.append
class Player(peewee.Model):
    uuid = UUIDField(primary_key=True)
    name = TextField(default='')
    points = IntegerField(default=0, index=True)
    rank = IntegerField(default=99999, index=True)
    last_points_update = DateTimeField(default=datetime.datetime.now)
    class Meta:
        table_name = "player"


@snapshot.append
class PlayerGame(peewee.Model):
    game = snapshot.ForeignKeyField(backref='player_games', index=True, model='game')
    player = snapshot.ForeignKeyField(backref='player_games', index=True, model='player')
    class Meta:
        table_name = "playergame"


def forward(old_orm, new_orm):
    player = new_orm['player']
    return [
        # Apply default value datetime.datetime(2024, 4, 5, 21, 29, 27, 479665) to the field player.last_points_update,
        player.update({player.last_points_update: datetime.datetime(2024, 4, 5, 21, 29, 27, 479665)}).where(player.last_points_update.is_null(True)),
    ]
