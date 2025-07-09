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
    min_elo = IntegerField(default=-1, index=True)
    average_elo = IntegerField(default=-1, index=True)
    max_elo = IntegerField(default=-1, index=True)
    time = DateTimeField(default=datetime.datetime(1970, 1, 1, 1, 0), index=True)
    is_finished = BooleanField(default=False)
    trackmaster_limit = IntegerField(default=999999)
    rounds = IntegerField(null=True)
    class Meta:
        table_name = "game"
        indexes = (
            (('id',), True),
            )


@snapshot.append
class Zone(peewee.Model):
    uuid = UUIDField(unique=True)
    name = TextField()
    parent = snapshot.ForeignKeyField(backref='children', index=True, model='@self', null=True)
    country_alpha3 = CharField(max_length=10, null=True)
    file_name = TextField()
    class Meta:
        table_name = "zone"


@snapshot.append
class Player(peewee.Model):
    uuid = UUIDField(primary_key=True)
    name = TextField(default='')
    points = IntegerField(default=0, index=True)
    rank = IntegerField(default=99999, index=True)
    last_points_update = DateTimeField(default=datetime.datetime.now)
    last_name_update = DateTimeField(default=datetime.datetime(1970, 1, 1, 1, 0))
    last_match = snapshot.ForeignKeyField(backref='players', index=True, model='game', null=True)
    zone = snapshot.ForeignKeyField(backref='players', index=True, model='zone', null=True)
    country = snapshot.ForeignKeyField(backref='player_countries', index=True, model='zone', null=True)
    club_tag = CharField(max_length=64, null=True)
    class Meta:
        table_name = "player"


@snapshot.append
class PlayerGame(peewee.Model):
    game = snapshot.ForeignKeyField(backref='player_games', index=True, model='game')
    player = snapshot.ForeignKeyField(backref='player_games', index=True, model='player')
    is_win = BooleanField(default=False)
    is_mvp = BooleanField(default=False)
    points = IntegerField(null=True)
    position = IntegerField(null=True)
    points_after_match = IntegerField(null=True)
    rank_after_match = IntegerField(null=True)
    class Meta:
        table_name = "playergame"


@snapshot.append
class Season(peewee.Model):
    name = CharField(max_length=32)
    start_time = DateTimeField()
    end_time = DateTimeField()
    is_aggregated = BooleanField(default=False)
    class Meta:
        table_name = "season"


@snapshot.append
class PlayerSeason(peewee.Model):
    season = snapshot.ForeignKeyField(backref='player_seasons', index=True, model='season')
    player = snapshot.ForeignKeyField(backref='player_seasons', index=True, model='player')
    points = IntegerField(default=0, index=True)
    rank = IntegerField(default=99999, index=True)
    class Meta:
        table_name = "playerseason"


@snapshot.append
class RankStatRollup(peewee.Model):
    start_time = DateTimeField(index=True)
    end_time = DateTimeField(index=True)
    period = IntegerField(default=0, index=True)
    rank = IntegerField(default=0, index=True)
    count = IntegerField(default=0)
    last_game_time = DateTimeField(default=datetime.datetime(1970, 1, 1, 1, 0), null=True)
    class Meta:
        table_name = "rankstatrollup"


def migrate_forward(op, old_orm, new_orm):
    op.run_data_migration()
    op.add_index(new_orm.game, 'game_time')


def migrate_backward(op, old_orm, new_orm):
    op.drop_index(old_orm.game, 'game_time')
    op.run_data_migration()
