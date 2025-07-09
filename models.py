import datetime

import peewee
from peewee import (
    Model,
    CharField,
    IntegerField,
    ForeignKeyField,
    TextField,
    BooleanField,
    DateTimeField,
)

from settings import db


class BaseModel(Model):
    class Meta:
        database = db


class Map(BaseModel):
    uid = CharField(verbose_name="UID", primary_key=True, max_length=64)
    name = CharField(verbose_name="Map name", max_length=128, default="")


class Game(BaseModel):
    map = ForeignKeyField(Map, backref="games")
    min_elo = IntegerField(default=-1, index=True)
    average_elo = IntegerField(default=-1, index=True)
    max_elo = IntegerField(default=-1, index=True)
    time = DateTimeField(default=datetime.datetime.fromtimestamp(0), index=True)
    is_finished = BooleanField(default=False)
    trackmaster_limit = IntegerField(default=999999)
    rounds = IntegerField(null=True)

    class Meta:
        ordering = ("-id",)
        indexes = ((("id",), True),)  # Crée un index UNIQUE sur le champ id


class Zone(BaseModel):
    uuid = peewee.UUIDField(unique=True)
    name = TextField()
    parent = ForeignKeyField("self", backref="children", null=True)
    country_alpha3 = CharField(max_length=10, null=True)
    file_name = TextField()


class Player(BaseModel):
    uuid = peewee.UUIDField(primary_key=True)
    name = TextField(default="")
    points = IntegerField(default=0, index=True)
    rank = IntegerField(default=99999, index=True)
    last_points_update = DateTimeField(default=datetime.datetime.now)
    last_name_update = DateTimeField(default=datetime.datetime.fromtimestamp(0))
    last_match = ForeignKeyField(Game, backref="players", null=True)
    zone = ForeignKeyField(Zone, backref="players", null=True)
    country = ForeignKeyField(Zone, backref="player_countries", null=True)
    club_tag = CharField(default=None, null=True, max_length=64)

    class Meta:
        ordering = ("-points",)

    def __str__(self):
        return f"{self.name} ({self.points} pts)"


class PlayerGame(BaseModel):
    game = ForeignKeyField(Game, backref="player_games")
    player = ForeignKeyField(Player, backref="player_games")
    is_win = BooleanField(default=False)
    is_mvp = BooleanField(default=False)
    points = IntegerField(null=True)
    position = IntegerField(null=True)
    points_after_match = IntegerField(null=True)
    rank_after_match = IntegerField(null=True)


class Season(BaseModel):
    name = CharField(max_length=32)
    start_time = DateTimeField()
    end_time = DateTimeField()
    is_aggregated = BooleanField(default=False)

    @classmethod
    def get_current_season(cls):
        now = datetime.datetime.now()
        return cls.select().where(Season.start_time <= now, Season.end_time >= now)[0]


class PlayerSeason(BaseModel):
    season = ForeignKeyField(Season, backref="player_seasons")
    player = ForeignKeyField(Player, backref="player_seasons")
    points = IntegerField(default=0, index=True)
    rank = IntegerField(default=99999, index=True)


class RankStatRollup(BaseModel):
    start_time = DateTimeField(index=True)
    end_time = DateTimeField(index=True)
    period = IntegerField(default=0, index=True)
    rank = IntegerField(default=0, index=True)
    count = IntegerField(default=0)
    last_game_time = DateTimeField(default=datetime.datetime.fromtimestamp(0), null=True)
