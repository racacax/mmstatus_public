# auto-generated snapshot
from peewee import *
import datetime
import peewee


snapshot = Snapshot()


@snapshot.append
class Command(peewee.Model):
    guild_id = BigIntegerField()
    channel_ids = TextField(default='')
    command = CharField(max_length=255)
    response = TextField()
    class Meta:
        table_name = "command"


