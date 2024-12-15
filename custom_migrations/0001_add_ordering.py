# auto-generated snapshot
from peewee import *
import datetime
import peewee
from playhouse.migrate import MySQLMigrator

from settings import db

migrator = MySQLMigrator(db)


def migrate():
    # Ajouter la clause ORDER BY DESC sur le champ ID
    db.execute_sql("ALTER TABLE game ORDER BY id DESC;")
    db.execute_sql("ALTER TABLE player ORDER BY points DESC;")


if __name__ == '__main__':
    migrate()
