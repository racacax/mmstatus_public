# auto-generated snapshot
import os
import sys

from src.services import NadeoCore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from playhouse.migrate import MySQLMigrator

from models import Zone
from settings import db

migrator = MySQLMigrator(db)


def migrate():
    zones = NadeoCore.get_zones()
    zs = []
    with db.atomic():
        for zone in zones:
            file_name = zone["icon"].split("Flags/")[1].replace(".dds", ".jpg")
            z = Zone.create(uuid=zone["zoneId"], file_name=file_name, name=zone["name"])
            zs.append([z, zone])
            print("adding", zone["name"])

        for [z_obj, z_ret] in zs:
            print("setting parent", z_ret["name"], z_ret["parentId"])
            if z_ret["parentId"] is not None:
                z_obj.parent = Zone.get(uuid=z_ret["parentId"])
                z_obj.save()


if __name__ == "__main__":
    migrate()
