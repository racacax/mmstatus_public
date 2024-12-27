from datetime import datetime

from models import Game

snapshot = Snapshot()

def migrate_forward(op, old_orm, new_orm):
    """
    Migration to reinstate some matches that have been wrongly
    updated + might be bugged bcs of server crash
    These matches will either be correctly updated or deleted
    """
    print("Updating old matches")

    update_query = Game.update(is_finished=False).where(
        (Game.is_finished == True) &
        (Game.rounds == 0) &
        (Game.time > datetime.fromtimestamp(1734204680)) &
        (Game.time < datetime.fromtimestamp(1735404680))
    )
    rows_updated = update_query.execute()

    print(f"{rows_updated} matches updated")

def migrate_backward(op, old_orm, new_orm):
    pass