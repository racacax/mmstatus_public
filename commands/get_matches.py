import time
import traceback
from datetime import datetime

import settings
from models import PlayerGame, Game, Map, Player
from src.log_utils import create_logger
from src.services import NadeoLive


logger = create_logger("get_matches")


def get_matches():
    last_game = Game.select().order_by(Game.id.desc()).paginate(1, 1).get_or_none()
    logger.info(f"Starting get_matches thread. Last match: {last_game}")
    id = (last_game and last_game.id or settings.START_ID) + 1
    id = max(id, settings.START_ID)
    while True:
        while True:
            try:
                with settings.db.atomic():
                    logger.info(f"Adding new match with id {id}")
                    match = NadeoLive.get_match(id)
                    if "exception" in match:
                        logger.warning(
                            f"Exception while getting match id {id}. Most likely reached last match id.",
                            extra={"response": match},
                        )
                        break
                    logger.info(
                        f"Get match response for id {id}", extra={"response": match}
                    )
                    name: str = match["name"]
                    if "Official 3v3" in name:
                        logger.info(f"{name} is a valid match. Getting participants...")
                        participants = NadeoLive.get_match_participants(id)
                        logger.info(
                            f"Get participants response for match {id}",
                            extra={"response": participants},
                        )
                        players_o = []
                        map, _ = Map.get_or_create(uid=match["publicConfig"]["maps"][0])
                        f_trackmaster = (
                            Player.select(Player.points)
                            .where(Player.rank <= 10, Player.points >= 4000)
                            .order_by(Player.rank.desc())
                        )

                        if len(f_trackmaster) > 0:
                            tm_limit = f_trackmaster[0].points
                        else:
                            tm_limit = 99999
                        logger.info(f"Creating match with id {match['id']}")
                        g = Game.create(
                            id=match["id"],
                            time=datetime.fromtimestamp(match["startDate"]),
                            is_finished=match["status"] == "COMPLETED",
                            map=map,
                            trackmaster_limit=tm_limit,
                        )
                        for p in participants:
                            logger.info(
                                f"Getting/creating player with id {p['participant']}"
                            )
                            player, created = Player.get_or_create(
                                uuid=p["participant"]
                            )
                            player.last_match = g
                            players_o.append(player)
                            if created:
                                player.last_points_update = datetime.fromtimestamp(0)
                            player.save()

                        for p in players_o:
                            PlayerGame.create(game=g, player=p)
                    else:
                        logger.info(
                            f"Got match name {name} which is not a valid Matchmaking name. Skipping..."
                        )
            except Exception as e:
                logger.error(
                    f"Exception while creating match with id {id}",
                    extra={"error": e, "traceback": traceback.format_exc()},
                )
                if "lock" in str(e).lower():
                    logger.warning(
                        f"Previous error for match id {id} was a deadlock. We'll restart the transaction"
                    )
                    time.sleep(1)
                    continue
            id += 1
        logger.info(f"Waiting 30s before fetching new matches")
        time.sleep(30)
