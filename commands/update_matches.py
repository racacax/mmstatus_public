import time
import traceback
from datetime import datetime, timedelta

from models import Game
from settings import db
from src.log_utils import create_logger
from src.services import NadeoLive

logger = create_logger("update_matches")


def update_matches():
    while True:
        try:
            logger.info("Getting unfinished matches...")
            games = Game.select(Game).where(
                Game.is_finished == False,
                Game.time < datetime.now() - timedelta(minutes=4),
            )
            # we ignore games newer than 4 minutes ago. There is almost no chance they are finished
            logger.info(f"Found {len(games)} non terminated games")
            for game in games:
                logger.info(
                    f"Starting update procedure for match id {game.id}",
                    extra={"game": game},
                )
                while True:
                    try:
                        with db.atomic():
                            logger.info(f"Get match data for match id {game.id}")
                            match = NadeoLive.get_match(game.id)
                            logger.info(f"Match response {game.id}", {"response": match})
                            if match["status"] == "COMPLETED":
                                logger.info(f"Match {game.id} is completed")
                                teams = NadeoLive.get_match_teams(game.id)
                                logger.info(
                                    f"Terminating match id {game.id}",
                                    {"response": teams},
                                )
                                game.is_finished = True
                                if len(teams) > 0:
                                    game.rounds = teams[0]["score"]
                                if len(teams) > 1:
                                    game.rounds += teams[1]["score"]
                                game.save()

                                logger.info(f"Fetching participants results for match id {game.id}")
                                participants = NadeoLive.get_match_participants(game.id)
                                logger.info(
                                    f"Participants response for match id {game.id}",
                                    {"response": participants},
                                )
                                participants = {p["participant"]: p for p in participants}

                                teams = {p["position"]: p for p in teams}
                                for p in game.player_games:
                                    logger.info(f"Match id {game.id}, player {p.player} : Updating info")
                                    player = p.player
                                    p.is_mvp = participants.get(str(player.uuid), {"mvp": None})["mvp"]
                                    p.is_win = (
                                        teams.get(
                                            participants.get(str(player.uuid), {"teamPosition": 0})["teamPosition"],
                                            {"rank": 1},
                                        )["rank"]
                                    ) == 1
                                    p.position = participants.get(str(player.uuid), {"position": None})["position"]
                                    if p.position is not None:
                                        p.position += 1
                                    p.points = participants.get(str(player.uuid), {"score": None})["score"]
                                    p.save()
                                    player.last_points_update = datetime.fromtimestamp(0)
                                    player.save()
                        break
                    except Exception as e:
                        logger.error(
                            f"Error while updating match with id {game.id}",
                            extra={"exception": e, "traceback": traceback.format_exc()},
                        )
                        if "lock" in str(e).lower():
                            logger.warning(
                                f"Previous error for match id {game.id} was a deadlock. We'll restart the transaction"
                            )
                            time.sleep(1)
                            continue
                        break
        except Exception as e2:
            logger.error(
                "General error in the thread",
                extra={"exception": e2, "traceback": traceback.format_exc()},
            )
        logger.info("Waiting 30s before updating next matches...")
        time.sleep(30)
