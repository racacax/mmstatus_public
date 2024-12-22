import time
import traceback
from datetime import datetime
from typing import Union

import settings
from threads.abstract_thread import AbstractThread
from models import PlayerGame, Game, Map, Player
from src.log_utils import create_logger
from src.services import NadeoLive
from src.types import NadeoMatch, NadeoParticipant
from src.utils import get_trackmaster_limit

logger = create_logger("get_matches")


class GetMatchesThread(AbstractThread):
    def __init__(self):
        """
        Get last match id in the database. Will set the current id to last inserted id + 1
        If a custom START_ID is provided, this one will be used if higher (+1)
        """
        last_match = Game.select().order_by(Game.id.desc()).paginate(1, 1).get_or_none()
        logger.info(f"Starting get_matches thread. Last match: {last_match}")
        match_id = (last_match and last_match.id or settings.START_ID) + 1
        self.match_id = max(match_id, settings.START_ID)

    def get_match(self) -> Union[NadeoMatch, None]:
        """
            Return current Nadeo match.
            Returns None if API threw an exception
        :return:
        """
        match = NadeoLive.get_match(self.match_id)
        if "exception" in match:
            logger.warning(
                f"Exception while getting match id {self.match_id}. Most likely reached last match id.",
                extra={"response": match},
            )
            return None
        logger.info(f"Get match response for id {self.match_id}", extra={"response": match})
        return match

    def get_match_info(self, match: NadeoMatch) -> tuple[int, list[NadeoParticipant], Map]:
        """
            Get last Trackmaster current points, match participants, and current database Map
        :param match: Nadeo match
        :return:
        """
        logger.info(f"{match['name']} is a valid match. Getting participants...")
        participants = NadeoLive.get_match_participants(self.match_id)
        logger.info(
            f"Get participants response for match {self.match_id}",
            extra={"response": participants},
        )
        match_map, _ = Map.get_or_create(uid=match["publicConfig"]["maps"][0])
        tm_limit = get_trackmaster_limit()
        return tm_limit, participants, match_map

    @staticmethod
    def get_or_create_players(participants: list[NadeoParticipant], match_o: Game) -> list[Player]:
        """
            Will get database Player objects related to the participants of the match.
            If they don't exist, they will be created.
            Current match will be affected to their last match
        :param participants: Match participants (Nadeo objects)
        :param match_o: Database match object
        :return:
        """
        players_o = []
        for p in participants:
            logger.info(f"Getting/creating player with id {p['participant']}")
            player, created = Player.get_or_create(uuid=p["participant"])
            player.last_match = match_o
            players_o.append(player)
            if created:
                player.last_points_update = datetime.fromtimestamp(0)
            player.save()
        return players_o

    def insert_match(self) -> bool:
        """
            Fetch and insert a match in the database according to current id.
            Will return True when :
                - Unknown failure
                - Royal match found (just skips)
                - Match insertion succeeded
            Will return False when:
                - API threw an exception while getting match. It probably means match didn't exist.
        :return:
        """
        try:
            with settings.db.atomic():
                logger.info(f"Adding new match with id {self.match_id}")
                match = self.get_match()
                if not match:
                    return False
                name: str = match["name"]
                if "Official 3v3" in name:
                    tm_limit, participants, match_map = self.get_match_info(match)
                    logger.info(f"Creating match with id {match['id']}")
                    match_o = Game.create(
                        id=match["id"],
                        time=datetime.fromtimestamp(match["startDate"]),
                        is_finished=match["status"] == "COMPLETED",
                        map=match_map,
                        trackmaster_limit=tm_limit,
                    )

                    players_o = self.get_or_create_players(participants, match_o)

                    # Link players to current match with intermediate table
                    for p in players_o:
                        PlayerGame.create(game=match_o, player=p)
                else:
                    logger.info(f"Got match name {name} which is not a valid Matchmaking name. Skipping...")
                    return True
        except Exception as e:
            logger.error(
                f"Exception while creating match with id {self.match_id}",
                extra={"error": e, "traceback": traceback.format_exc()},
            )
            if "lock" in str(e).lower():
                logger.warning(
                    f"Previous error for match id {self.match_id} was a deadlock. We'll restart the transaction"
                )
                time.sleep(1)
                return self.insert_match()
        return True

    def run_insert_matches_loop(self):
        """
        Will insert matches while incrementing id by 1 succeeds
        :return:
        """
        while self.insert_match():
            self.match_id += 1

    def handle(self):
        while True:
            self.run_insert_matches_loop()
            logger.info("Waiting 30s before fetching new matches")
            time.sleep(30)
