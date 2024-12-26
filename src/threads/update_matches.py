import time
import traceback
from datetime import datetime, timedelta

from models import Game
from settings import db
from src.log_utils import create_logger
from src.services import NadeoLive
from src.types import NadeoParticipant, NadeoMatchTeam
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_matches")


class UpdateMatchesThread(AbstractThread):
    """
    Checks ongoing matches and fill the information about scores, win, ... if the match is completed
    Note: Only checks matches that are at least 4 minutes old
    """

    def run_iteration(self):
        logger.info("Getting unfinished matches...")
        matches = Game.select(Game).where(
            Game.is_finished == False,
            Game.time < datetime.now() - timedelta(minutes=4),
        )
        # we ignore games newer than 4 minutes ago. There is almost no chance they are finished
        logger.info(f"Found {len(matches)} non terminated matches")
        for match in matches:
            logger.info(
                f"Starting update procedure for match id {match.id}",
                extra={"match": match},
            )
            self.update_match(match)

    @staticmethod
    def update_players_match(match: Game, participants: list[NadeoParticipant], teams: list[NadeoMatchTeam]):
        """
        Update players individual performances
        :param match:
        :param participants:
        :param teams:
        :return:
        """
        participants = {p["participant"]: p for p in participants}

        teams = {p["position"]: p for p in teams}
        for p in match.player_games:
            logger.info(f"Match id {match.id}, player {p.player} : Updating info")
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

    def complete_match(self, match: Game):
        """
        Update db match info and starts the update of db player
        individual performances
        :param match:
        :return:
        """
        logger.info(f"Match {match.id} is completed")
        teams = NadeoLive.get_match_teams(match.id)
        logger.info(
            f"Terminating match id {match.id}",
            {"response": teams},
        )
        match.is_finished = True
        match.rounds = sum(t["score"] for t in teams)
        match.save()

        logger.info(f"Fetching participants results for match id {match.id}")
        participants = NadeoLive.get_match_participants(match.id)
        logger.info(
            f"Participants response for match id {match.id}",
            {"response": participants},
        )

        self.update_players_match(match, participants, teams)

    def update_match(self, match: Game):
        """
        Check if match is completed. If it is, we update the info
        :param match:
        :return:
        """
        try:
            with db.atomic():
                logger.info(f"Get match data for match id {match.id}")
                nadeo_match = NadeoLive.get_match(match.id)
                logger.info(f"Match response {match.id}", {"response": nadeo_match})
                if nadeo_match["status"] == "COMPLETED":
                    self.complete_match(match)
        except Exception as e:
            logger.error(
                f"Error while updating match with id {match.id}",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )
            if "lock" in str(e).lower():
                logger.warning(f"Previous error for match id {match.id} was a deadlock. We'll restart the transaction")
                time.sleep(1)
                return self.update_match(match)  # We restart the function

    def handle(self):
        while True:
            try:
                self.run_iteration()
            except Exception as e:
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 30s before updating next matches...")
            time.sleep(30)
