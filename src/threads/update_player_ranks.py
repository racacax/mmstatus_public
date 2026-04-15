import time
import traceback
from datetime import datetime, timedelta

from peewee import JOIN

from models import Player, PlayerGame, Season, PlayerSeason, Game
from src.log_utils import create_logger
from src.services import NadeoLive
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_player_ranks")


class UpdatePlayerRanksThread(AbstractThread):
    """
    Will fetch players with points and position not updated in 12 hours
    and update them.
    Notes:
        We ignore players with 0 points except if they have never been updated.
        We check the oldest entries first

    """

    def update_player(self, p: Player, points: int, rank: int, season: Season) -> None:
        try:
            p.points = points
            p.rank = rank
            p.last_points_update = datetime.now()
            p.save()

            if points > 0:
                ps, _ = PlayerSeason.get_or_create(player=p, season=season)
                ps.points = p.points
                ps.rank = p.rank
                ps.save()
            if p.last_match and p.last_match.is_finished:
                pg = PlayerGame.get(game=p.last_match, player=p)
            else:
                # If new match already started, need to look for previous matches
                pg = (
                    PlayerGame.select(PlayerGame, Game)
                    .join(Game, JOIN.LEFT_OUTER)
                    .where(Game.is_finished == True, PlayerGame.player_id == p.uuid)
                    .order_by(PlayerGame.id.desc())
                    .get_or_none()
                )

            if pg and pg.points_after_match is None:
                game_season = (
                    Season.select()
                    .where(Season.start_time <= pg.game.time, Season.end_time >= pg.game.time)
                    .get_or_none()
                )
                if game_season and game_season.id != season.id:
                    # In case of season switch.
                    prev_ps = PlayerSeason.get_or_none(player=p, season=game_season)
                    pg.points_after_match = prev_ps.points if prev_ps else p.points
                    pg.rank_after_match = prev_ps.rank if prev_ps else p.rank
                else:
                    pg.points_after_match = p.points
                    pg.rank_after_match = p.rank
                pg.save()
        except Exception as e:
            self._record_error()
            logger.error(
                "Error while updating player rank",
                extra={
                    "exception": e,
                    "traceback": traceback.format_exc(),
                    "player": p,
                },
            )

    MAX_POINTS_FETCH_RETRIES = 20

    def update_players(self, players: list[Player], season: Season):
        try:
            ids = [str(p.uuid) for p in players]
            logger.info("Updating players", extra={"ids": ids})
            result = NadeoLive.get_player_ranks(ids)
            logger.info("get_player_ranks response", extra={"response": result})
            scores = {p["player"]: p["score"] for p in result["results"]}
            ranks = {p["player"]: p["rank"] for p in result["results"]}
            for p in players:
                pid = str(p.uuid)
                if pid not in scores:
                    p.points_fetch_retries += 1
                    if p.points_fetch_retries >= self.MAX_POINTS_FETCH_RETRIES:
                        logger.warning(
                            "Player absent from API after max retries, setting points to 0",
                            extra={"player": p, "retries": p.points_fetch_retries},
                        )
                        p.points_fetch_retries = 0
                        self.update_player(p, 0, 0, season)
                    else:
                        logger.info(
                            "Player absent from API, will retry",
                            extra={"player": p, "retries": p.points_fetch_retries},
                        )
                        p.last_points_update = datetime.now()
                        p.save()
                else:
                    p.points_fetch_retries = 0
                    self.update_player(p, scores[pid], ranks[pid], season)
        except Exception as e:
            self._record_error()
            logger.error(
                "Error while updating players ranks",
                extra={
                    "exception": e,
                    "traceback": traceback.format_exc(),
                },
            )

    def run_iteration(self):
        logger.info("Getting players with outdated points (oldest 100)")
        season = Season.get_current_season()
        players = (
            Player.select(Player)
            .where(
                (Player.last_points_update < datetime.now() - timedelta(hours=12))
                & (
                    (Player.points != 0)
                    | (Player.last_points_update == datetime.fromtimestamp(0))
                    | (Player.points_fetch_retries > 0)
                )
            )
            .order_by(Player.last_points_update.asc())
            .paginate(1, 100)
        )
        count = len(players)
        logger.info(f"Found {count} players")
        if count > 0:
            self.update_players(players, season)

    def handle(self):
        while True:
            try:
                self.run_iteration()
            except Exception as e:
                self._record_error()
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            logger.info("Waiting 10s before starting thread again...")
            time.sleep(10)
