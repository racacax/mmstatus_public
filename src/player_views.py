from datetime import datetime
from uuid import UUID

from peewee import Case, fn

from models import Player, PlayerGame, Game, Map, PlayerSeason, Season
from src.utils import Option, route, RouteDescriber


class PlayerAPIViews(RouteDescriber):
    tags = ["Player"]
    prefix = "player/"

    @staticmethod
    @route(
        name="map_statistics",
        summary="Map statistics",
        description="Returns statistics of a selected player on different maps based on order_by and order parameters.",
    )
    def get_map_statistics(
        player: Option(UUID, description="Unique UUID identifying a player") = UUID(
            "84078894-bae1-4399-b869-7b42a5240f02"
        ),
        order_by: Option(
            str,
            description="With which field should data be sorted",
            enum=["played", "losses", "wins", "lossrate", "winrate", "mvps", "mvprate"],
        ) = "played",
        order: Option(
            str,
            description="Whether sorting should be ascending or descending",
            enum=["asc", "desc"],
        ) = "desc",
        page: int = 1,
        min_date: Option(
            int,
            description="Unix timestamp representing the start date of filtered data",
        ) = 0,
        max_date: Option(
            int,
            description="Unix timestamp representing the end date of filtered data",
            formatted_default="<current timestamp>",
        ) = None,
    ):
        min_date = datetime.fromtimestamp(min_date)
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())
        uid = Map.uid.alias("map_uid")
        wins = fn.SUM(PlayerGame.is_win)
        mvps = fn.SUM(PlayerGame.is_mvp)
        losses = fn.SUM(Case(None, [((PlayerGame.is_win == False), 1)], 0))
        played = fn.COUNT(Map.uid)
        winrate = (wins * 100 / played).alias("winrate")
        lossrate = (losses * 100 / played).alias("lossrate")
        mvprate = (mvps * 100 / played).alias("mvprate")
        if order_by == "losses":
            order_by = losses
        elif order_by == "wins":
            order_by = wins
        elif order_by == "lossrate":
            order_by = lossrate
        elif order_by == "winrate":
            order_by = winrate
        elif order_by == "mvps":
            order_by = mvps
        elif order_by == "mvprate":
            order_by = mvprate
        else:
            order_by = played

        if order == "desc":
            order_by = order_by.desc()
        else:
            order_by = order_by.asc()

        query = (
            Player.select(
                Map.name.alias("map_name"),
                uid,
                wins.alias("wins"),
                losses.alias("losses"),
                played.alias("played"),
                mvps.alias("mvps"),
                winrate,
                lossrate,
                mvprate,
            )
            .join(PlayerGame)
            .join(Game)
            .join(Map)
            .where(
                Player.uuid == player,
                Game.time >= min_date,
                Game.time <= max_date,
            )
            .group_by(uid)
            .order_by(order_by)
            .paginate(page, 10)
            .dicts()
        )
        data = []
        for q in query:
            data.append(
                {
                    "played": int(q["played"] or 0),
                    "wins": int(q["wins"] or 0),
                    "losses": int(q["losses"] or 0),
                    "mvps": int(q["mvps"] or 0),
                    "lossrate": float(q["lossrate"] or 0),
                    "winrate": float(q["winrate"] or 0),
                    "mvprate": float(q["mvprate"] or 0),
                    "map_uid": q["map_uid"],
                    "map_name": q["map_name"],
                }
            )
        return 200, {"results": data, "player": player}

    @staticmethod
    @route(
        name="points_evolution",
        summary="Points evolution",
        description="Returns points (elo) evolution of a selected player between two dates",
    )
    def get_points_evolution(
        player: Option(UUID, description="Unique UUID identifying a player") = UUID(
            "84078894-bae1-4399-b869-7b42a5240f02"
        ),
        min_date: Option(
            int,
            description="Unix timestamp representing the start date of filtered data",
        ) = 0,
        max_date: Option(
            int,
            description="Unix timestamp representing the end date of filtered data",
            formatted_default="<current timestamp>",
        ) = None,
    ):
        min_date = datetime.fromtimestamp(min_date)
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())

        query = (
            PlayerGame.select(Game.time, PlayerGame.points_after_match)
            .join(Game)
            .where(
                PlayerGame.player_id == player,
                PlayerGame.points_after_match != None,
                Game.time >= min_date,
                Game.time <= max_date,
            )
            .order_by(Game.time.asc())
            .dicts()
        )

        return 200, {
            "results": list(
                map(
                    lambda x: {
                        "time": x["time"].timestamp(),
                        "points": x["points_after_match"],
                    },
                    query.execute(),
                )
            ),
            "player": player,
        }

    @staticmethod
    @route(
        name="rank_evolution",
        summary="Rank evolution",
        description="Returns leaderboard position evolution of a selected player between two dates",
    )
    def get_rank_evolution(
        min_date: Option(
            int,
            description="Unix timestamp representing the start date of filtered data",
        ) = 0,
        max_date: Option(
            int,
            description="Unix timestamp representing the end date of filtered data",
            formatted_default="<current timestamp>",
        ) = None,
        player: Option(UUID, description="Unique UUID identifying a player") = UUID(
            "84078894-bae1-4399-b869-7b42a5240f02"
        ),
    ):
        min_date = datetime.fromtimestamp(min_date)
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())

        query = (
            PlayerGame.select(Game.time, PlayerGame.rank_after_match)
            .join(Game)
            .where(
                PlayerGame.player_id == player,
                PlayerGame.rank_after_match != None,
                Game.time >= min_date,
                Game.time <= max_date,
            )
            .order_by(Game.time.asc())
            .dicts()
        )

        return 200, {
            "results": list(
                map(
                    lambda x: {
                        "time": x["time"].timestamp(),
                        "rank": x["rank_after_match"],
                    },
                    query.execute(),
                )
            ),
            "player": player,
        }

    @staticmethod
    @route(
        name="opponents_statistics",
        summary="Opponents statistics",
        description="Returns statistics of a selected player performance with players they played against/along.",
    )
    def get_opponents_statistics(
        player: Option(UUID, description="Unique UUID identifying a player") = UUID(
            "84078894-bae1-4399-b869-7b42a5240f02"
        ),
        order_by: Option(
            str,
            description="With which field should the data be sorted",
            enum=[
                "most_played",
                "played_against",
                "played_along",
                "games_lost_against",
                "games_won_against",
                "games_lost_along",
                "games_won_along",
            ],
        ) = "most_played",
        order: Option(
            str,
            description="Whether sorting should be ascending or descending",
            enum=["asc", "desc"],
        ) = "desc",
        page: int = 1,
        min_date: Option(
            int,
            description="Unix timestamp representing the start date of filtered data",
        ) = 0,
        max_date: Option(
            int,
            description="Unix timestamp representing the end date of filtered data",
            formatted_default="<current timestamp>",
        ) = None,
    ):
        min_date = datetime.fromtimestamp(min_date)
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())
        Opponent = Player.alias("opponent")
        OpponentGame = PlayerGame.alias("pg2")
        total_played = fn.COUNT(Opponent.uuid)
        total_played_against = fn.SUM(Case(None, [((PlayerGame.is_win != OpponentGame.is_win), 1)], 0))
        total_played_along = fn.SUM(Case(None, [((PlayerGame.is_win == OpponentGame.is_win), 1)], 0))
        total_games_lost_against = fn.SUM(
            Case(
                None,
                [(((PlayerGame.is_win == False) & (OpponentGame.is_win == True)), 1)],
                0,
            )
        )
        total_games_won_against = fn.SUM(
            Case(
                None,
                [(((OpponentGame.is_win == False) & (PlayerGame.is_win == True)), 1)],
                0,
            )
        )
        total_games_lost_along = fn.SUM(
            Case(
                None,
                [(((PlayerGame.is_win == False) & (OpponentGame.is_win == False)), 1)],
                0,
            )
        )
        total_games_won_along = fn.SUM(
            Case(
                None,
                [(((OpponentGame.is_win == True) & (PlayerGame.is_win == True)), 1)],
                0,
            )
        )

        if order_by == "played_against":
            order_by = total_played_against
        elif order_by == "played_along":
            order_by = total_played_along
        elif order_by == "games_lost_against":
            order_by = total_games_lost_against
        elif order_by == "games_won_against":
            order_by = total_games_won_against
        elif order_by == "games_lost_along":
            order_by = total_games_lost_along
        elif order_by == "games_won_along":
            order_by = total_games_won_along
        else:
            order_by = total_played

        if order == "desc":
            order_by = order_by.desc()
        else:
            order_by = order_by.asc()

        query = (
            Player.select(
                Opponent.uuid,
                Opponent.name,
                total_played.alias("total_played"),
                total_played_against.alias("total_played_against"),
                total_played_along.alias("total_played_along"),
                total_games_lost_against.alias("total_games_lost_against"),
                total_games_won_against.alias("total_games_won_against"),
                total_games_lost_along.alias("total_games_lost_along"),
                total_games_won_along.alias("total_games_won_along"),
            )
            .join(PlayerGame)
            .join(Game)
            .join(OpponentGame)
            .join(Opponent)
            .where(
                Player.uuid == player,
                Game.time >= min_date,
                Game.time <= max_date,
            )
            .group_by(Opponent.uuid)
            .order_by(order_by)
            .paginate(page, 10)
            .dicts()
        )
        data = []
        for q in query:
            if q["uuid"] == player:
                continue
            data.append(
                {
                    "uuid": str(q["uuid"]),
                    "name": q["name"],
                    "total_played": q["total_played"],
                    "total_played_against": int(q["total_played_against"]),
                    "total_played_along": int(q["total_played_along"]),
                    "total_games_lost_against": int(q["total_games_lost_against"]),
                    "total_games_won_against": int(q["total_games_won_against"]),
                    "total_games_lost_along": int(q["total_games_lost_along"]),
                    "total_games_won_along": int(q["total_games_won_along"]),
                }
            )
        return 200, {"results": data, "player": player}

    @staticmethod
    @route(
        name="statistics",
        summary="General statistics",
        description="Returns basic statistics on a selected player (points, position, matches played, ...)",
    )
    def get_statistics(
        player: Option(UUID, description="Unique UUID identifying a player") = UUID(
            "84078894-bae1-4399-b869-7b42a5240f02"
        ),
        min_date: Option(
            int,
            description="Unix timestamp representing the start date of filtered data",
        ) = 0,
        max_date: Option(
            int,
            description="Unix timestamp representing the end date of filtered data",
            formatted_default="<current timestamp>",
        ) = None,
        season: Option(
            int,
            description="ID representing a season (provided by the seasons endpoint)",
            formatted_default="<current season>",
        ) = -1,
    ):
        selected_season = None
        show_season_points = True
        if min_date or max_date:
            show_season_points = False
            min_date = datetime.fromtimestamp(min_date)
            max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())
            selected_season = Season.filter(Season.start_time <= min_date, Season.end_time >= max_date).get_or_none()
        elif season:
            selected_season = Season.get(id=season)
            min_date = selected_season.start_time
            max_date = selected_season.end_time

        if not selected_season:
            return 404, {"message": "Cannot retrieve any season with these filters"}

        total_played = fn.COUNT(PlayerGame.id)
        total_wins = fn.SUM(PlayerGame.is_win)
        total_losses = fn.SUM(Case(None, [((PlayerGame.is_win == False), 1)], 0))
        total_mvp = fn.SUM(PlayerGame.is_mvp)

        query = (
            PlayerSeason.select(
                Player.uuid,
                Player.name,
                Player.club_tag,
                PlayerSeason.rank,
                PlayerSeason.points,
                total_played.alias("total_played"),
                total_wins.alias("total_wins"),
                total_losses.alias("total_losses"),
                total_mvp.alias("total_mvp"),
            )
            .join(Player)
            .join(PlayerGame)
            .join(Game)
            .where(
                Player.uuid == player,
                Game.time >= min_date,
                Game.time <= max_date,
                PlayerSeason.season == selected_season,
            )
            .group_by(Player.uuid)
            .dicts()
        )
        if len(query) > 0:
            data = query[0]
            points = data["points"]
            if not show_season_points:
                # Get player points at the time of the filter
                last_match = (
                    PlayerGame.select(PlayerGame.points_after_match)
                    .join(Game)
                    .switch(PlayerGame)
                    .join(Player)
                    .where(
                        Player.uuid == player,
                        Game.time >= selected_season.start_time,
                        # maybe player didn't play any match during the filtered time
                        Game.time <= max_date,
                    )
                    .order_by(PlayerGame.id.desc())
                    .paginate(1, 1)
                    .get_or_none()
                )
                if last_match:
                    points = last_match.points_after_match
            return 200, {
                "uuid": str(data["uuid"]),
                "name": data["name"],
                "club_tag": data["club_tag"],
                "stats": {
                    "season": selected_season.name,
                    "rank": data["rank"],
                    "points": points,
                    "total_played": int(data["total_played"]),
                    "total_wins": int(data["total_wins"]),
                    "total_losses": int(data["total_losses"]),
                    "total_mvp": int(data["total_mvp"]),
                },
            }
        else:
            return 404, {"message": "Current player either doesn't exist or didn't play this season"}
