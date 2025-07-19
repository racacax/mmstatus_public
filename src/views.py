import json
from datetime import datetime, timedelta
from inspect import signature
from uuid import UUID

from peewee import Case, fn, JOIN

from models import Player, PlayerGame, Game, Map, Season, Zone
from src.player_views import PlayerAPIViews
from src.utils import Option, format_type, POINTS_TYPE, route, RouteDescriber, RANKS
from src.view_utils.status import get_rollup_stats, compute_remaining_stats, get_merged_stats


class APIViews(RouteDescriber):
    tags = ["Global"]
    prefix = ""

    @staticmethod
    @route(
        name="players",
        summary="Last active players",
        description="Returns the list of players ordered by last match time.",
    )
    def get_players(
        min_elo: Option(int, description="Minimum points of filtered players") = 0,
        max_elo: Option(int, description="Maximum points of filtered players") = 999999,
        max_rank: Option(int, description="Maximum of filtered players") = 999999,
        min_rank: Option(int, description="Minimum rank of filtered players") = 1,
        page: int = 1,
        name: Option(str, description="Search string for player name (case insensitive)") = "",
        compute_matches_played: Option(
            str,
            description="Indicate if we need to compute stats about matches played by players",
        ) = "true",
    ):
        compute_matches_played = compute_matches_played == "true"
        players = (
            Game.select(
                Player.name,
                Player.uuid,
                Player.rank,
                Player.points,
                Player.club_tag,
                Zone.name.alias("country_name"),
                Zone.country_alpha3,
                Zone.file_name,
                Game.id.alias("match_id"),
                Game.time,
                Game.is_finished,
            )
            .join(Player, JOIN.LEFT_OUTER)
            .join(Zone, JOIN.LEFT_OUTER, on=(Player.country_id == Zone.id), attr="country")
            .order_by(Game.id.desc())
            .where(
                Player.name.contains(name),
                Player.rank >= min_rank,
                Player.rank <= max_rank,
                Player.points >= min_elo,
                Player.points <= max_elo,
            )
            .paginate(page, 15)
            .dicts()
        )

        uuids = [p["uuid"] for p in players]
        now = datetime.now()
        if compute_matches_played:
            player_games = (
                PlayerGame.select(
                    PlayerGame.player_id,
                    fn.COUNT(Game.id).alias("total_last_month"),
                    fn.SUM(Case(None, [(Game.time > (now - timedelta(hours=24)), 1)], 0)).alias("total_24_hours"),
                    fn.SUM(Case(None, [(Game.time > (now - timedelta(days=7)), 1)], 0)).alias("total_1_week"),
                )
                .join(Game, JOIN.LEFT_OUTER)
                .where(PlayerGame.player_id << uuids, Game.time > now - timedelta(days=30))
                .group_by(PlayerGame.player_id)
                .dicts()
            )
            player_games = {str(p["player"]): p for p in player_games}
        else:
            player_games = {}
        data = []
        for p in players:
            data.append(
                {
                    "name": p["name"],
                    "uuid": str(p["uuid"]),
                    "club_tag": p["club_tag"],
                    "country": p["country_alpha3"]
                    and {
                        "name": p["country_name"],
                        "file_name": p["file_name"],
                        "alpha3": p["country_alpha3"],
                    },
                    "rank": p["rank"],
                    "points": p["points"],
                    "games_last_24_hours": int(
                        player_games.get(str(p["uuid"]), {"total_24_hours": 0})["total_24_hours"]
                    ),
                    "games_last_week": int(player_games.get(str(p["uuid"]), {"total_1_week": 0})["total_1_week"]),
                    "games_last_month": int(
                        player_games.get(str(p["uuid"]), {"total_last_month": 0})["total_last_month"]
                    ),
                    "last_active": p["match_id"] and p["time"].timestamp() or 0,
                    "last_game_finished": p["match_id"] and p["is_finished"],
                    "last_game_id": p["match_id"] and p["match_id"] or 0,
                    "last_match": p["match_id"]
                    and {
                        "id": p["match_id"],
                        "date": p["time"].timestamp(),
                        "is_finished": p["is_finished"],
                    },
                }
            )
        return 200, data

    @staticmethod
    @route(
        name="matches",
        summary="Last matches",
        description="Return a list of all the matches ordered by descending date.",
    )
    def get_matches(
        page: int = 1,
        min_elo: Option(int, description="Minimum points (elo) of worst player") = 0,
        max_elo: Option(int, description="Maximum points (elo) of best player") = 999999,
        min_average_elo: Option(int, description="Minimum points (elo) for the average of the match") = 0,
        max_average_elo: Option(int, description="Maximum points (elo) for the average of the match") = 999999,
    ):
        games = (
            Game.select(Game, Map)
            .join(Map, JOIN.LEFT_OUTER)
            .where(
                Game.average_elo > -1,
                Game.max_elo <= max_elo,
                Game.min_elo >= min_elo,
                Game.average_elo >= min_average_elo,
                Game.average_elo <= max_average_elo,
            )
            .order_by(Game.id.desc())
            .paginate(page, 15)
        )
        data = []
        for g in games:
            t: datetime = g.time
            data.append(
                {
                    "id": g.id,
                    "map": {"name": g.map.name, "uid": g.map.uid},
                    "time": t.timestamp(),
                    "min_elo": g.min_elo,
                    "max_elo": g.max_elo,
                    "average_elo": g.average_elo,
                    "is_finished": g.is_finished,
                    "trackmaster_points_limit": g.trackmaster_limit,
                }
            )
        return 200, data

    @staticmethod
    @route(
        name="games",
        description="Return a list of all the matches ordered by descending date. Same endpoint as matches.",
        summary="Last games",
        deprecated=True,
    )
    def get_games(
        page: int = 1,
        min_elo: Option(int, description="Minimum points (elo) of worst player") = 0,
        max_elo: Option(int, description="Maximum points (elo) of best player") = 999999,
        min_average_elo: Option(int, description="Minimum points (elo) for the average of the match") = 0,
        max_average_elo: Option(int, description="Maximum points (elo) for the average of the match") = 999999,
    ):
        return APIViews.get_matches(page, min_elo, max_elo, min_average_elo, max_average_elo)

    @staticmethod
    @route(
        name="computed_metric",
        summary="Computed metric",
        description="Returns computed metric data such has country stats, players activity, ...",
    )
    def get_computed(
        metric: Option(
            str,
            description="Which computed data to gather",
            enum=[
                "activity_per_country",
                "activity_per_hour",
                "activity_per_country_and_hour",
                "players_per_country",
                "rank_distribution",
                "top_100_per_country",
                "maps_statistics",
            ],
        ) = "activity_per_country",
        season: Option(
            int,
            description="ID representing a season (provided by the seasons endpoint)",
            formatted_default="<current season>",
        ) = -1,
        min_elo: Option(
            int,
            description="Minimum points (elo) for the selected metric",
            enum=POINTS_TYPE,
        ) = 0,
    ):
        if season == -1:
            season = Season.get_current_season().id

        if metric not in [
            "activity_per_country",
            "activity_per_hour",
            "activity_per_country_and_hour",
            "players_per_country",
            "activity_per_players_per_country",
            "rank_distribution",
            "top_100_per_country",
            "maps_statistics",
            "countries_leaderboard",
            "clubs_leaderboard",
        ]:
            return 404, {"message": f"'{metric}' metric doesn't exist."}
        if metric not in [
            "countries_leaderboard",
            "clubs_leaderboard",
            "rank_distribution",
            "maps_statistics",
        ]:
            min_elo_str = "_" + str(min_elo)
        else:
            min_elo_str = ""
        if metric in ["players_per_country", "rank_distribution"]:
            metric = "activity_per_" + metric
        f = open(
            "cache/get_" + metric + min_elo_str + "_" + str(season) + ".txt",
            "r",
        )
        content = f.read()
        f.close()

        return 200, json.loads(content)

    @staticmethod
    @route(
        name="leaderboard",
        summary="Top X players by a certain metric",
        description="Returns top X players of current season depending on a metric",
    )
    def get_leaderboard(
        metric: Option(
            str,
            description="Which computed data to gather",
            enum=[
                "country",
            ],
        ) = "country",
        metric_value: Option(
            str,
            description="Filters a value corresponding to the metric (ex : if you set metric as country and metric "
            "value as FRA, you'll get top 100 players form France).\n "
            " Valid values for each metric:\n - country : ISO-3166-alpha3 of a country (ex: FRA)",
        ) = "FRA",
        season: Option(
            int,
            description="ID representing a season (provided by the seasons endpoint)",
            formatted_default="<current season>",
        ) = -1,
    ):
        if season == -1:
            season = Season.get_current_season().id

        if metric not in [
            "country",
        ]:
            return 404, {"message": f"'{metric}' metric doesn't exist."}

        formatted_metric_value = metric_value.replace("/", "").replace("\\", "").replace("~", "")
        f = open(
            f"cache/top_100_by_{metric}/{season}/{formatted_metric_value}.txt",
            "r",
        )
        content = f.read()
        f.close()

        return 200, json.loads(content)

    @staticmethod
    @route(
        name="countries",
        summary="List of all zones which are countries",
        description="List of all zones which are countries",
    )
    def get_countries():
        data = Zone.select().where(Zone.country_alpha3 != None).dicts()
        return 200, {"results": [d for d in data]}

    @staticmethod
    @route(
        name="activity_per",
        summary="Activity per metric",
        description="Returns computed data such has country stats, players activity, ...",
        deprecated=True,
    )
    def get_activity_per(
        metric: Option(
            str,
            description="Which computed data to gather",
            enum=["country", "hour", "country_and_hour", "players_per_country"],
        ) = "country",
        season: Option(
            int,
            description="ID representing a season (provided by the seasons endpoint)",
            formatted_default="<current season>",
        ) = -1,
        min_elo: Option(
            int,
            description="Minimum points (elo) for the selected metric",
            enum=POINTS_TYPE,
        ) = 0,
    ):
        return APIViews.get_computed("activity_per_" + metric, season, min_elo)

    @staticmethod
    @route(
        name="status",
        summary="Status",
        description="Returns number of matches per rank between two dates.",
    )
    def get_status(
        min_date: Option(
            int,
            description="Unix timestamp representing the start date of filtered data",
            formatted_default="<1 hour ago timestamp>",
        ) = None,
        max_date: Option(
            int,
            description="Unix timestamp representing the end date of filtered data",
            formatted_default="<current timestamp>",
        ) = None,
    ):
        min_date = datetime.fromtimestamp(min_date or (datetime.now().timestamp() - 3600))
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())

        stats, remaining_condition = get_rollup_stats(min_date, max_date)
        if remaining_condition:
            remaining_stats = compute_remaining_stats(remaining_condition)
            stats = get_merged_stats(stats, remaining_stats)

        ranks = RANKS[::-1]
        final_stats = {stat["rank"]: stat for stat in stats}
        ranks = {r["id"]: r["key"] for r in ranks}
        # use key instead of id for returned payload (e.g. m3 will be used instead of 11 for Master III)
        return 200, {
            k: {
                "last_time": final_stats.get(id)
                and final_stats[id]["last_game_time"]
                and final_stats[id]["last_game_time"].timestamp(),
                "count": (final_stats.get(id) and final_stats[id]["count"]) or 0,
            }
            for id, k in ranks.items()
        }

    @staticmethod
    @route(
        name="seasons",
        summary="Seasons",
        description="Returns list of all matchmaking seasons since Spring 2024",
    )
    def get_seasons():
        return 200, {"results": [s for s in Season.filter().dicts()]}

    @staticmethod
    @route(
        name="player_map_statistics",
        summary="Player map statistics",
        description="Returns statistics of a selected player on different maps based on order_by and order parameters.",
        deprecated=True,
    )
    def get_player_map_statistics(
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
        return PlayerAPIViews.get_map_statistics(player, order_by, order, page, min_date, max_date)

    @staticmethod
    @route(
        name="player_points",
        summary="Player points evolution",
        description="Returns points (elo) evolution of a selected player between two dates",
        deprecated=True,
    )
    def get_player_points(
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
        return PlayerAPIViews.get_points_evolution(player, min_date, max_date)

    @staticmethod
    @route(
        name="player_ranks",
        summary="Player rank evolution",
        description="Returns leaderboard position evolution of a selected player between two dates",
        deprecated=True,
    )
    def get_player_ranks(
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
        return PlayerAPIViews.get_rank_evolution(min_date, max_date, player)

    @staticmethod
    @route(
        name="players_statistics",
        summary="Players statistics",
        description="Returns top 100 players of the current season depending on the order_by parameter.",
    )
    def get_players_statistics(
        season: Option(
            int,
            description="ID representing a season (provided by the seasons endpoint)",
            formatted_default="<current season>",
        ) = -1,
        order_by: Option(
            str,
            description="With which field should the data be sorted",
            enum=["played", "losses", "wins", "mvps"],
        ) = "played",
    ):
        if season == -1:
            season = Season.get_current_season().id

        if order_by not in ["played", "losses", "wins", "mvps"]:
            return
        f = open("cache/get_players_matches_" + order_by + "_" + str(season) + ".txt", "r")
        content = f.read()
        f.close()

        return 200, json.loads(content)

    @staticmethod
    @route(
        name="maps_statistics",
        summary="Maps statistics",
        description="Deleted endpoint",
        deprecated=True,
    )
    def get_maps_statistics():

        return 503, {"message": "Endpoint disabled due to poor performance. It is now a season global computed metric."}

    @staticmethod
    @route(
        name="search_player",
        summary="Search player",
        description="Returns a list of up to 10 players matching the search string provided.",
    )
    def search_player(name: str = ""):
        query = Player.select(Player.uuid, Player.name).where(Player.name.contains(name)).paginate(1, 10).dicts()
        return 200, {"results": [q for q in query]}

    @staticmethod
    @route(
        name="player_opponents_statistics",
        summary="Player opponents statistics",
        deprecated=True,
        description="Returns statistics of a selected player performance with players they played against/along.",
    )
    def get_player_opponents_statistics(
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
        group_by: Option(
            str,
            description="On which opponent type should the stats be made",
            enum=["uuid", "country", "club"],
        ) = "uuid",
    ):
        return PlayerAPIViews.get_opponents_statistics(player, order_by, order, page, min_date, max_date, group_by)

    @staticmethod
    @route(
        name="player_statistics",
        summary="Player general statistics",
        description="Returns basic statistics on a selected player (points, position, matches played, ...)",
        deprecated=True,
    )
    def get_player_statistics(
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
        return PlayerAPIViews.get_statistics(player, min_date, max_date, season)

    @staticmethod
    @route(
        name="swagger.json",
        summary="OpenAPI specification",
        description="Returns OpenAPI specification of the API",
    )
    def get_swagger():
        from src.routes import routes

        return 200, {
            "openapi": "3.0.0",
            "info": {
                "title": "Matchmaking Status API",
                "description": "API providing data and statistics about Trackmania matchmaking. API is free to use as "
                "long as usage is reasonable. If you want to create a project relying on heavy usage "
                "of the API, contact me on Discord (racacax).",
                "version": "1.0.0",
                "contact": {"name": "racacax"},
            },
            "paths": {
                f"/api/{path}": {
                    "get": {
                        "tags": func.tags,
                        "summary": func.summary,
                        "description": func.description,
                        "deprecated": func.deprecated,
                        "parameters": [
                            {
                                "name": name,
                                "in": "query",
                                "required": False,
                                "schema": format_type(data),
                            }
                            for name, data in signature(func).parameters.items()
                        ],
                    }
                }
                for path, func in routes.items()
            },
        }
