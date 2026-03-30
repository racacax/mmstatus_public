from datetime import datetime
from uuid import UUID

from peewee import Case, fn, JOIN

from models import Player, PlayerGame, Game, Map, PlayerSeason, Season
from src.utils import Option, route, RouteDescriber
from src.view_utils.opponents_statistics import get_query


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
        finished = Game.is_finished == True
        wins = fn.SUM(Case(None, [((finished & (PlayerGame.is_win == True)), 1)], 0))
        losses = fn.SUM(Case(None, [((finished & (PlayerGame.is_win == False)), 1)], 0))
        mvps = fn.SUM(PlayerGame.is_mvp)
        played = fn.COUNT(Map.uid)
        finished_count = fn.NULLIF(fn.SUM(Case(None, [(finished, 1)], 0)), 0)
        winrate = (wins * 100 / finished_count).alias("winrate")
        lossrate = (losses * 100 / finished_count).alias("lossrate")
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
            .join(PlayerGame, JOIN.LEFT_OUTER)
            .join(Game, JOIN.LEFT_OUTER)
            .join(Map, JOIN.LEFT_OUTER)
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
            .join(Game, JOIN.LEFT_OUTER)
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
            .join(Game, JOIN.LEFT_OUTER)
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
                "played",
                "played_against",
                "played_along",
                "games_lost_against",
                "games_won_against",
                "games_lost_along",
                "games_won_along",
            ],
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
        group_by: Option(
            str,
            description="On which opponent type should the stats be made",
            enum=["uuid", "country", "club"],
        ) = "uuid",
    ):
        min_date = datetime.fromtimestamp(min_date)
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())
        data = get_query(min_date, max_date, player, group_by, f"total_{order_by}", order, page)
        return 200, {"results": [e for e in data], "player": player}

    @staticmethod
    @route(
        name="activity_heatmap",
        summary="Activity heatmap",
        description="Returns match counts grouped by day of week and hour of day for a selected player. "
        "day: 0=Sunday … 6=Saturday. hour: 0-23 (server local time).",
    )
    def get_activity_heatmap(
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

        day = (fn.DAYOFWEEK(Game.time) - 1).alias("day")
        hour = fn.HOUR(Game.time).alias("hour")
        count = fn.COUNT(PlayerGame.id).alias("count")

        query = (
            PlayerGame.select(day, hour, count)
            .join(Game, JOIN.LEFT_OUTER)
            .where(
                PlayerGame.player_id == player,
                Game.time >= min_date,
                Game.time <= max_date,
            )
            .group_by(fn.DAYOFWEEK(Game.time), fn.HOUR(Game.time))
            .order_by(fn.DAYOFWEEK(Game.time), fn.HOUR(Game.time))
            .dicts()
        )

        return 200, {
            "results": [{"day": r["day"], "hour": r["hour"], "count": r["count"]} for r in query],
            "player": player,
        }

    @staticmethod
    @route(
        name="performance_vs_elo",
        summary="Performance vs relative elo",
        description="Returns win rates split into three buckets based on the player's elo relative to the match "
        "average at the time of each game. 'underdog': player elo was below match average by at least threshold "
        "points. 'favorite': above by at least threshold. 'even': within threshold. "
        "The player's elo before each match is approximated from the previous match's points_after_match. "
        "The first game in the window is excluded (no prior elo reference). "
        "Games with uncomputed average elo are excluded.",
    )
    def get_performance_vs_elo(
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
        threshold: Option(
            int,
            description="Elo point gap separating underdog / even / favorite buckets",
        ) = 200,
    ):
        min_date = datetime.fromtimestamp(min_date)
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())

        rows = list(
            PlayerGame.select(
                PlayerGame.points_after_match,
                PlayerGame.is_win,
                Game.average_elo,
            )
            .join(Game, JOIN.LEFT_OUTER)
            .where(
                PlayerGame.player_id == player,
                PlayerGame.points_after_match != None,
                Game.is_finished == True,
                Game.average_elo > -1,
                Game.time >= min_date,
                Game.time <= max_date,
            )
            .order_by(Game.time.asc())
            .dicts()
        )

        buckets = {
            "underdog": {"games_played": 0, "wins": 0, "losses": 0},
            "even": {"games_played": 0, "wins": 0, "losses": 0},
            "favorite": {"games_played": 0, "wins": 0, "losses": 0},
        }

        for i, row in enumerate(rows):
            if i == 0:
                continue  # no prior match to establish a "before" elo
            elo_diff = rows[i - 1]["points_after_match"] - row["average_elo"]
            if elo_diff <= -threshold:
                bucket = "underdog"
            elif elo_diff >= threshold:
                bucket = "favorite"
            else:
                bucket = "even"
            b = buckets[bucket]
            b["games_played"] += 1
            if row["is_win"]:
                b["wins"] += 1
            else:
                b["losses"] += 1

        results = []
        for bucket_name, b in buckets.items():
            results.append(
                {
                    "bucket": bucket_name,
                    "games_played": b["games_played"],
                    "wins": b["wins"],
                    "losses": b["losses"],
                    "win_rate": round(b["wins"] * 100 / b["games_played"], 2) if b["games_played"] > 0 else 0.0,
                }
            )

        return 200, {"results": results, "player": player, "threshold": threshold}

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
        use_time_range = bool(min_date or max_date)

        if use_time_range:
            min_date = datetime.fromtimestamp(min_date)
            max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())
            # Best season = highest points among PlayerSeasons whose season overlaps the range
            best_ps = (
                PlayerSeason.select(PlayerSeason, Season)
                .join(Season)
                .where(
                    PlayerSeason.player == player,
                    Season.start_time <= max_date,
                    Season.end_time >= min_date,
                )
                .order_by(PlayerSeason.points.desc())
                .get_or_none()
            )
            if not best_ps:
                return 404, {"message": "Cannot retrieve any season with these filters"}
            selected_season = best_ps.season
        else:
            if season == -1:
                selected_season = Season.get_current_season()
            else:
                selected_season = Season.get_or_none(id=season)
            if not selected_season:
                return 404, {"message": "Cannot retrieve any season with these filters"}
            min_date = selected_season.start_time
            max_date = selected_season.end_time

        finished = Game.is_finished == True
        total_played = fn.COUNT(PlayerGame.id)
        total_wins = fn.SUM(Case(None, [((finished & (PlayerGame.is_win == True)), 1)], 0))
        total_losses = fn.SUM(Case(None, [((finished & (PlayerGame.is_win == False)), 1)], 0))
        total_mvp = fn.SUM(PlayerGame.is_mvp)

        if use_time_range:
            p_obj = Player.get_or_none(Player.uuid == player)
            if not p_obj:
                return 404, {"message": "Current player either doesn't exist or didn't play this season"}

            game_stats = (
                PlayerGame.select(
                    total_played.alias("total_played"),
                    total_wins.alias("total_wins"),
                    total_losses.alias("total_losses"),
                    total_mvp.alias("total_mvp"),
                )
                .join(Game, JOIN.LEFT_OUTER)
                .where(
                    PlayerGame.player_id == player,
                    Game.time >= min_date,
                    Game.time <= max_date,
                )
                .dicts()
            )[0]

            return 200, {
                "uuid": str(p_obj.uuid),
                "name": p_obj.name,
                "club_tag": p_obj.club_tag,
                "stats": {
                    "season": selected_season.name,
                    "rank": best_ps.rank,
                    "points": best_ps.points,
                    "total_played": int(game_stats["total_played"] or 0),
                    "total_wins": int(game_stats["total_wins"] or 0),
                    "total_losses": int(game_stats["total_losses"] or 0),
                    "total_mvp": int(game_stats["total_mvp"] or 0),
                },
            }

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
            .join(Player, JOIN.LEFT_OUTER)
            .join(PlayerGame, JOIN.LEFT_OUTER)
            .join(Game, JOIN.LEFT_OUTER)
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
            return 200, {
                "uuid": str(data["uuid"]),
                "name": data["name"],
                "club_tag": data["club_tag"],
                "stats": {
                    "season": selected_season.name,
                    "rank": data["rank"],
                    "points": data["points"],
                    "total_played": int(data["total_played"]),
                    "total_wins": int(data["total_wins"]),
                    "total_losses": int(data["total_losses"]),
                    "total_mvp": int(data["total_mvp"]),
                },
            }
        else:
            return 404, {"message": "Current player either doesn't exist or didn't play this season"}

    @staticmethod
    @route(
        name="matches",
        summary="Match history",
        description="Returns the list of matches played by a selected player, sorted by most recent first.",
    )
    def get_matches(
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
        page: int = 1,
    ):
        min_date = datetime.fromtimestamp(min_date)
        max_date = datetime.fromtimestamp(max_date or datetime.now().timestamp())

        rows = list(
            PlayerGame.select(
                Game.id.alias("game_id"),
                Game.time,
                Game.is_finished,
                Game.average_elo,
                Game.min_elo,
                Map.uid.alias("map_uid"),
                Map.name.alias("map_name"),
                PlayerGame.is_win,
                PlayerGame.is_mvp,
                PlayerGame.position,
                PlayerGame.points_after_match,
                PlayerGame.rank_after_match,
            )
            .join(Game, JOIN.LEFT_OUTER)
            .join(Map, JOIN.LEFT_OUTER)
            .where(
                PlayerGame.player_id == player,
                Game.time >= min_date,
                Game.time <= max_date,
            )
            .order_by(Game.time.desc())
            .paginate(page, 20)
            .dicts()
        )

        return 200, {
            "results": [
                {
                    "id": r["game_id"],
                    "time": r["time"].timestamp() if r["time"] else None,
                    "is_finished": r["is_finished"],
                    "average_elo": r["average_elo"],
                    "min_elo": r["min_elo"],
                    "map_uid": r["map_uid"],
                    "map_name": r["map_name"],
                    "is_win": r["is_win"],
                    "is_mvp": r["is_mvp"],
                    "position": r["position"],
                    "points_after_match": r["points_after_match"],
                    "rank_after_match": r["rank_after_match"],
                }
                for r in rows
            ],
            "player": player,
            "page": page,
        }
