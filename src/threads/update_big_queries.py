import json
import os
import time
import traceback
from collections import defaultdict
from datetime import datetime, time as time2, timedelta
from typing import Callable

from peewee import fn, Case, JOIN

from models import Player, PlayerGame, Game, Season, Zone, PlayerSeason, Map
from src.log_utils import create_logger
from src.utils import CustomJSONEncoder, RANKS, distribute_points
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_big_queries")


def get_countries_leaderboard(season_id):
    total_players = 1000
    points_repartition = distribute_points(max_points=10000, num_people=total_players)
    player_seasons = (
        PlayerSeason.select(
            Player.uuid,
            Player.country,
            PlayerSeason.rank,
            Zone.name,
            Zone.country_alpha3,
            Zone.file_name,
        )
        .join(Season)
        .switch(PlayerSeason)
        .join(Player)
        .join(Zone, on=(Zone.id == Player.country))
        .where(Season.id == season_id, PlayerSeason.rank != 0)
        .order_by(PlayerSeason.rank.asc())
        .paginate(1, total_players)
    ).dicts()
    objs = [o for o in player_seasons]
    countries = {}
    for o in objs:
        if o["rank"] > len(points_repartition):
            computed_points = 0
        else:
            computed_points = int(points_repartition[o["rank"] - 1])
        if not o["country"] in countries:
            countries[o["country"]] = {
                "name": o["name"],
                "file_name": o["file_name"],
                "country_alpha3": o["country_alpha3"],
                "points": 0,
                "count": 0,
            }
        if countries[o["country"]]["count"] < 10:
            countries[o["country"]]["points"] += computed_points
            countries[o["country"]]["count"] += 1
    countries = countries.values()
    countries = sorted(countries, key=lambda x: x["points"], reverse=True)
    return json.dumps(
        {
            "last_updated": datetime.now().timestamp(),
            "compute_method": "top_10",
            "results": countries,
        },
    )


def get_clubs_leaderboard(season_id):
    total_players = 1000
    points_repartition = distribute_points(max_points=10000, num_people=total_players)
    player_seasons = (
        PlayerSeason.select(
            Player.uuid,
            Player.club_tag,
            PlayerSeason.rank,
        )
        .join(Season)
        .switch(PlayerSeason)
        .join(Player)
        .where(Season.id == season_id, PlayerSeason.rank != 0, Player.club_tag != None)
        .order_by(PlayerSeason.rank.asc())
        .paginate(1, total_players)
    ).dicts()
    objs = [o for o in player_seasons]
    club_tags = {}
    for o in objs:
        if o["rank"] > len(points_repartition):
            computed_points = 0
        else:
            computed_points = int(points_repartition[o["rank"] - 1])
        if not o["club_tag"] in club_tags:
            club_tags[o["club_tag"]] = {"name": o["club_tag"], "points": 0, "count": 0}
        if club_tags[o["club_tag"]]["count"] < 10:
            club_tags[o["club_tag"]]["count"] += 1
            club_tags[o["club_tag"]]["points"] += computed_points
    club_tags = club_tags.values()
    club_tags = sorted(club_tags, key=lambda x: x["points"], reverse=True)
    return json.dumps(
        {
            "last_updated": datetime.now().timestamp(),
            "compute_method": "top_10",
            "results": club_tags,
        },
    )


def get_leaderboards_funcs(season_id):
    funcs = []
    for func in [get_clubs_leaderboard, get_countries_leaderboard]:

        def parent_func(scoped_func):
            def my_func(_, __):
                return scoped_func(season_id)

            my_func.__name__ = scoped_func.__name__
            return my_func

        funcs.append(parent_func(func))
    return funcs


def get_maps_statistics(min_date, max_date):
    uid = Map.uid
    count = fn.COUNT(uid)
    query = (
        Game.select(
            Map.uid.alias("map_uid"),
            Map.name.alias("map_name"),
            count.alias("total_played"),
        )
        .join(Map)
        .where(Game.time >= min_date, Game.time <= max_date)
        .group_by(uid)
        .order_by(count.desc())
        .dicts()
    )
    return json.dumps({"last_updated": datetime.now().timestamp(), "results": [q for q in query]})


def get_players_statistics(o_by, min_date, max_date):
    played = fn.COUNT(Player.uuid)
    wins = fn.SUM(PlayerGame.is_win)
    mvps = fn.SUM(PlayerGame.is_mvp)
    losses = fn.SUM(Case(None, [((PlayerGame.is_win == False), 1)], 0))

    order_by = played
    if o_by == "losses":
        order_by = losses
    elif o_by == "wins":
        order_by = wins
    elif o_by == "mvps":
        order_by = mvps
    query = (
        PlayerGame.select(
            Player.name,
            Player.uuid,
            wins.alias("wins"),
            losses.alias("losses"),
            played.alias("played"),
            mvps.alias("mvps"),
        )
        .join(Game)
        .switch(PlayerGame)
        .join(Player)
        .where(Game.time >= min_date, Game.time <= max_date)
        .group_by(Player.uuid)
        .order_by(order_by.desc())
        .paginate(1, 100)
        .dicts()
    )

    data = []
    for q in query:
        data.append(
            {
                "name": q["name"],
                "uuid": str(q["uuid"]),
                "played": int(q["played"] or 0),
                "wins": int(q["wins"] or 0),
                "losses": int(q["losses"] or 0),
                "mvps": int(q["mvps"] or 0),
            }
        )
    return json.dumps({"last_updated": datetime.now().timestamp(), "results": data})


def get_players_matches_funcs():
    players_matches = []
    for o_by in ["played", "wins", "losses", "mvps"]:

        def parent_func(scoped_o_by):
            def my_func(min_date, max_date):
                return get_players_statistics(scoped_o_by, min_date, max_date)

            my_func.__name__ = "get_players_matches_" + str(scoped_o_by)
            return my_func

        players_matches.append(parent_func(o_by))
    return players_matches


def get_activity_per_hour(min_elo, min_date, max_date):
    def get_condition(min_hour):
        return fn.SUM(
            Case(
                None,
                [
                    (
                        fn.TIME(Game.time).between(time2(min_hour), time2(min_hour, 59, 59)),
                        1,
                    )
                ],
                0,
            )
        ).alias(f"{min_hour}-{min_hour + 1}")

    conditions = [(get_condition(i)) for i in range(24)]
    games = (
        Game.select(*conditions)
        .where(
            Game.average_elo > -1,
            Game.min_elo >= min_elo,
            Game.time >= min_date,
            Game.time <= max_date,
        )
        .dicts()
    )

    return json.dumps(
        {"results": games[0], "last_updated": datetime.now().timestamp()},
        cls=CustomJSONEncoder,
    )


def get_activity_per_day_of_the_week(min_elo, min_date, max_date):
    # DAYOFWEEK() returns 1=Sunday … 7=Saturday; subtract 1 for 0-indexed keys
    # COALESCE handles the case where the WHERE clause filters all rows (SUM returns NULL)
    def get_condition(day):
        return fn.COALESCE(fn.SUM(Case(None, [(fn.DAYOFWEEK(Game.time) == day + 1, 1)], 0)), 0).alias(str(day))

    conditions = [get_condition(i) for i in range(7)]
    games = (
        Game.select(*conditions)
        .where(
            Game.average_elo > -1,
            Game.min_elo >= min_elo,
            Game.time >= min_date,
            Game.time <= max_date,
        )
        .dicts()
    )
    return json.dumps(
        {"results": games[0], "last_updated": datetime.now().timestamp()},
        cls=CustomJSONEncoder,
    )


def get_activity_heatmap(min_elo, min_date, max_date):
    day = (fn.DAYOFWEEK(Game.time) - 1).alias("day")
    hour = fn.HOUR(Game.time).alias("hour")
    count = fn.COUNT(Game.id).alias("count")
    query = (
        Game.select(day, hour, count)
        .where(
            Game.average_elo > -1,
            Game.min_elo >= min_elo,
            Game.time >= min_date,
            Game.time <= max_date,
        )
        .group_by(fn.DAYOFWEEK(Game.time), fn.HOUR(Game.time))
        .order_by(fn.DAYOFWEEK(Game.time), fn.HOUR(Game.time))
        .dicts()
    )
    return json.dumps(
        {
            "results": [{"day": r["day"], "hour": r["hour"], "count": r["count"]} for r in query],
            "last_updated": datetime.now().timestamp(),
        },
        cls=CustomJSONEncoder,
    )


def get_activity_heatmap_funcs():
    funcs = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_activity_heatmap(min_elo, min_date, max_date)

            my_func.__name__ = "get_activity_heatmap_" + str(min_elo)
            return my_func

        funcs.append(parent_func(rank["min_elo"]))
    return funcs


def get_activity_day_of_the_week_funcs():
    activity_per_day_of_the_week = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_activity_per_day_of_the_week(min_elo, min_date, max_date)

            my_func.__name__ = "get_activity_per_day_of_the_week_" + str(min_elo)
            return my_func

        activity_per_day_of_the_week.append(parent_func(rank["min_elo"]))
    return activity_per_day_of_the_week


def get_activity_hours_funcs():
    activity_per_hours = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_activity_per_hour(min_elo, min_date, max_date)

            my_func.__name__ = "get_activity_per_hour_" + str(min_elo)
            return my_func

        activity_per_hours.append(parent_func(rank["min_elo"]))
    return activity_per_hours


def get_activity_per_country(min_elo, min_date, max_date):
    games = (
        PlayerGame.select(
            Zone.id,
            Zone.name,
            Zone.file_name,
            Zone.country_alpha3,
            fn.COUNT(Game.id).alias("total"),
            fn.SUM(PlayerGame.is_win).alias("wins"),
        )
        .join(Player)
        .join(Zone, JOIN.LEFT_OUTER, on=(Player.country_id == Zone.id), attr="country")
        .switch(PlayerGame)
        .join(Game)
        .where(
            Game.average_elo > -1,
            Game.max_elo >= min_elo,
            Game.time >= min_date,
            Game.time <= max_date,
        )
        .group_by(Zone.id)
        .dicts()
    )

    return json.dumps(
        {"results": [g for g in games], "last_updated": datetime.now().timestamp()},
        cls=CustomJSONEncoder,
    )


def get_activity_per_country_funcs():
    activity_per_country = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_activity_per_country(min_elo, min_date, max_date)

            my_func.__name__ = "get_activity_per_country_" + str(min_elo)
            return my_func

        activity_per_country.append(parent_func(rank["min_elo"]))
    return activity_per_country


def get_activity_hours_countries(min_elo, min_date, max_date):
    def get_condition(min_hour):
        return fn.SUM(
            Case(
                None,
                [
                    (
                        fn.TIME(Game.time).between(time2(min_hour), time2(min_hour, 59, 59)),
                        1,
                    )
                ],
                0,
            )
        ).alias(f"{min_hour}-{min_hour + 1}")

    conditions = [(get_condition(i)) for i in range(24)]
    games = (
        PlayerGame.select(Zone.id, Zone.name, Zone.file_name, Zone.country_alpha3, *conditions)
        .join(Player)
        .join(Zone, JOIN.LEFT_OUTER, on=(Player.country_id == Zone.id), attr="country")
        .switch(PlayerGame)
        .join(Game)
        .where(
            Game.average_elo > -1,
            Game.max_elo >= min_elo,
            Game.time >= min_date,
            Game.time <= max_date,
        )
        .group_by(Zone.id)
        .dicts()
    )

    return json.dumps(
        {"results": [g for g in games], "last_updated": datetime.now().timestamp()},
        cls=CustomJSONEncoder,
    )


def get_activity_hours_countries_funcs():
    activity_per_hours = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_activity_hours_countries(min_elo, min_date, max_date)

            my_func.__name__ = "get_activity_per_country_and_hour_" + str(min_elo)
            return my_func

        activity_per_hours.append(parent_func(rank["min_elo"]))
    return activity_per_hours


def get_activity_players_per_country(min_elo, min_rank, min_date):
    players = (
        Player.select(
            Zone.id,
            Zone.name,
            Zone.file_name,
            Zone.country_alpha3,
            fn.COUNT(Player.uuid).alias("total"),
        )
        .join(Zone, JOIN.LEFT_OUTER, on=(Player.country_id == Zone.id), attr="country")
        .switch(Player)
        .join(Game)
        .where(
            Player.points >= min_elo,
            *([Player.rank <= min_rank] if min_rank else []),
            Game.time > min_date,
        )
        .group_by(Zone.id)
        .dicts()
    )

    return json.dumps(
        {"results": [g for g in players], "last_updated": datetime.now().timestamp()},
        cls=CustomJSONEncoder,
    )


H2H_ELOS = [3000, 3300, 3600, 4000]


def get_country_h2h_func(path, season, min_elo):
    def get_country_h2h(min_date, max_date):
        season_path = f"{path}country_h2h_{min_elo}/{season.id}/"
        if not os.path.exists(season_path):
            os.makedirs(season_path)

        pg1 = PlayerGame.alias()
        pg2 = PlayerGame.alias()
        p1 = Player.alias()
        p2 = Player.alias()
        z1 = Zone.alias()
        z2 = Zone.alias()

        wins_expr = fn.COUNT(fn.DISTINCT(Case(None, ((pg1.is_win == 1, Game.id),), None))).alias("wins")

        query = (
            Game.select(
                z1.country_alpha3.alias("country_a"),
                z2.country_alpha3.alias("country_b"),
                wins_expr,
                fn.COUNT(fn.DISTINCT(Game.id)).alias("games"),
            )
            .join(pg1, on=(pg1.game_id == Game.id))
            .join(p1, on=(p1.uuid == pg1.player_id))
            .join(z1, on=((z1.id == p1.country_id) & (z1.country_alpha3.is_null(False))))
            .switch(Game)
            .join(pg2, on=((pg2.game_id == Game.id) & (pg2.player_id != pg1.player_id)))
            .join(p2, on=(p2.uuid == pg2.player_id))
            .join(
                z2,
                on=(
                    (z2.id == p2.country_id)
                    & (z2.country_alpha3.is_null(False))
                    & (z2.country_alpha3 != z1.country_alpha3)
                ),
            )
            .where(
                (Game.average_elo > -1)
                & (Game.is_finished == True)
                & (Game.min_elo >= min_elo)
                & (Game.time >= min_date)
                & (Game.time <= max_date)
            )
            .group_by(z1.country_alpha3, z2.country_alpha3)
        )
        rows = list(query.dicts())

        by_country = defaultdict(list)
        for row in rows:
            wins = int(row["wins"] or 0)
            games = int(row["games"] or 0)
            by_country[row["country_a"]].append(
                {
                    "opponent": row["country_b"],
                    "wins": wins,
                    "losses": games - wins,
                    "games": games,
                }
            )

        countries_written = []
        for country_a, records in by_country.items():
            records.sort(key=lambda r: r["games"], reverse=True)
            with open(season_path + country_a + ".txt", "w") as f:
                f.write(
                    json.dumps(
                        {"results": records, "last_updated": datetime.now().timestamp()},
                        cls=CustomJSONEncoder,
                    )
                )
            countries_written.append(country_a)

        return json.dumps({"countries": sorted(countries_written), "last_updated": datetime.now().timestamp()})

    get_country_h2h.__name__ = f"get_country_h2h_{min_elo}"
    return get_country_h2h


def get_country_h2h_funcs(path, season):
    return [get_country_h2h_func(path, season, min_elo) for min_elo in H2H_ELOS]


def get_top_100_per_country_func(path, season):
    def get_top_100_per_country_0(_, __):
        countries = Zone.select().where(Zone.country_alpha3 != None)
        countries_gathered = []
        season_path = path + f"top_100_by_country/{season.id}/"
        if not os.path.exists(season_path):
            os.makedirs(season_path)
        for country in countries:
            players = (
                PlayerSeason.select(Player.name, Player.uuid, PlayerSeason.rank, PlayerSeason.points)
                .join(Season)
                .switch(PlayerSeason)
                .join(Player)
                .join(Zone, on=(Zone.id == Player.country))
                .where(Zone.id == country.id, Season.id == season.id)
                .order_by(PlayerSeason.points.desc())
                .paginate(1, 100)
                .dicts()
            )
            if len(players) > 0:
                countries_gathered.append({"country_alpha3": country.country_alpha3, "name": country.name})
                f = open(season_path + country.country_alpha3 + ".txt", "w")
                f.write(
                    json.dumps(
                        {
                            "results": [p for p in players],
                            "last_updated": datetime.now().timestamp(),
                        },
                        cls=CustomJSONEncoder,
                    )
                )
                f.close()
        return json.dumps(
            {"results": countries_gathered, "last_updated": datetime.now().timestamp()},
            cls=CustomJSONEncoder,
        )

    return get_top_100_per_country_0


def get_activity_players_per_country_funcs():
    players_per_country = []
    for rank in RANKS:

        def parent_func(min_elo, min_rank):
            def my_func(min_date, __):
                return get_activity_players_per_country(min_elo, min_rank, min_date)

            my_func.__name__ = "get_activity_per_players_per_country_" + str(min_elo)
            return my_func

        players_per_country.append(parent_func(rank["min_elo"], rank["min_rank"]))
    return players_per_country


def get_rank_distribution_func(season: Season):
    def get_activity_per_rank_distribution(min_date, _):
        def get_condition(code, min_elo, max_elo, min_rank):
            if min_rank:
                condition = Player.rank <= min_rank
            else:
                condition = Player.rank > 10
            condition = (Player.points >= min_elo) & (Player.points < max_elo) & condition
            return fn.SUM(Case(None, [(condition, 1)], 0)).alias(code)

        conditions = []
        for i in range(len(RANKS) - 1, -1, -1):
            if i != 0:
                max_elo = RANKS[i - 1]["min_elo"]
            else:
                max_elo = 99999999
            conditions.append(get_condition(RANKS[i]["key"], RANKS[i]["min_elo"], max_elo, RANKS[i]["min_rank"]))
        players = (
            Player.select(*conditions)
            .join(Game)
            .where(
                Game.time >= min_date,
            )
            .dicts()
        )
        try:
            f = open("cache/get_activity_per_rank_distribution_" + str(season.id) + ".txt", "r")
            j = json.loads(f.read())
            f.close()
        except FileNotFoundError:
            j = {"results": [], "last_updated": 0}
        if len(j) == 0 or j["last_updated"] < (datetime.now() - timedelta(hours=12)).timestamp():
            d = players[0]
            d["date"] = datetime.now().timestamp()
            j["last_updated"] = d["date"]
            j["results"].append(d)

        return json.dumps(
            j,
            cls=CustomJSONEncoder,
        )

    get_activity_per_rank_distribution.__name__ = "get_activity_per_rank_distribution"
    return get_activity_per_rank_distribution


def get_player_retention(min_elo, min_date, max_date):
    """
    For each complete week-pair (N, N+1) within the season window, compute:
      - total_players : distinct players who played at least once in week N
      - retained_players : how many of those also played in week N+1
      - retention_rate : retained / total * 100

    Optimization: one SQL query fetches every distinct (player_uuid, week_number)
    pair; all set-intersection logic runs in Python — no per-week subqueries.
    """
    week_expr = fn.FLOOR(fn.DATEDIFF(Game.time, min_date) / 7)

    # Drive from game (range scan on time index) → playergame (game_id FK lookup).
    # No join to player needed: playergame.player_id already holds the UUID.
    rows = (
        Game.select(
            PlayerGame.player_id.alias("player_id"),
            week_expr.alias("week_num"),
        )
        .join(PlayerGame)
        .where(
            Game.average_elo > -1,
            Game.min_elo >= min_elo,
            Game.time >= min_date,
            Game.time <= max_date,
        )
        .group_by(PlayerGame.player_id, week_expr)
        .dicts()
    )

    weeks: dict[int, set] = defaultdict(set)
    for row in rows:
        weeks[int(row["week_num"])].add(row["player_id"])

    results = []
    for n in sorted(weeks.keys()):
        if n + 1 not in weeks:
            continue  # no data for the following week yet
        total = len(weeks[n])
        retained = len(weeks[n] & weeks[n + 1])
        results.append(
            {
                "week": n,
                "week_start": (min_date + timedelta(weeks=n)).timestamp(),
                "total_players": total,
                "retained_players": retained,
                "retention_rate": round(retained * 100 / total, 2) if total > 0 else 0.0,
            }
        )

    return json.dumps(
        {"results": results, "last_updated": datetime.now().timestamp()},
        cls=CustomJSONEncoder,
    )


def get_player_retention_funcs():
    funcs = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_player_retention(min_elo, min_date, max_date)

            my_func.__name__ = "get_player_retention_" + str(min_elo)
            return my_func

        funcs.append(parent_func(rank["min_elo"]))
    return funcs


def get_hot_this_week(min_elo, _min_date, _max_date):
    start = datetime.now() - timedelta(days=7)
    wins = fn.SUM(PlayerGame.is_win)
    played = fn.COUNT(PlayerGame.id)
    query = (
        Game.select(
            Player.name,
            Player.uuid,
            Player.club_tag,
            wins.alias("wins"),
            played.alias("played"),
        )
        .join(PlayerGame)
        .switch(Game)
        .join(Player, on=(Player.uuid == PlayerGame.player_id))
        .where(
            Game.average_elo > -1,
            Game.min_elo >= min_elo,
            Game.time >= start,
        )
        .group_by(Player.uuid)
        .order_by(wins.desc())
        .paginate(1, 20)
        .dicts()
    )
    data = []
    for q in query:
        data.append(
            {
                "name": q["name"],
                "uuid": str(q["uuid"]),
                "club_tag": q["club_tag"],
                "wins": int(q["wins"] or 0),
                "played": int(q["played"] or 0),
            }
        )
    return json.dumps({"last_updated": datetime.now().timestamp(), "results": data})


def get_hot_this_week_funcs():
    funcs = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_hot_this_week(min_elo, min_date, max_date)

            my_func.__name__ = "get_hot_this_week_" + str(min_elo)
            return my_func

        funcs.append(parent_func(rank["min_elo"]))
    return funcs


def get_hot_this_week_by_points_delta(min_elo, _min_date, _max_date):
    start = datetime.now() - timedelta(days=7)
    cursor = Game._meta.database.execute_sql(
        """
        WITH period_games AS (
            SELECT
                pg.player_id,
                pg.points_after_match,
                ROW_NUMBER() OVER (PARTITION BY pg.player_id ORDER BY g.time ASC)  AS rn_asc,
                ROW_NUMBER() OVER (PARTITION BY pg.player_id ORDER BY g.time DESC) AS rn_desc,
                COUNT(*)     OVER (PARTITION BY pg.player_id)                       AS played
            FROM game g
            JOIN playergame pg ON pg.game_id = g.id
            WHERE g.average_elo > -1
              AND g.min_elo >= %s
              AND g.time >= %s
              AND pg.points_after_match IS NOT NULL
        )
        SELECT p.name, p.uuid, p.club_tag,
               MAX(CASE WHEN rn_desc = 1 THEN points_after_match END)
               - MAX(CASE WHEN rn_asc  = 1 THEN points_after_match END) AS delta,
               MAX(played) AS played
        FROM period_games pg
        JOIN player p ON p.uuid = pg.player_id
        GROUP BY p.uuid, p.name, p.club_tag
        ORDER BY delta DESC
        LIMIT 20
        """,
        (min_elo, start),
    )
    data = []
    for row in cursor.fetchall():
        name, uuid, club_tag, delta, played = row
        data.append(
            {
                "name": name,
                "uuid": str(uuid),
                "club_tag": club_tag,
                "delta": int(delta or 0),
                "played": int(played or 0),
            }
        )
    return json.dumps({"last_updated": datetime.now().timestamp(), "results": data})


def get_hot_this_week_by_points_delta_funcs():
    funcs = []
    for rank in RANKS:

        def parent_func(min_elo):
            def my_func(min_date, max_date):
                return get_hot_this_week_by_points_delta(min_elo, min_date, max_date)

            my_func.__name__ = "get_hot_this_week_by_points_delta_" + str(min_elo)
            return my_func

        funcs.append(parent_func(rank["min_elo"]))
    return funcs


def get_seasons_to_update() -> list:
    """Return seasons that need a big-queries refresh.

    Includes the current active season and any season that ended within
    the last 24 hours, so a season that finished a few hours before the
    last scheduled run still gets a final refresh.
    """
    cutoff = datetime.now() - timedelta(hours=24)
    return list(Season.select().where(Season.end_time >= cutoff).order_by(Season.end_time.desc()))


class UpdateBigQueriesThread(AbstractThread):
    def __init__(self):
        super().__init__()
        self.path = "cache/"
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def get_queries(self, season: Season):
        return [
            *get_leaderboards_funcs(season and season.id or 1),
            get_maps_statistics,
            get_top_100_per_country_func(self.path, season),
            *get_country_h2h_funcs(self.path, season),
            get_rank_distribution_func(season),
            *get_activity_per_country_funcs(),
            *get_activity_players_per_country_funcs(),
            *get_activity_hours_countries_funcs(),
            *get_activity_hours_funcs(),
            *get_activity_heatmap_funcs(),
            *get_activity_day_of_the_week_funcs(),
            *get_player_retention_funcs(),
            *get_hot_this_week_funcs(),
            *get_hot_this_week_by_points_delta_funcs(),
            *get_players_matches_funcs(),
        ]

    def run_query(self, q: Callable[[int, int], str], season: Season):
        name = q.__name__
        logger.info(f"Starting query {name}")
        try:
            results = q(season.start_time, season.end_time)
            f = open(self.path + name + "_" + str(season.id) + ".txt", "w")
            f.write(results)
            f.close()
        except Exception as e:
            self._record_error()
            logger.error(
                f"Error for query {name}",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )

    def run_iteration(self):
        seasons = get_seasons_to_update()
        for season in seasons:
            logger.info(f"Running big queries for season {season.id} ({season.name})")
            queries = self.get_queries(season)
            for q in queries:
                self.run_query(q, season)
                logger.info("Waiting 20s before running new query...")
                time.sleep(
                    20
                )  # since some of these queries are locking the whole database, we add gaps between queries to allow
                # other normal threads to work again

    def handle(self):
        while True:
            self.run_iteration()
            logger.info("Waiting 1h before updating data...")
            time.sleep(3600)
