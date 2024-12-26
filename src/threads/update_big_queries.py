import json
import os
import time
import traceback
from datetime import datetime, time as time2, timedelta
from typing import Callable

from peewee import fn, Case, JOIN

from models import Player, PlayerGame, Game, Season, Zone, PlayerSeason, Map
from src.log_utils import create_logger
from src.utils import CustomJSONEncoder, RANKS, calculate_points
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_big_queries")


def get_countries_leaderboard(season_id):
    total_players = 1000
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
        computed_points = calculate_points(o["rank"], total_players=total_players)
        if not o["country"] in countries:
            countries[o["country"]] = {
                "name": o["name"],
                "file_name": o["file_name"],
                "country_alpha3": o["country_alpha3"],
                "points": 0,
            }
        countries[o["country"]]["points"] += computed_points
    countries = countries.values()
    countries = sorted(countries, key=lambda x: x["points"], reverse=True)
    return json.dumps(
        {
            "last_updated": datetime.now().timestamp(),
            "results": countries,
        },
    )


def get_clubs_leaderboard(season_id):
    total_players = 1000
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
        computed_points = calculate_points(o["rank"], total_players=total_players)
        if not o["club_tag"] in club_tags:
            club_tags[o["club_tag"]] = {
                "name": o["club_tag"],
                "points": 0,
            }
        club_tags[o["club_tag"]]["points"] += computed_points
    club_tags = club_tags.values()
    club_tags = sorted(club_tags, key=lambda x: x["points"], reverse=True)
    return json.dumps(
        {
            "last_updated": datetime.now().timestamp(),
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
            *([(Player.rank <= min_rank)] if min_rank else []),
            Game.time > min_date,
        )
        .group_by(Zone.id)
        .dicts()
    )

    return json.dumps(
        {"results": [g for g in players], "last_updated": datetime.now().timestamp()},
        cls=CustomJSONEncoder,
    )


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
    season = Season.get_current_season()
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


class UpdateBigQueriesThread(AbstractThread):
    def __init__(self):
        self.path = "cache/"
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def get_queries(self, season: Season):
        return [
            *get_leaderboards_funcs(season and season.id or 1),
            get_maps_statistics,
            get_top_100_per_country_func(self.path, season),
            get_activity_per_rank_distribution,
            *get_activity_per_country_funcs(),
            *get_activity_players_per_country_funcs(),
            *get_activity_hours_countries_funcs(),
            *get_activity_hours_funcs(),
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
            logger.error(
                f"Error for query {name}",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )

    def run_iteration(self):
        season = Season.get_current_season()

        queries = self.get_queries(season)
        for q in queries:
            self.run_query(q, season)
            logger.info("Waiting 60s before running new query...")
            time.sleep(
                60
            )  # since some of these query are locking the whole database, we add gaps between queries to allow other
            # normal threads to work again

    def handle(self):
        while True:
            self.run_iteration()
            logger.info("Waiting 1h before updating data...")
            time.sleep(3600)
