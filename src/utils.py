import base64
import decimal
import inspect
import json
import numpy as np
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler
from inspect import Parameter
from typing import Union
from uuid import UUID

import requests

from models import Player


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        if isinstance(o, datetime):
            return o.timestamp()
        if isinstance(o, UUID):
            return str(o)
        return super().default(o)


def get(url, token):
    return requests.get(
        url,
        headers={
            "Authorization": f"nadeo_v1 t={token}",
        },
    ).json()


def post(url, data, token):
    return requests.post(
        url,
        data=data,
        headers={
            "Authorization": f"nadeo_v1 t={token}",
        },
    ).json()


def authenticated(method):
    def func(cls, *args, **kwargs):
        if cls.EXPIRE_TIME <= datetime.now():
            cls.refresh_token()
        return method(cls, *args, **kwargs)

    return func


def get_uuid_from_login(login: str):
    login = login.ljust(24).replace(" ", "=")
    login = login.replace("_", "/").replace("-", "+")
    login = base64.b64decode(login)
    return UUID(login.hex())


RANKS = [
    {"name": "Trackmaster", "image": "TM.png", "key": "tm", "min_elo": 4000, "min_rank": 10, "id": 12},
    {"name": "Master III", "image": "M3.png", "key": "m3", "min_elo": 3600, "min_rank": None, "id": 11},
    {"name": "Master II", "image": "M2.png", "key": "m2", "min_elo": 3300, "min_rank": None, "id": 10},
    {"name": "Master I", "image": "M1.png", "key": "m1", "min_elo": 3000, "min_rank": None, "id": 9},
    {"name": "Gold III", "image": "G3.png", "key": "g3", "min_elo": 2600, "min_rank": None, "id": 8},
    {"name": "Gold II", "image": "G2.png", "key": "g2", "min_elo": 2300, "min_rank": None, "id": 7},
    {"name": "Gold I", "image": "G1.png", "key": "g1", "min_elo": 2000, "min_rank": None, "id": 6},
    {"name": "Silver III", "image": "S3.png", "key": "s3", "min_elo": 1600, "min_rank": None, "id": 5},
    {"name": "Silver II", "image": "S2.png", "key": "s2", "min_elo": 1300, "min_rank": None, "id": 4},
    {"name": "Silver I", "image": "S1.png", "key": "s1", "min_elo": 1000, "min_rank": None, "id": 3},
    {"name": "Bronze III", "image": "B3.png", "key": "b3", "min_elo": 600, "min_rank": None, "id": 2},
    {"name": "Bronze II", "image": "B2.png", "key": "b2", "min_elo": 300, "min_rank": None, "id": 1},
    {"name": "Bronze I", "image": "B1.png", "key": "b1", "min_elo": 0, "min_rank": None, "id": 0},
]


def send_error(server: BaseHTTPRequestHandler, code: int, message: str):
    send_response(server, code, {"code": code, "message": message})


def send_response(server: BaseHTTPRequestHandler, code: int, content: Union[list, dict]):
    encoded_response = json.dumps(content, cls=CustomJSONEncoder).encode()
    server.send_response(code)
    server.send_header("Content-Type", "application/json")
    server.send_header("Access-Control-Allow-Origin", "*")
    server.end_headers()
    server.wfile.write(encoded_response)


def format_type(data: Parameter):
    if isinstance(data.annotation, Option):
        return {
            "type": f"{data.annotation.formatted_cast or data.annotation.cast.__name__}",
            "default": data.annotation.formatted_default or data.default,
            "enum": data.annotation.enum,
            "description": data.annotation.description or "No description provided",
        }
    else:
        return {
            "type": f"{data.annotation.__name__}",
            "default": data.default,
        }


class Option:
    def __init__(
        self,
        cast: callable,
        formatted_cast=None,
        formatted_default=None,
        description=None,
        enum=None,
    ):
        self.cast = cast
        self.formatted_cast = formatted_cast
        self.description = description
        self.formatted_default = formatted_default
        self.enum = enum

    def __call__(self, value):
        if self.cast == bool:
            return str(value).lower() == "true"
        else:
            return self.cast(value)


POINTS_TYPE = [str(r["min_elo"]) for r in RANKS]
STATS_PERIODS = {
    "HOURLY": 0,
    "DAILY": 3,
    "WEEKLY": 1,
    "MONTHLY": 2,
}


def route(name, description, summary=None, deprecated=False):
    def decorator(func):
        func.name = name
        func.summary = summary or name
        func.description = description
        func.is_route = True
        func.deprecated = deprecated
        return func

    return decorator


class RouteDescriber:
    prefix = ""
    tags = []

    @classmethod
    def routes(cls):
        routes = inspect.getmembers(cls, predicate=inspect.isfunction)
        formatted_routes = {}
        for route in routes:
            if hasattr(route[1], "is_route") and route[1].is_route:
                route[1].tags = cls.tags
                formatted_routes[cls.prefix + route[1].name] = route[1]
        return formatted_routes


def distribute_points(num_people=1000, max_points=10000):
    log_values = np.log2(np.arange(1, num_people + 1))
    scaled_points = (log_values - log_values.min()) / (log_values.max() - log_values.min())
    scaled_points = scaled_points * max_points
    rounded_points = np.round(scaled_points, 0)
    rounded_points = map(lambda x: max_points - x, rounded_points)
    return list(rounded_points)


def get_trackmaster_limit():
    f_trackmaster = (
        Player.select(Player.points)
        .where(Player.rank <= 10, Player.points >= 4000)
        .order_by(Player.rank.desc())
        .paginate(1, 1)
    )

    if len(f_trackmaster) > 0:
        return f_trackmaster[0].points
    else:
        return 99999


class DateUtils:

    @classmethod
    def divide_generic(cls, min_date: datetime, max_date: datetime, cursor: datetime, incr_cursor_fn: callable):
        """
        Generic function to divide an interval of two dates into:
        - A base interval containing the max period between cursor and max_date. incr_cursor_fn will increment cursor
        until it reaches max_date. The base interval will be between cursor and the max cursor (<= max_date).
        Can be None if first incr_cursor_fn call is above max_date.
        - Up to 2 intervals containing the ranges of dates that are not in the base interval. Can be between min_date
        and cursor parameter (if min_date <> cursor) and between the last cursor generated by incr_cursor_fn
        and max_date)
        """
        if min_date > max_date:
            raise ValueError("min_date cannot be greater than max_date")
        remaining_ranges = []
        full_range: Union[list, None] = None

        while cursor < max_date:
            new_cursor = incr_cursor_fn(cursor)
            new_cursor_with_delta = new_cursor - timedelta(seconds=1)
            if new_cursor_with_delta > max_date:
                if full_range:
                    full_range[1] = cursor - timedelta(seconds=1)
                remaining_ranges.append([cursor, max_date])
            elif new_cursor_with_delta == max_date or new_cursor == max_date:
                if full_range:
                    full_range[1] = new_cursor_with_delta
                else:
                    full_range = [cursor, new_cursor_with_delta]
                    if cursor != min_date:
                        remaining_ranges.append([min_date, cursor - timedelta(seconds=1)])
            elif not full_range:
                full_range = [cursor, None]
                if min_date != cursor:
                    remaining_ranges.append([min_date, cursor - timedelta(seconds=1)])

            cursor = new_cursor
        if not full_range:
            return None, [[min_date, max_date]]
        return full_range, remaining_ranges

    @classmethod
    def divide_range_by_full_months(cls, min_date: datetime, max_date: datetime):
        """
        Generates a base interval containing all the full months between min_date and max_date. (from 1st to last
        day of months).
        lower boundary will be first day of first full month. upper boundary will be last day of last full month.
        Up to two intervals will be generated containing the ranges of dates not in full months.
        (can be between min_date and 1st day of first full month, and between last day of last full month and max_date)
        """

        if min_date == min_date.replace(hour=0, minute=0, second=0, microsecond=0, day=1):
            cursor = min_date
        else:
            cursor = (min_date + timedelta(days=31)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return cls.divide_generic(
            min_date, max_date, cursor, incr_cursor_fn=lambda x: (x + timedelta(days=31)).replace(day=1)
        )

    @classmethod
    def divide_range_by_full_weeks(cls, min_date: datetime, max_date: datetime):
        """
        Same logic as divide_range_by_full_months with weeks (from monday to sunday)
        """
        if min_date.isoweekday() == 1 and min_date == min_date.replace(hour=0, minute=0, second=0, microsecond=0):
            cursor = min_date
        else:
            cursor = (min_date + timedelta(days=8 - min_date.isoweekday())).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        return cls.divide_generic(min_date, max_date, cursor, incr_cursor_fn=lambda x: x + timedelta(days=7))

    @classmethod
    def divide_range_by_full_days(cls, min_date: datetime, max_date: datetime):
        """
        Same logic as divide_range_by_full_months with days (from 00:00:00 to 23:59:59)
        """
        if min_date == min_date.replace(hour=0, minute=0, second=0, microsecond=0):
            cursor = min_date
        else:
            cursor = (min_date + timedelta(hours=24)).replace(hour=0, minute=0, second=0, microsecond=0)
        return cls.divide_generic(min_date, max_date, cursor, incr_cursor_fn=lambda x: x + timedelta(hours=24))

    @classmethod
    def divide_range_by_full_hours(cls, min_date: datetime, max_date: datetime):
        """
        Same logic as divide_range_by_full_months with hours (from 00:00:00 to 23:59:59)
        """
        if min_date == min_date.replace(minute=0, second=0, microsecond=0):
            cursor = min_date
        else:
            cursor = (min_date + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        return cls.divide_generic(min_date, max_date, cursor, incr_cursor_fn=lambda x: x + timedelta(hours=1))

    @classmethod
    def format_ranges_to_sql(cls, min_parameter, max_parameter, ranges: list, additional_condition=None):
        condition = None
        for r in ranges:
            new_condition = (min_parameter >= r[0]) & (max_parameter <= r[1])
            if condition is None:
                condition = new_condition
            else:
                condition |= new_condition
        if additional_condition and condition is not None:
            condition &= additional_condition
        return condition
