import base64
import decimal
import inspect
import json
import math
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from inspect import Parameter
from typing import Union
from uuid import UUID

import requests


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
    {
        "name": "Trackmaster",
        "image": "TM.png",
        "key": "tm",
        "min_elo": 4000,
        "min_rank": 10,
    },
    {
        "name": "Master III",
        "image": "M3.png",
        "key": "m3",
        "min_elo": 3600,
        "min_rank": None,
    },
    {
        "name": "Master II",
        "image": "M2.png",
        "key": "m2",
        "min_elo": 3300,
        "min_rank": None,
    },
    {
        "name": "Master I",
        "image": "M1.png",
        "key": "m1",
        "min_elo": 3000,
        "min_rank": None,
    },
    {
        "name": "Gold III",
        "image": "G3.png",
        "key": "g3",
        "min_elo": 2600,
        "min_rank": None,
    },
    {
        "name": "Gold II",
        "image": "G2.png",
        "key": "g2",
        "min_elo": 2300,
        "min_rank": None,
    },
    {
        "name": "Gold I",
        "image": "G1.png",
        "key": "g1",
        "min_elo": 2000,
        "min_rank": None,
    },
    {
        "name": "Silver III",
        "image": "S3.png",
        "key": "s3",
        "min_elo": 1600,
        "min_rank": None,
    },
    {
        "name": "Silver II",
        "image": "S2.png",
        "key": "s2",
        "min_elo": 1300,
        "min_rank": None,
    },
    {
        "name": "Silver I",
        "image": "S1.png",
        "key": "s1",
        "min_elo": 1000,
        "min_rank": None,
    },
    {
        "name": "Bronze III",
        "image": "B3.png",
        "key": "b3",
        "min_elo": 600,
        "min_rank": None,
    },
    {
        "name": "Bronze II",
        "image": "B2.png",
        "key": "b2",
        "min_elo": 300,
        "min_rank": None,
    },
    {
        "name": "Bronze I",
        "image": "B1.png",
        "key": "b1",
        "min_elo": 0,
        "min_rank": None,
    },
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
        return self.cast(value)


POINTS_TYPE = [str(r["min_elo"]) for r in RANKS]


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


def calculate_points(rank, max_points=10000, min_points=1000, total_players=5000):
    scale_factor = (max_points - min_points) / (math.log(2) - math.log(total_players + 1))
    points = int(max_points - scale_factor * (math.log(rank + 1) - math.log(2)))

    return max(min_points, points)
