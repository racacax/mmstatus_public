import base64
import json
from datetime import datetime

import requests

import settings
from src.utils import post, authenticated, get


class NadeoAudience:
    BASE_URL = None
    ACCESS_TOKEN = ""
    REFRESH_TOKEN = ""
    AUDIENCE = "MyAudience"
    EXPIRE_TIME = datetime.fromtimestamp(0)

    @classmethod
    def refresh_token(cls):

        if cls.ACCESS_TOKEN == "":
            result = NadeoCore.get_nadeoservices(cls.AUDIENCE)
        else:
            result = NadeoCore.get_tokens(cls.REFRESH_TOKEN)
        cls.ACCESS_TOKEN = result["accessToken"]
        cls.EXPIRE_TIME = datetime.fromtimestamp(
            json.loads(base64.b64decode(cls.ACCESS_TOKEN.split(".")[1].split("_")[0] + "========"))["exp"]
        )
        cls.REFRESH_TOKEN = result["refreshToken"]


class NadeoCore:
    BASE_URL = "https://prod.trackmania.core.nadeo.online/"
    ACCESS_TOKEN = ""
    EXPIRE_TIME = datetime.fromtimestamp(0)

    @classmethod
    def get_tokens(cls, refresh_token: str):
        return post(
            f"{cls.BASE_URL}v2/authentication/token/refresh",
            data="",
            token=refresh_token,
        )

    @classmethod
    def refresh_token(cls):
        result = cls.get_tokens(settings.NADEO_REFRESH_TOKEN)
        cls.ACCESS_TOKEN = result["accessToken"]
        cls.EXPIRE_TIME = datetime.fromtimestamp(
            json.loads(base64.b64decode(cls.ACCESS_TOKEN.split(".")[1].split("_")[0] + "========"))["exp"]
        )
        settings.NADEO_REFRESH_TOKEN = result["refreshToken"]
        with open(settings.NADEO2_FILE_PATH, "w") as f:
            f.write(result["refreshToken"])
            f.close()

    @classmethod
    @authenticated
    def get_player_club_tags(cls, account_ids: list):
        return get(
            f"{cls.BASE_URL}accounts/clubTags/?accountIdList={','.join(account_ids)}",
            token=cls.ACCESS_TOKEN,
        )

    @classmethod
    @authenticated
    def get_player_zones(cls, account_ids: list):
        return get(
            f"{cls.BASE_URL}accounts/zones/?accountIdList={','.join(account_ids)}",
            token=cls.ACCESS_TOKEN,
        )

    @classmethod
    @authenticated
    def get_zones(cls):
        return get(f"{cls.BASE_URL}zones", token=cls.ACCESS_TOKEN)

    @classmethod
    @authenticated
    def get_nadeoservices(cls, audience: str):
        data = post(
            f"{cls.BASE_URL}v2/authentication/token/nadeoservices",
            data={"audience": audience},
            token=cls.ACCESS_TOKEN,
        )
        return data


class NadeoLive(NadeoAudience):
    BASE_URL = "https://live-services.trackmania.nadeo.live/api/"
    MEET_BASE_URL = "https://meet.trackmania.nadeo.club/api/"
    ACCESS_TOKEN = ""
    REFRESH_TOKEN = ""
    AUDIENCE = "NadeoLiveServices"
    EXPIRE_TIME = datetime.fromtimestamp(0)

    @classmethod
    @authenticated
    def get_map_info(cls, map_uid):
        return get(f"{cls.BASE_URL}token/map/{map_uid}", cls.ACCESS_TOKEN)

    @classmethod
    @authenticated
    def get_match_participants(cls, id: int):
        return get(f"{cls.MEET_BASE_URL}matches/{id}/participants", cls.ACCESS_TOKEN)

    @classmethod
    @authenticated
    def get_match_teams(cls, id: int):
        return get(f"{cls.MEET_BASE_URL}matches/{id}/teams", cls.ACCESS_TOKEN)

    @classmethod
    @authenticated
    def get_match(cls, id: int):
        return get(f"{cls.MEET_BASE_URL}matches/{id}", cls.ACCESS_TOKEN)

    @classmethod
    @authenticated
    def get_player_ranks(cls, account_ids: list):
        players = "&players[]=".join(account_ids)
        return get(
            f"{cls.MEET_BASE_URL}matchmaking/2/leaderboard/players?players[]={players}",
            cls.ACCESS_TOKEN,
        )


class NadeoOauth(NadeoAudience):
    BASE_URL = "https://api.trackmania.com/api/"
    ACCESS_TOKEN = ""
    REFRESH_TOKEN = settings.UBISOFT_OAUTH_REFRESH_TOKEN
    EXPIRE_TIME = datetime.fromtimestamp(0)

    @classmethod
    def get_tokens(cls, refresh_token: str):
        return requests.post(
            f"{cls.BASE_URL}access_token",
            data={
                "grant_type": "refresh_token",
                "client_id": settings.CLIENT_ID,
                "client_secret": settings.CLIENT_SECRET,
                "refresh_token": refresh_token,
            },
        ).json()

    @classmethod
    def get(cls, path):
        return requests.get(
            f"{cls.BASE_URL}{path}",
            headers={"Authorization": "Bearer " + cls.ACCESS_TOKEN},
        ).json()

    @classmethod
    def post(cls, path, data):
        return requests.post(
            f"{cls.BASE_URL}{path}",
            data=data,
            headers={"Authorization": "Bearer" + cls.ACCESS_TOKEN},
        ).json()

    @classmethod
    def refresh_token(cls):
        result = cls.get_tokens(cls.REFRESH_TOKEN)
        cls.ACCESS_TOKEN = result["access_token"]
        cls.EXPIRE_TIME = datetime.fromtimestamp(
            json.loads(base64.b64decode(cls.ACCESS_TOKEN.split(".")[1].split("_")[0] + "========"))["exp"]
        )
        cls.REFRESH_TOKEN = result["refresh_token"]
        with open(settings.NADEO_FILE_PATH, "w") as f:
            f.write(result["refresh_token"])
            f.close()

    @classmethod
    @authenticated
    def get_player_display_names(cls, account_ids: list):
        ids = "&accountId[]=".join(account_ids)
        return cls.get(f"display-names?accountId[]={ids}")
