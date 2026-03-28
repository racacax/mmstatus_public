import base64
import json
from datetime import datetime

import requests

import settings
from src.nadeo_credentials import read_credentials, write_credentials
from src.types import NadeoParticipant, NadeoMatch, NadeoMapInfo, NadeoMatchTeam, NadeoPlayerRanks, NadeoAccountInfo
from src.utils import post, authenticated, get


def _decode_expire_time(access_token: str) -> datetime:
    payload = access_token.split(".")[1].split("_")[0] + "========"
    return datetime.fromtimestamp(json.loads(base64.b64decode(payload))["exp"])


class NadeoCredentialsMixin:
    """Provides disk-backed credential access with a 10 s in-process TTL cache."""

    CREDENTIALS_KEY: str = ""

    @classmethod
    def _bootstrap_refresh_token(cls) -> str:
        """Initial refresh token when the credentials file has no entry yet."""
        return ""

    @classmethod
    def get_access_token(cls) -> str:
        return read_credentials().get(cls.CREDENTIALS_KEY, {}).get("access_token", "")

    @classmethod
    def get_refresh_token(cls) -> str:
        stored = read_credentials().get(cls.CREDENTIALS_KEY, {}).get("refresh_token", "")
        return stored or cls._bootstrap_refresh_token()

    @classmethod
    def get_expire_time(cls) -> datetime:
        raw = read_credentials().get(cls.CREDENTIALS_KEY, {}).get("expire_time")
        return datetime.fromisoformat(raw) if raw else datetime.fromtimestamp(0)


class NadeoAudience(NadeoCredentialsMixin):
    BASE_URL = None
    AUDIENCE = "MyAudience"

    @classmethod
    def refresh_token(cls):
        refresh = cls.get_refresh_token()
        if not refresh:
            result = NadeoCore.get_nadeoservices(cls.AUDIENCE)
        else:
            result = NadeoCore.get_tokens(refresh)
        access_token = result["accessToken"]
        write_credentials(cls.CREDENTIALS_KEY, access_token, result["refreshToken"], _decode_expire_time(access_token))


class NadeoCore(NadeoCredentialsMixin):
    BASE_URL = "https://prod.trackmania.core.nadeo.online/"
    CREDENTIALS_KEY = "NadeoCore"

    @classmethod
    def _bootstrap_refresh_token(cls) -> str:
        return settings.NADEO_REFRESH_TOKEN

    @classmethod
    def get_tokens(cls, refresh_token: str):
        return post(
            f"{cls.BASE_URL}v2/authentication/token/refresh",
            data="",
            token=refresh_token,
        )

    @classmethod
    def refresh_token(cls):
        result = cls.get_tokens(cls.get_refresh_token())
        access_token = result["accessToken"]
        write_credentials(cls.CREDENTIALS_KEY, access_token, result["refreshToken"], _decode_expire_time(access_token))

    @classmethod
    @authenticated
    def get_player_club_tags(cls, account_ids: list) -> list[NadeoAccountInfo]:
        return get(
            f"{cls.BASE_URL}accounts/clubTags/?accountIdList={','.join(account_ids)}",
            token=cls.get_access_token(),
        )

    @classmethod
    @authenticated
    def get_player_zones(cls, account_ids: list):
        return get(
            f"{cls.BASE_URL}accounts/zones/?accountIdList={','.join(account_ids)}",
            token=cls.get_access_token(),
        )

    @classmethod
    @authenticated
    def get_zones(cls):
        return get(f"{cls.BASE_URL}zones", token=cls.get_access_token())

    @classmethod
    @authenticated
    def get_nadeoservices(cls, audience: str):
        return post(
            f"{cls.BASE_URL}v2/authentication/token/nadeoservices",
            data={"audience": audience},
            token=cls.get_access_token(),
        )


class NadeoLive(NadeoAudience):
    BASE_URL = "https://live-services.trackmania.nadeo.live/api/"
    MEET_BASE_URL = "https://meet.trackmania.nadeo.club/api/"
    AUDIENCE = "NadeoLiveServices"
    CREDENTIALS_KEY = "NadeoLive"

    @classmethod
    @authenticated
    def get_map_info(cls, map_uid) -> NadeoMapInfo:
        return get(f"{cls.BASE_URL}token/map/{map_uid}", cls.get_access_token())

    @classmethod
    @authenticated
    def get_match_participants(cls, id: int) -> list[NadeoParticipant]:
        return get(f"{cls.MEET_BASE_URL}matches/{id}/participants", cls.get_access_token())

    @classmethod
    @authenticated
    def get_match_teams(cls, id: int) -> list[NadeoMatchTeam]:
        return get(f"{cls.MEET_BASE_URL}matches/{id}/teams", cls.get_access_token())

    @classmethod
    @authenticated
    def get_match(cls, id: int) -> NadeoMatch:
        return get(f"{cls.MEET_BASE_URL}matches/{id}", cls.get_access_token())

    @classmethod
    @authenticated
    def get_player_ranks(cls, account_ids: list) -> NadeoPlayerRanks:
        players = "&players[]=".join(account_ids)
        return get(
            f"{cls.MEET_BASE_URL}matchmaking/5/leaderboard/players?players[]={players}",
            cls.get_access_token(),
        )


class NadeoOauth(NadeoCredentialsMixin):
    BASE_URL = "https://api.trackmania.com/api/"
    CREDENTIALS_KEY = "NadeoOauth"

    @classmethod
    def _bootstrap_refresh_token(cls) -> str:
        return settings.UBISOFT_OAUTH_REFRESH_TOKEN

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
            headers={"Authorization": "Bearer " + cls.get_access_token()},
        ).json()

    @classmethod
    def post(cls, path, data):
        return requests.post(
            f"{cls.BASE_URL}{path}",
            data=data,
            headers={"Authorization": "Bearer " + cls.get_access_token()},
        ).json()

    @classmethod
    def refresh_token(cls):
        result = cls.get_tokens(cls.get_refresh_token())
        access_token = result["access_token"]
        write_credentials(cls.CREDENTIALS_KEY, access_token, result["refresh_token"], _decode_expire_time(access_token))

    @classmethod
    @authenticated
    def get_player_display_names(cls, account_ids: list) -> dict[str, str]:
        ids = "&accountId[]=".join(account_ids)
        return cls.get(f"display-names?accountId[]={ids}")
