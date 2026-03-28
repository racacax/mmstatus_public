"""Tests for credential access in services.py and the authenticated decorator.

Covers:
- NadeoCredentialsMixin (get_access_token, get_refresh_token, get_expire_time)
- Bootstrap fallback for NadeoCore (legacy nd_tk.txt / NADEO_REFRESH_TOKEN)
- Bootstrap fallback for NadeoOauth (legacy tk.txt / UBISOFT_OAUTH_REFRESH_TOKEN)
- NadeoLive has no bootstrap (first token obtained via NadeoCore)
- refresh_token() writes all fields to the credentials file
- authenticated decorator triggers refresh only when token is expired
- _decode_expire_time helper
"""

import base64
import json
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

import settings
import src.nadeo_credentials as creds_module
from src.nadeo_credentials import write_credentials
from src.services import NadeoCore, NadeoLive, NadeoOauth, _decode_expire_time


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def reset_cache():
    creds_module._cache = {}
    creds_module._cache_loaded_at = 0.0
    yield
    creds_module._cache = {}
    creds_module._cache_loaded_at = 0.0


@pytest.fixture
def creds_file(tmp_path, monkeypatch):
    path = str(tmp_path / "nadeo_credentials.json")
    monkeypatch.setattr(settings, "NADEO_CREDENTIALS_FILE", path)
    return path


def make_jwt(exp: int) -> str:
    """Build a minimal fake JWT whose payload contains only {"exp": <exp>}."""
    payload = base64.b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return f"eyJhbGciOiJIUzI1NiJ9.{payload}.fakesignature"


FUTURE = int(datetime(2099, 1, 1).timestamp())
PAST = int(datetime(2000, 1, 1).timestamp())


# ── _decode_expire_time ───────────────────────────────────────────────────────


class TestDecodeExpireTime:
    def test_decodes_future_timestamp(self):
        token = make_jwt(FUTURE)
        result = _decode_expire_time(token)
        assert result == datetime.fromtimestamp(FUTURE)

    def test_decodes_past_timestamp(self):
        token = make_jwt(PAST)
        result = _decode_expire_time(token)
        assert result == datetime.fromtimestamp(PAST)

    def test_decode_is_deterministic(self):
        token = make_jwt(1700000000)
        assert _decode_expire_time(token) == _decode_expire_time(token)


# ── NadeoCredentialsMixin ─────────────────────────────────────────────────────


class TestNadeoCredentialsMixin:
    """Tests against NadeoCore as the concrete representative of the mixin."""

    def test_get_access_token_from_file(self, creds_file):
        write_credentials("NadeoCore", "my_access", "my_refresh", datetime(2099, 1, 1))
        assert NadeoCore.get_access_token() == "my_access"

    def test_get_access_token_empty_when_no_file(self, creds_file):
        assert NadeoCore.get_access_token() == ""

    def test_get_access_token_empty_when_key_absent(self, creds_file):
        write_credentials("NadeoOauth", "oauth_at", "oauth_rt", datetime(2099, 1, 1))
        assert NadeoCore.get_access_token() == ""

    def test_get_refresh_token_from_file(self, creds_file):
        write_credentials("NadeoCore", "at", "stored_refresh", datetime(2099, 1, 1))
        assert NadeoCore.get_refresh_token() == "stored_refresh"

    def test_get_expire_time_from_file(self, creds_file):
        dt = datetime(2099, 6, 15, 12, 0, 0)
        write_credentials("NadeoCore", "at", "rt", dt)
        assert NadeoCore.get_expire_time() == dt

    def test_get_expire_time_returns_epoch_zero_when_missing(self, creds_file):
        assert NadeoCore.get_expire_time() == datetime.fromtimestamp(0)

    def test_each_class_reads_its_own_key(self, creds_file):
        write_credentials("NadeoCore", "core_at", "core_rt", datetime(2099, 1, 1))
        write_credentials("NadeoLive", "live_at", "live_rt", datetime(2099, 1, 1))
        write_credentials("NadeoOauth", "oauth_at", "oauth_rt", datetime(2099, 1, 1))
        assert NadeoCore.get_access_token() == "core_at"
        assert NadeoLive.get_access_token() == "live_at"
        assert NadeoOauth.get_access_token() == "oauth_at"


# ── Bootstrap fallback (legacy credentials) ───────────────────────────────────


class TestNadeoCoreBootstrap:
    def test_bootstrap_used_when_no_file(self, creds_file, monkeypatch):
        monkeypatch.setattr(settings, "NADEO_REFRESH_TOKEN", "legacy_nd_token")
        assert NadeoCore.get_refresh_token() == "legacy_nd_token"

    def test_bootstrap_used_when_key_absent_from_file(self, creds_file, monkeypatch):
        monkeypatch.setattr(settings, "NADEO_REFRESH_TOKEN", "legacy_nd_token")
        write_credentials("NadeoOauth", "at", "rt", datetime(2099, 1, 1))  # other key only
        assert NadeoCore.get_refresh_token() == "legacy_nd_token"

    def test_file_takes_priority_over_bootstrap(self, creds_file, monkeypatch):
        monkeypatch.setattr(settings, "NADEO_REFRESH_TOKEN", "legacy_nd_token")
        write_credentials("NadeoCore", "at", "stored_refresh", datetime(2099, 1, 1))
        assert NadeoCore.get_refresh_token() == "stored_refresh"

    def test_empty_bootstrap_returns_empty_string(self, creds_file, monkeypatch):
        monkeypatch.setattr(settings, "NADEO_REFRESH_TOKEN", "")
        assert NadeoCore.get_refresh_token() == ""


class TestNadeoOauthBootstrap:
    def test_bootstrap_used_when_no_file(self, creds_file, monkeypatch):
        monkeypatch.setattr(settings, "UBISOFT_OAUTH_REFRESH_TOKEN", "legacy_tk_token")
        assert NadeoOauth.get_refresh_token() == "legacy_tk_token"

    def test_bootstrap_used_when_key_absent_from_file(self, creds_file, monkeypatch):
        monkeypatch.setattr(settings, "UBISOFT_OAUTH_REFRESH_TOKEN", "legacy_tk_token")
        write_credentials("NadeoCore", "at", "rt", datetime(2099, 1, 1))
        assert NadeoOauth.get_refresh_token() == "legacy_tk_token"

    def test_file_takes_priority_over_bootstrap(self, creds_file, monkeypatch):
        monkeypatch.setattr(settings, "UBISOFT_OAUTH_REFRESH_TOKEN", "legacy_tk_token")
        write_credentials("NadeoOauth", "at", "stored_refresh", datetime(2099, 1, 1))
        assert NadeoOauth.get_refresh_token() == "stored_refresh"


class TestNadeoLiveNoBootstrap:
    def test_no_bootstrap_refresh_token(self, creds_file):
        assert NadeoLive._bootstrap_refresh_token() == ""

    def test_get_refresh_token_empty_when_no_file(self, creds_file):
        assert NadeoLive.get_refresh_token() == ""

    def test_get_refresh_token_from_file(self, creds_file):
        write_credentials("NadeoLive", "at", "live_refresh", datetime(2099, 1, 1))
        assert NadeoLive.get_refresh_token() == "live_refresh"


# ── refresh_token() writes to credentials file ────────────────────────────────


class TestNadeoCoreRefresh:
    def test_refresh_writes_access_token(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        monkeypatch.setattr(
            NadeoCore, "get_tokens", classmethod(lambda cls, rt: {"accessToken": access_jwt, "refreshToken": "new_rt"})
        )
        write_credentials("NadeoCore", "old_at", "old_rt", datetime(2000, 1, 1))
        NadeoCore.refresh_token()
        assert NadeoCore.get_access_token() == access_jwt

    def test_refresh_writes_refresh_token(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        monkeypatch.setattr(
            NadeoCore, "get_tokens", classmethod(lambda cls, rt: {"accessToken": access_jwt, "refreshToken": "new_rt"})
        )
        write_credentials("NadeoCore", "old_at", "old_rt", datetime(2000, 1, 1))
        NadeoCore.refresh_token()
        assert NadeoCore.get_refresh_token() == "new_rt"

    def test_refresh_writes_expire_time(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        monkeypatch.setattr(
            NadeoCore, "get_tokens", classmethod(lambda cls, rt: {"accessToken": access_jwt, "refreshToken": "new_rt"})
        )
        write_credentials("NadeoCore", "old_at", "old_rt", datetime(2000, 1, 1))
        NadeoCore.refresh_token()
        assert NadeoCore.get_expire_time() == datetime.fromtimestamp(FUTURE)

    def test_refresh_passes_stored_refresh_token_to_api(self, creds_file, monkeypatch):
        received = []
        access_jwt = make_jwt(FUTURE)

        def fake_get_tokens(cls, rt):
            received.append(rt)
            return {"accessToken": access_jwt, "refreshToken": "new_rt"}

        monkeypatch.setattr(NadeoCore, "get_tokens", classmethod(fake_get_tokens))
        write_credentials("NadeoCore", "old_at", "stored_rt", datetime(2000, 1, 1))
        NadeoCore.refresh_token()
        assert received == ["stored_rt"]

    def test_refresh_uses_bootstrap_when_no_stored_refresh_token(self, creds_file, monkeypatch):
        received = []
        access_jwt = make_jwt(FUTURE)

        def fake_get_tokens(cls, rt):
            received.append(rt)
            return {"accessToken": access_jwt, "refreshToken": "new_rt"}

        monkeypatch.setattr(NadeoCore, "get_tokens", classmethod(fake_get_tokens))
        monkeypatch.setattr(settings, "NADEO_REFRESH_TOKEN", "bootstrap_rt")
        NadeoCore.refresh_token()
        assert received == ["bootstrap_rt"]


class TestNadeoOauthRefresh:
    def test_refresh_writes_all_fields(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        monkeypatch.setattr(
            NadeoOauth,
            "get_tokens",
            classmethod(lambda cls, rt: {"access_token": access_jwt, "refresh_token": "new_oauth_rt"}),
        )
        write_credentials("NadeoOauth", "old_at", "old_rt", datetime(2000, 1, 1))
        NadeoOauth.refresh_token()
        assert NadeoOauth.get_access_token() == access_jwt
        assert NadeoOauth.get_refresh_token() == "new_oauth_rt"
        assert NadeoOauth.get_expire_time() == datetime.fromtimestamp(FUTURE)

    def test_refresh_does_not_affect_nadeocore_entry(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        monkeypatch.setattr(
            NadeoOauth,
            "get_tokens",
            classmethod(lambda cls, rt: {"access_token": access_jwt, "refresh_token": "new_rt"}),
        )
        write_credentials("NadeoCore", "core_at", "core_rt", datetime(2099, 1, 1))
        write_credentials("NadeoOauth", "oauth_at", "oauth_rt", datetime(2000, 1, 1))
        NadeoOauth.refresh_token()
        assert NadeoCore.get_access_token() == "core_at"


class TestNadeoLiveRefresh:
    def test_refresh_with_stored_token_calls_get_tokens(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        received = []

        def fake_get_tokens(cls, rt):
            received.append(rt)
            return {"accessToken": access_jwt, "refreshToken": "new_live_rt"}

        monkeypatch.setattr(NadeoCore, "get_tokens", classmethod(fake_get_tokens))
        write_credentials("NadeoLive", "old_at", "stored_live_rt", datetime(2000, 1, 1))
        NadeoLive.refresh_token()
        assert received == ["stored_live_rt"]
        assert NadeoLive.get_access_token() == access_jwt
        assert NadeoLive.get_refresh_token() == "new_live_rt"

    def test_refresh_without_stored_token_calls_nadeoservices(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        received_audience = []

        def fake_get_nadeoservices(cls, audience):
            received_audience.append(audience)
            return {"accessToken": access_jwt, "refreshToken": "brand_new_rt"}

        monkeypatch.setattr(NadeoCore, "get_nadeoservices", classmethod(fake_get_nadeoservices))
        # No NadeoLive entry in file → get_refresh_token() returns ""
        NadeoLive.refresh_token()
        assert received_audience == ["NadeoLiveServices"]
        assert NadeoLive.get_access_token() == access_jwt


# ── authenticated decorator ───────────────────────────────────────────────────


class TestAuthenticatedDecorator:
    """The decorator should refresh only when get_expire_time() is in the past."""

    def test_no_refresh_when_token_still_valid(self, creds_file, monkeypatch):
        refresh_calls = []

        def fake_refresh(cls):
            refresh_calls.append(True)

        monkeypatch.setattr(NadeoCore, "refresh_token", classmethod(fake_refresh))
        write_credentials("NadeoCore", "valid_at", "rt", datetime.now() + timedelta(hours=1))

        # Trigger the decorator via a method that uses @authenticated
        with patch("src.services.get", return_value={}):
            NadeoCore.get_zones()

        assert refresh_calls == []

    def test_refresh_called_when_token_expired(self, creds_file, monkeypatch):
        refresh_calls = []
        access_jwt = make_jwt(FUTURE)

        def fake_refresh(cls):
            refresh_calls.append(True)
            write_credentials("NadeoCore", access_jwt, "new_rt", datetime(2099, 1, 1))

        monkeypatch.setattr(NadeoCore, "refresh_token", classmethod(fake_refresh))
        write_credentials("NadeoCore", "expired_at", "rt", datetime(2000, 1, 1))

        with patch("src.services.get", return_value={}):
            NadeoCore.get_zones()

        assert refresh_calls == [True]

    def test_refresh_called_when_no_credentials_at_all(self, creds_file, monkeypatch):
        refresh_calls = []
        access_jwt = make_jwt(FUTURE)

        def fake_refresh(cls):
            refresh_calls.append(True)
            write_credentials("NadeoCore", access_jwt, "new_rt", datetime(2099, 1, 1))

        monkeypatch.setattr(NadeoCore, "refresh_token", classmethod(fake_refresh))

        with patch("src.services.get", return_value={}):
            NadeoCore.get_zones()

        assert refresh_calls == [True]

    def test_api_called_with_fresh_token_after_refresh(self, creds_file, monkeypatch):
        access_jwt = make_jwt(FUTURE)
        tokens_used = []

        def fake_refresh(cls):
            write_credentials("NadeoCore", access_jwt, "new_rt", datetime(2099, 1, 1))

        def fake_get(url, token):
            tokens_used.append(token)
            return {}

        monkeypatch.setattr(NadeoCore, "refresh_token", classmethod(fake_refresh))
        write_credentials("NadeoCore", "expired_at", "rt", datetime(2000, 1, 1))

        with patch("src.services.get", new=fake_get):
            NadeoCore.get_zones()

        assert tokens_used == [access_jwt]
