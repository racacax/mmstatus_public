"""
Verify that src.utils.get and src.utils.post always forward a timeout to requests.
conftest.py replaces src.utils.get at session start, so we reload the module to
recover the real implementations before patching requests.
"""

import importlib
from unittest.mock import MagicMock, patch

import requests
import src.utils


def _reload_utils():
    """Return a freshly-loaded src.utils module, bypassing the conftest patch."""
    importlib.reload(src.utils)
    return src.utils


class TestGetTimeout:
    def test_timeout_kwarg_is_forwarded(self):
        utils = _reload_utils()
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        with patch.object(requests, "get", return_value=mock_response) as mock_req:
            utils.get("http://fake.url", "fake_token")
        _, kwargs = mock_req.call_args
        assert "timeout" in kwargs

    def test_timeout_value_is_30(self):
        utils = _reload_utils()
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        with patch.object(requests, "get", return_value=mock_response) as mock_req:
            utils.get("http://fake.url", "fake_token")
        _, kwargs = mock_req.call_args
        assert kwargs["timeout"] == 30

    def test_authorization_header_still_sent(self):
        utils = _reload_utils()
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        with patch.object(requests, "get", return_value=mock_response) as mock_req:
            utils.get("http://fake.url", "my_token")
        _, kwargs = mock_req.call_args
        assert kwargs["headers"]["Authorization"] == "nadeo_v1 t=my_token"


class TestPostTimeout:
    def test_timeout_kwarg_is_forwarded(self):
        utils = _reload_utils()
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        with patch.object(requests, "post", return_value=mock_response) as mock_req:
            utils.post("http://fake.url", data="body", token="fake_token")
        _, kwargs = mock_req.call_args
        assert "timeout" in kwargs

    def test_timeout_value_is_30(self):
        utils = _reload_utils()
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        with patch.object(requests, "post", return_value=mock_response) as mock_req:
            utils.post("http://fake.url", data="body", token="fake_token")
        _, kwargs = mock_req.call_args
        assert kwargs["timeout"] == 30

    def test_authorization_header_still_sent(self):
        utils = _reload_utils()
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        with patch.object(requests, "post", return_value=mock_response) as mock_req:
            utils.post("http://fake.url", data="body", token="my_token")
        _, kwargs = mock_req.call_args
        assert kwargs["headers"]["Authorization"] == "nadeo_v1 t=my_token"
