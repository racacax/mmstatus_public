import copy
import fcntl
import json
import os
import threading
import time
from datetime import datetime

_CACHE_TTL = 10  # seconds

_lock = threading.Lock()
_cache: dict = {}
_cache_loaded_at: float = 0.0


def _path() -> str:
    import settings

    return settings.NADEO_CREDENTIALS_FILE


def _lock_path() -> str:
    return _path() + ".lock"


def read_credentials() -> dict:
    """Return all credentials, reading from disk at most once per TTL seconds."""
    global _cache, _cache_loaded_at
    now = time.monotonic()
    with _lock:
        if _cache and now - _cache_loaded_at < _CACHE_TTL:
            return copy.deepcopy(_cache)
        try:
            with open(_lock_path(), "w") as lf:
                fcntl.flock(lf, fcntl.LOCK_SH)
                try:
                    with open(_path()) as f:
                        data = json.load(f)
                finally:
                    fcntl.flock(lf, fcntl.LOCK_UN)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}
        _cache = data
        _cache_loaded_at = now
        return copy.deepcopy(data)


def write_credentials(key: str, access_token: str, refresh_token: str, expire_time: datetime) -> None:
    """Persist credentials for *key*, holding an exclusive cross-process file lock."""
    global _cache, _cache_loaded_at
    path = _path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with _lock:
        with open(_lock_path(), "w") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                try:
                    with open(path) as f:
                        data = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    data = {}
                data[key] = {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "expire_time": expire_time.isoformat(),
                }
                tmp = path + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(data, f, indent=2)
                os.replace(tmp, path)
                _cache = data
                _cache_loaded_at = time.monotonic()
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)
