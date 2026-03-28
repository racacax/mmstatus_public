import json
import os

HEALTH_FILE = "cache/thread_health.json"


def write_health_file(active_threads):
    health = {}
    for cls, (t, instance) in active_threads.items():
        health[cls.__name__] = {
            "is_alive": t.is_alive(),
            "start_time": instance.start_time.isoformat(),
            "last_error_time": instance.last_error_time.isoformat() if instance.last_error_time else None,
            "error_count": instance.error_count,
        }
    os.makedirs(os.path.dirname(HEALTH_FILE), exist_ok=True)
    with open(HEALTH_FILE, "w") as f:
        json.dump(health, f)
