import os

import peewee
from dotenv import load_dotenv
from playhouse.shortcuts import ReconnectMixin

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
NADEO_FILE_PATH = os.path.join(os.path.dirname(__file__), "tk.txt")
UBISOFT_OAUTH_REFRESH_TOKEN = open(NADEO_FILE_PATH, "r").read()
START_ID = int(os.getenv("START_ID", default=8958871))
NADEO2_FILE_PATH = os.path.join(os.path.dirname(__file__), "nd_tk.txt")
NADEO_REFRESH_TOKEN = open(NADEO2_FILE_PATH, "r").read()
ENABLE_OAUTH = os.getenv("ENABLE_OAUTH", "True") == "True"
ENABLE_THREADS = os.getenv("ENABLE_THREADS", "True") == "True"
DATABASE_NAME = os.getenv("DATABASE_NAME", "mmstatus")
DATABASE_USER = os.getenv("DATABASE_USER", "root")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "doweneedpasswordindocker")
DATABASE_HOST = os.getenv("DATABASE_HOST", "db")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", 3306))
SHOW_LOGS = os.getenv("SHOW_LOGS", "False") == "True"
if os.environ.get("ENVIRONMENT") == "test":
    DATABASE_NAME = f"mmstatus_test_{os.environ.get('PYTEST_XDIST_WORKER')}"


class ReconnectMySQLDatabase(ReconnectMixin, peewee.MySQLDatabase):
    def reconnect_if_lost(self):
        try:
            self.execute_sql("SELECT 1")
        except peewee.OperationalError:
            if not self.is_closed():
                self.close()
            self.connect()


db = ReconnectMySQLDatabase(
    DATABASE_NAME,
    user=DATABASE_USER,
    password=DATABASE_PASSWORD,
    host=DATABASE_HOST,
    port=DATABASE_PORT,
)
