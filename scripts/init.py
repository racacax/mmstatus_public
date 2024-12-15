import os
import sys

import pymysql

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from settings import DATABASE_NAME, DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD

# useful when using docker database
conn = pymysql.connect(
    host=DATABASE_HOST, user=DATABASE_USER, password=DATABASE_PASSWORD
)
conn.cursor().execute(
    "CREATE DATABASE `" + DATABASE_NAME + "` COLLATE utf8mb4_general_ci"
)
conn.close()
