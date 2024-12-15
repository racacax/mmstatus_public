import os
import sys

import pymysql
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import settings


def get_conn():
    return pymysql.connect(
        host=settings.DATABASE_HOST,
        user=settings.DATABASE_USER,
        password=settings.DATABASE_PASSWORD,
    )


db = settings.db

TEST_DB = "mmstatus_test"


def pytest_sessionstart(session):
    """
    Configures test database for tests scenarios

    """
    print("Configuring database for tests...")
    if settings.DATABASE_NAME != TEST_DB:
        raise ValueError(f"DATABASE_NAME should be {TEST_DB}")
    conn = get_conn()
    conn.cursor().execute("DROP DATABASE IF EXISTS`" + TEST_DB + "`;")
    conn.cursor().execute(
        "CREATE DATABASE `" + TEST_DB + "` COLLATE utf8mb4_general_ci"
    )
    conn.close()
    print(os.popen("pem migrate").read())


def pytest_sessionfinish(session):
    """
    Delete test database after tests scenarios finished

    """
    conn = get_conn()
    conn.cursor().execute("DROP DATABASE IF EXISTS`" + TEST_DB + "`;")
    conn.close()


@pytest.fixture(autouse=True)
def run_around_tests():
    """
    Fixture to run all tests within a transaction.
    Transaction will be rollbacked after each test
    """
    with db.atomic() as transaction:
        try:
            yield
            transaction.rollback()
        except Exception as e:
            transaction.rollback()
            raise e
