import sys

import pymysql

from settings import DATABASE_NAME, DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD

# useful when using docker database
conn = pymysql.connect(
    host=DATABASE_HOST,
    user=DATABASE_USER,
    password=DATABASE_PASSWORD,
    database=DATABASE_NAME,
)


def parse_sql(data):
    stmt = ""
    stmts = []
    for line in data:
        if line:
            if line.startswith("--"):
                continue
            stmt += line.strip() + " "
            if ";" in stmt:
                stmts.append(stmt.strip())
                stmt = ""
    return stmts


with open(f"sql_scripts/{sys.argv[1]}.sql") as f:
    print("Running script...")
    with conn.cursor() as cursor:
        queries = parse_sql(f.read().splitlines())
        for query in queries:
            print(query)
            cursor.execute(query)
    conn.close()
    print("Script ran successfully")
