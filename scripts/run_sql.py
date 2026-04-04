import subprocess
import sys

from settings import DATABASE_NAME, DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD

sql_file = f"sql_scripts/{sys.argv[1]}.sql"
print(f"Running script {sql_file}...")

with open(sql_file, "rb") as f:
    result = subprocess.run(
        [
            "mysql",
            f"-h{DATABASE_HOST}",
            f"-u{DATABASE_USER}",
            f"-p{DATABASE_PASSWORD}",
            DATABASE_NAME,
        ],
        stdin=f,
    )

if result.returncode != 0:
    print("Script failed")
    sys.exit(result.returncode)

print("Script ran successfully")
