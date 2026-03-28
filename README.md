# Matchmaking Status (Backend)
## Description
Matchmaking Status is a project that tracks Trackmania matchmaking activity in-real time. It is connected to the Trackmania API and get matches data. With this data, statistics are computed.

You can find this project in production on : https://matchmaking.racacax.fr/

## Requirements
This project has a Docker image with all dependencies installed :

- Python 3.10 and its requirements
- Node 18 (for nodemon)

Docker Compose is also present with a MySQL docker image attached. It is optional if you want to use an external MySQL database. If your database is on localhost, you will have to use `host.docker.internal` as a host. Otherwise, default credentials will be used.

Libraries used:
- Peewee ORM
- peewee_migrations to manage Peewee ORM migrations
- pymysql to connect to MySQL database
## Installation
### Environment config
#### .env file
Set those variables in the .env file (root of the project)

`START_ID` : Start Match id from which the project will start fetch data. Look on trackmania.io or matchmaking.racacax.fr to see what are the latest ids. It has to be a recent match (less than a month ago).

`CLIENT_ID` and `CLIENT_SECRET` are credentials for the Trackmania public API. You can get these credentials from https://doc.trackmania.com/web/web-services/auth/ . Needed to get player names.

`ENABLE_OAUTH` will enable (or not) functions related to the Trackmania public API. Set it to `False` while everything is not configured yet.

`ENABLE_THREADS` will enable (or not) functions related to the Trackmania In-game API and stats calculations. Set it to `False` while everything is not configured yet.
```
START_ID=9112380
CLIENT_ID=XXXX
CLIENT_SECRET=XXXX
ENABLE_OAUTH=True
ENABLE_THREADS=True
NADEO_REFRESH_TOKEN=XXXX
UBISOFT_OAUTH_REFRESH_TOKEN=XXXX
# NADEO_CREDENTIALS_FILE=nadeo_credentials.json  # optional, default shown
```
#### API credentials
Tokens for the Trackmania APIs are stored in a single JSON file (`nadeo_credentials.json` by default).
The file path is configurable via the `NADEO_CREDENTIALS_FILE` env variable.

The file is created and updated automatically by the app. On first run, you need to supply the initial
refresh tokens via `.env` so the app can bootstrap itself:

```
NADEO_REFRESH_TOKEN=<your in-game API refresh token>
UBISOFT_OAUTH_REFRESH_TOKEN=<your public OAuth refresh token>
```

- `NADEO_REFRESH_TOKEN` — in-game (NadeoCore) refresh token. Follow https://webservices.openplanet.dev/auth to obtain it.
- `UBISOFT_OAUTH_REFRESH_TOKEN` — public OAuth refresh token. See https://doc.trackmania.com/web/web-services/auth/

Once the app has run once, these bootstrap tokens are no longer needed — all three credential sets
(`NadeoCore`, `NadeoLive`, `NadeoOauth`) are stored and refreshed in `nadeo_credentials.json`.

The file format is:
```json
{
  "NadeoCore":  { "access_token": "...", "refresh_token": "...", "expire_time": "2099-01-01T00:00:00" },
  "NadeoLive":  { "access_token": "...", "refresh_token": "...", "expire_time": "2099-01-01T00:00:00" },
  "NadeoOauth": { "access_token": "...", "refresh_token": "...", "expire_time": "2099-01-01T00:00:00" }
}
```

**Migrating from legacy `nd_tk.txt` / `tk.txt`:** set `NADEO_REFRESH_TOKEN` and
`UBISOFT_OAUTH_REFRESH_TOKEN` in `.env` to the values from those files and remove the old files.
The app will write `nadeo_credentials.json` on the next startup.
### Build image
Run `make update` to build the mmstatus image.
### Run container
If you want to use integrated database, use `make up` command. If you want to use an external database, use `make up_web` command to only start app container.
### Database creation (Docker only)
If you use Docker internal db image, you can run `make init`. It'll create the database with InnoDB and correct collation.
### Create tables, insert data and run migrations
At this stage, database is empty. To create models, you will have to run these commands
```shell
make run_sql SQL_SCRIPT="init_tables"  # Create all models
make run_sql SQL_SCRIPT="insert_zones" # insert Nadeo zones, linked to countries (optional)
make migrate # Run peewee-migrations to upgrade database to its current state
```
### Create a season
For the scripts to work, a season has to be active (current time between start and end time).
Create a season by running script `make create_season`.
### End of installation
Change .env variables to enable threads and oauth (if you want), stop container with `make stop` and run it again with `make up` or `make up_web`.

GG it's done.

## Usage and info
### Makefile
Multiple commands have been added to manage migrations (see Peewee ORM documentation) and also run bash.
#### Updating image
When requirements change, just run `make update`
### Custom migrations
custom_migrations folder contains multiple scripts that have been used at some point to do some operations. 
They might be useful in some cases, even though they are not supposed to be used for a simple usage.
### Computed data
Computed data are stored in the cache folder as TXT/JSON files. Those files mostly have JSON content.
Most files start with function names and end with season id. However, some stats are computed in subfolders (top 100 by country for instance).
### Tests
Tests are present in the tests folder. Run `make test` or `make testt` (with TTY) to trigger the tests.

Note: pytest will create a database named mmstatus_test. If you don't use docker internal db image, user needs to have database creation permission.
### Logs
Logs are stored in the logs folder. A new file is created each day and logs are kept for 5 days.
Each log line is a JSON. You can use tools like Log.io to monitor them in real time.
You can also see them via command line with `make show_all_logs` command.

You can also use `FILE="Your file" make show_logs` (e.g `FILE="get_matches" make show_logs`) to see logs for a special function only

You can also filter logs by adding a grep pattern (for both show_logs and show_all_logs). E.g. : `PATTERN="WARNING" FILE="get_matches" make show_logs`
### Lint
If you want to do any contribution, run `make lintt` before any commit to check for formatting errors. Black will reformat the file and flake8 will show remaining errors.