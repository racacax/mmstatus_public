# Matchmaking Status (Backend)
## Description
Matchmaking Status is a project that tracks Trackmania matchmaking activity in-real time. It is connected to the Trackmania API and get matches data. With this data, statistics, are computed.

You can find this project in production on : https://matchmaking.racacax.fr/

## Requirements
This projects has a Docker image with all dependencies installed :

- Python 3.10 and its requirements
- Node 18 (for nodemon)

Docker Compose is also present with a MySQL docker image attached. It is optional if you want to use an external MySQL database. If your database is on localhost, you will have to use `host.docker.internal` as a host. Otherwise, default credentials will be used.

Librairies used:
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
```
#### nd_tk.txt and tk.txt files
To configure in-game API, you need to get refresh token. Follow this tutorial to get it.

Once done, you'll need to put it in the `nd_tk.txt` file. This file will be updated by the app because it keeps changing. If the app didn't run for a while, you might it to change it later.

Create also a file named `tk.txt`. It will be used for the public API. This one will get updated as well, depending on `CLIENT_ID` and `CLIENT_SECRET`.
However you need to put a refresh token in the first place (see: https://doc.trackmania.com/web/web-services/auth/ )
### Build image
Run `docker-compose build` to build the mmstatus image.
### Run container
If you want to use integrated database, use `make up` command. If you want to use an external database, use `make up_web` command to only start app container.
### Database creation (Docker only)
If you use an external server and database is not created, you can run `make init`. It'll create the database with InnoDB and correct collation.

Note : If you have an error "No such container", see section "Docker image name".
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
### Docker image name
Depending on your Docker version, separator might be - or _. If it is -, you will need to add `SEPARATOR="-"` after any `make` command.

Example : `make run_sql SEPARATOR="-" SQL_SCRIPT="init_tables"`

### Makefile
Multiple commands have been added to manage migrations (see Peewee ORM documentation) and also run bash.

### Custom migrations
custom_migrations folder contains multiple scripts that have been used at some point to do some operations. 
They might be useful in some cases, even though they are not supposed to be used for a simple usage.