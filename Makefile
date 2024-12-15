DOCKER_EXE		= docker exec -i
DOCKER_EXE_TTY	= docker exec -it
DCO_EXE			= docker-compose
SEPARATOR       = _
PYTHON			= ${DOCKER_EXE_TTY} mmstatus${SEPARATOR}web${SEPARATOR}1 python

up:
	${DCO_EXE} up
up_web:
	${DCO_EXE} up web
stop:
	${DCO_EXE} stop
update build:
	${DCO_EXE} build
init:
	${PYTHON} scripts/init.py
run_sql:
	${PYTHON} scripts/run_sql.py ${SQL_SCRIPT}
create_season:
	${PYTHON} scripts/create_season.py
watch:
	${DOCKER_EXE_TTY} mmstatus${SEPARATOR}web${SEPARATOR}1 pem watch
migrate:
	${DOCKER_EXE_TTY} mmstatus${SEPARATOR}web${SEPARATOR}1 pem migrate

bash ssh:
	${DOCKER_EXE_TTY} mmstatus${SEPARATOR}web${SEPARATOR}1 bash