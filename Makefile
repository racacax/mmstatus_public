DOCKER_EXE		= docker exec -i
DOCKER_EXE_TTY	= docker exec -it
DCO_EXE			= docker-compose
SEPARATOR       = _
PYTHON			= ${DOCKER_EXE_TTY} mmstatus_web python

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
	${DOCKER_EXE_TTY} mmstatus_web pem watch
migrate:
	${DOCKER_EXE_TTY} mmstatus_web pem migrate

bash ssh:
	${DOCKER_EXE_TTY} mmstatus_web bash
testt:
	${DOCKER_EXE_TTY} mmstatus_web pytest --verbose
test:
	${DOCKER_EXE} mmstatus_web pytest --verbose