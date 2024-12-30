DOCKER_EXE		= docker exec -i
DOCKER_EXE_TTY	= docker exec -it
DCO_EXE			= docker compose
SEPARATOR       = _
PYTHON_TTY		= ${DOCKER_EXE_TTY} mmstatus_web python
PYTHON			= ${DOCKER_EXE} mmstatus_web python

up:
	${DCO_EXE} up
up_web:
	${DCO_EXE} up web
stop:
	${DCO_EXE} stop
update build:
	${DCO_EXE} build
init:
	${PYTHON_TTY} scripts/init.py
run_sql:
	${PYTHON_TTY} scripts/run_sql.py ${SQL_SCRIPT}
create_season:
	${PYTHON_TTY} scripts/create_season.py
watch:
	${DOCKER_EXE_TTY} mmstatus_web pem watch
migrate:
	${DOCKER_EXE_TTY} mmstatus_web pem migrate

bash ssh:
	${DOCKER_EXE_TTY} mmstatus_web bash
testt:
	${DOCKER_EXE_TTY} mmstatus_web pytest -n auto --verbose
test:
	${DOCKER_EXE} mmstatus_web pytest -n auto --verbose
show_logs:
	${DOCKER_EXE} mmstatus_web tail -f logs/${FILE}.log | grep "${PATTERN}"
show_all_logs:
	FILE="*" PATTERN="${PATTERN}" make show_logs
lint:
	${PYTHON} -m black .
	${PYTHON} -m flake8 .
lintt:
	${PYTHON_TTY} -m black .
	${PYTHON_TTY} -m flake8 .
