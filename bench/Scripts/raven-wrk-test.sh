#!/bin/bash
IP="10.0.0.99"
PORT="18123"

LOGNAME="raven-wrk-test"
LOG="/tmp/${LOGNAME}.log"
RAVENDB_PATH="../../artifacts/ubuntu.16.04-x64/package/RavenDB/Server/Raven.Server"
# Warn: DATA_DIR is being removed rm -rf in this script
DATA_DIR="$(pwd)/TestDataDir"
RAVEN_CONF="--PublicServerUrl=http://${IP}:${PORT} --ServerUrl=http://0.0.0.0:${PORT} --DataDir=${DATA_DIR} --Security.UnsecuredAccessAllowed=PublicNetwork --Setup.Mode=None" 
TMP_FILE="/tmp/${LOGNAME}.out"
RESULT_FILE="/tmp/${LOGNAME}.results"
WRK_PATH="/usr/bin/wrk"
WRK_CONF="-d30 -t4 -c128 http://127.0.0.1:${PORT}"
WRK_SCRIPT_WRITES="-s writes.lua -- 4"
RES_MIN_WRITE_PER_SEC=7500

# exit code 0 for success, 2 for success but server did not shutdown in time
# exit code 1 on error

function log
{
	echo `date`: $1 >> ${LOG}
}

function shutdownServer
{
	if [[ $1 -ne 0 ]]; then
		log "About to kill process $1"
		kill $1 &>> ${LOG}
	else
		return
	fi

	CNT=0
	MAX=20
	while [[ ${CNT} -lt ${MAX} ]]; do
		RUNNING=$(ps -ef | awk '{print $2}' | grep "^$1$" | wc -l | awk '{print $1}') 
		if [[ ${RUNNING} -ne 0 ]]; then 
			((CNT++)); 
			sleep 1
		else 
			break; 
		fi
	done

	if [[ ${CNT} -eq ${MAX} ]]; then
		logerror "shutdown_flag" "RavenDB server did not quit on time"
	fi
}

function logerror
{
	log "ERROR($1): $2"
	if [[ "$1" == "exit_code_3" ]]; then
		shutdownServer $3
		exit 3
	fi

	if [[ "$1" != "shutdown_flag" ]]; then
		shutdownServer $3
		exit 1
	fi
}

log " "
log "Starting RavenDB wrk test"
log "========================="

JOBS=$(jobs -p | wc -l)
if [[ ${JOBS} -ne 0 ]]; then
	logerror 2 "Started with already parallel jobs. Practically cannot happen. JOBS=${JOBS}" 0
fi

if [[ -f ${RAVENDB_PATH} ]]; then
	log "${RAVENDB_PATH} will be used"
else
	logerror 4 "Cannot find ravendb dll ${RAVENDB_PATH}" 0
fi

if [[ -x ${WRK_PATH} ]]; then
        log "${WRK_PATH} will be used"
else
        logerror 5 "Cannot find wrk execautable ${WRK_PATH}" 0
fi

# TODO:
# remove the entire datadir
rm -rf ${DATA_DIR} &> /dev/null

log "About to run ${RAVENDB_PATH} ${RAVEN_CONF}"
${RAVENDB_PATH} ${RAVEN_CONF} &> ${TMP_FILE} &
log "Sleeping up to 30 seconds..."
CNT=0
MAX=10
while [[ ${CNT} -lt ${MAX} ]]; do
	RUNNING=$(grep -i "Running as Service" ${TMP_FILE} | wc -l | awk '{print $1}') 
	if [[ "${RUNNING}" -ne "1" ]]; then 
		((CNT++)); 
		sleep 1
	else 
		break; 
	fi
done

if [[ ${CNT} -eq ${MAX} ]]; then
	logerror 7 "Did not get 'Running as Service' on time" 0
fi
	
JOBS=$(jobs -p | wc -l)
if [[ ${JOBS} != 1 ]]; then
        logerror 6 "Failed to run raven server. JOBS=${JOBS}" 0
fi

PSNUM=$(jobs -p)

log "About to createdb. Response:"
curl -H "Content-Type: application/json" -X PUT -d '{"Settings": {"DataDir": "'${DATA_DIR}/Databases/'BenchmarkDB"}}' http://${IP}:${PORT}/admin/databases?name=BenchmarkDB &>> ${LOG}
echo "" >> ${LOG}

# TODO:
# import data
# test both read writes and queries

log "About to run ${WRK_PATH} ${WRK_CONF} ${WRK_SCRIPT_WRITES}"
${WRK_PATH} ${WRK_CONF} ${WRK_SCRIPT_WRITES} &> ${RESULT_FILE}

NONSUCCESS=$(grep -i "non" ${RESULT_FILE} | grep -i "responses" | wc -l | awk '{print $1}')
if [[ ${NONSUCCESS} -gt 0 ]]; then
        logerror 10 "Failed to get wrk results" ${PSNUM}
fi

cat ${RESULT_FILE} | grep 'Requests/sec:' &> ${RESULT_FILE}.tmp
if [[ $(cat ${RESULT_FILE}.tmp | wc -l | awk '{print $1}') -ne "1" ]]; then
	logerror 11 "Failed to get wrk results" ${PSNUM}
fi
RESULT=$(cat ${RESULT_FILE}.tmp | awk '{print $2}' | cut -f1 -d'.')

if [[ ${RESULT} -lt ${RES_MIN_WRITE_PER_SEC} ]]; then
	logerror "exit_code_3" "Got ${RESULT} writes per sec while minimum is: ${RES_MIN_WRITE_PER_SEC}" ${PSNUM}
fi

shutdownServer ${PSNUM}
IS_RUNNING=$(ps -ef | awk '{print $2}' | grep "^$1$" | wc -l | awk '{print $1}')
if [[ ${IS_RUNNING} -gt 0 ]]; then
	logerror 13 "Returning error as server did not shutdown in time"
	exit 2 # different exit code
fi
log "Ended successfully with total writes per second: ${RESULT}"
exit 0
