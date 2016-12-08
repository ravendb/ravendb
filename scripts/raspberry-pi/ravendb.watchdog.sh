#!/bin/bash 
LOGFILE="/tmp/ravendb.wathchdog.log"
RAVENDBPATH="${1}/Server"
DOTNETPATH="${2}"

${DOTNETPATH}/corerun ${RAVENDBPATH}/Raven.Server.dll \
	 --Raven/RunAsService=true \
	 --Raven/ServerUrl=http://0.0.0.0:8080 \
	 --Raven/LogsDirectory=${1}/../Logs \
	 --Raven/DataDir=${1}/../Databases \
	 --Raven/StudioDirectory=${1}/Server &

RDB_PID=$!
wait ${RDB_PID}
RDB_RC=$?
if [ -f /tmp/ravendb.${RDB_PID}.lockfile ]
then
	rm -f /tmp/ravendb.${RDB_PID}.lockfile
	echo "`date` : ravendb watchdog exited due to the existance of lockfile for PID=${RDB_PID}, while raven's exit code=${RDB_RC}" >> ${LOGFILE}
	exit 0
fi
echo "`date` : ravendb watchdog detected on PID=${RDB_PID} with exit code=${RDB_RC}. Restarting service..." >> ${LOGFILE}
sudo /usr/sbin/service ravendbd restart
echo "`date` : ravendb daemon restarted" >> ${LOGFILE}
exit $RDB_RC
