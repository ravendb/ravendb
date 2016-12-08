#!/bin/bash 
LOGFILE="/tmp/ravendb.wathchdog.log"

RDB_DOTNET_PATH RDB_RAVENDB_PATH/Raven.Server.dll \
	--Raven/RunAsService=true \
	 RDB_PARAMS &

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
